package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Storage is the actual I/O seam, implemented directly by *s3store.Store.
// Mutations make one attempt; unknown errors are always treated as ambiguous.
// List is ordered and complete only on nil return. Files are private disk spools.
type Storage interface {
	CheckBucketSafety(context.Context) error
	Head(context.Context, string) (s3store.Info, error)
	Read(context.Context, string, int64) ([]byte, s3store.Info, error)
	PutFile(context.Context, string, *os.File, s3store.Integrity, s3store.Condition, map[string]string) (s3store.Info, error)
	UploadFile(context.Context, string, *os.File, s3store.Integrity, map[string]string) (s3store.Upload, s3store.Info, error)
	Download(context.Context, string, *os.File, s3store.Integrity) (s3store.Info, error)
	List(context.Context, string, int, func(s3store.Info) error) error
	Delete(context.Context, string) error
	Abort(context.Context, s3store.Upload) error
	ListUploads(context.Context, string, int, func(s3store.Upload) error) error
}

var _ Storage = (*s3store.Store)(nil)

type Repository struct {
	store                    Storage
	id                       Identity
	root, workspace, process string
	gcMu                     sync.Mutex
	gcNotBefore              time.Time
}

// Open checks immutable identity and an existing gate. It never initializes or
// repairs one. A fresh Repository denotes a fresh process incarnation.
func Open(ctx context.Context, s Storage, id Identity, workspace string) (*Repository, error) {
	if s == nil || id.Validate() != nil || !filepath.IsAbs(workspace) {
		return nil, ErrInvalid
	}
	st, e := os.Stat(workspace)
	if e != nil || !st.IsDir() {
		return nil, ErrInvalid
	}
	r := &Repository{store: s, id: id, root: "v1/" + id.RepositoryID + "/", workspace: workspace, process: UUID()}
	b, _, e := s.Read(ctx, r.root+"repository.json", smallLimit)
	if e != nil {
		return nil, e
	}
	var got Identity
	if strict(b, smallLimit, &got) != nil || got != id {
		return nil, ErrIdentity
	}
	if _, _, e = r.readGate(ctx); e != nil {
		return nil, e
	}
	return r, nil
}

// Initialize must follow s3store CheckBucketSafety and CheckConditions. It
// finishes interrupted first initialization only when a COMPLETE inventory
// contains no data. It never recreates a missing gate over a used repository.
func Initialize(ctx context.Context, s *s3store.Store, id Identity, workspace string) (*Repository, error) {
	if s == nil || id.Validate() != nil {
		return nil, ErrInvalid
	}
	if e := s.CheckBucketSafety(ctx); e != nil {
		return nil, e
	}
	root := "v1/" + id.RepositoryID + "/"
	// Probes are permanent non-payload diagnostics until admitted cleanup exists.
	if _, e := s.CheckConditions(ctx, root+"probes/"+UUID(), workspace); e != nil {
		return nil, e
	}
	if e := initialize(ctx, s, id, workspace); e != nil {
		return nil, e
	}
	return Open(ctx, s, id, workspace)
}
func initialize(ctx context.Context, s Storage, id Identity, workspace string) error {
	if id.Validate() != nil {
		return ErrInvalid
	}
	r := &Repository{store: s, id: id, root: "v1/" + id.RepositoryID + "/", workspace: workspace, process: UUID()}
	_, _, readErr := s.Read(ctx, r.root+"repository.json", smallLimit)
	if s3store.Is(readErr, s3store.NotFound) {
		if e := s.List(ctx, r.root, MaxCatalogRecords, func(i s3store.Info) error {
			if strings.HasPrefix(i.Key, r.root+"probes/") {
				return nil
			}
			return ErrIdentity
		}); e != nil {
			return e
		}
	} else if readErr != nil {
		return readErr
	}
	b, _ := json.Marshal(id)
	if _, e := r.createExact(ctx, r.root+"repository.json", b, smallLimit); e != nil {
		return e
	}
	if _, _, e := r.readGate(ctx); e == nil {
		return nil
	} else if !s3store.Is(e, s3store.NotFound) {
		return e
	}
	if e := s.List(ctx, r.root, MaxCatalogRecords, func(i s3store.Info) error {
		if i.Key == r.root+"repository.json" || strings.HasPrefix(i.Key, r.root+"probes/") {
			return nil
		}
		return ErrCorrupt
	}); e != nil {
		return e
	}
	g := Gate{Schema: 1, RepositoryID: id.RepositoryID, Generation: "0", Nonce: UUID(), Holders: []Holder{}}
	b, _ = json.Marshal(g)
	_, e := r.put(ctx, r.root+"gate.json", b, s3store.Condition{Create: true})
	if e == nil {
		return nil
	}
	// An existing valid gate is sufficient; its nonce need not equal our candidate.
	_, _, readErr = r.readGate(ctx)
	if readErr == nil {
		return nil
	}
	return e
}
func (r *Repository) Identity() Identity       { return r.id }
func (r *Repository) ProcessID() string        { return r.process }
func (r *Repository) backup(uid string) string { return r.root + "backups/" + uid + "/" }
func (r *Repository) attempt(uid, attempt string) string {
	return r.backup(uid) + "attempts/" + attempt + "/"
}
func (r *Repository) artifact(c Commit, a Artifact) string {
	s := r.attempt(c.BackupUID, c.AttemptID) + "data/" + strconv.Itoa(a.Index) + ".tar"
	if a.Compression == "gzip" {
		s += ".gz"
	}
	return s
}
func (r *Repository) temp() (*os.File, error) { return os.CreateTemp(r.workspace, "repository-*") }
func removeFile(f *os.File)                   { name := f.Name(); f.Close(); os.Remove(name) }
func (r *Repository) put(ctx context.Context, key string, b []byte, c s3store.Condition) (s3store.Info, error) {
	f, e := r.temp()
	if e != nil {
		return s3store.Info{}, e
	}
	defer removeFile(f)
	if _, e = f.Write(b); e != nil {
		return s3store.Info{}, e
	}
	return r.store.PutFile(ctx, key, f, s3store.Integrity{Size: int64(len(b)), SHA256: digest(b)}, c, nil)
}
func (r *Repository) createExact(ctx context.Context, key string, b []byte, max int64) (s3store.Info, error) {
	i, e := r.put(ctx, key, b, s3store.Condition{Create: true})
	if e == nil {
		return i, nil
	}
	got, i, re := r.store.Read(ctx, key, max)
	if re != nil {
		return i, e
	}
	if !bytes.Equal(got, b) {
		return i, ErrIdentity
	}
	return i, nil
}
func ambiguous(e error) bool {
	if e == nil {
		return false
	}
	var se *s3store.Error
	if errors.As(e, &se) {
		return se.Ambiguous
	}
	return true
}
func (r *Repository) readCommit(ctx context.Context, uid string) (Commit, []byte, s3store.Info, error) {
	var c Commit
	if !validID(uid) {
		return c, nil, s3store.Info{}, ErrInvalid
	}
	b, i, e := r.store.Read(ctx, r.backup(uid)+"commit.json", commitLimit)
	if e != nil {
		return c, nil, i, e
	}
	if e = strict(b, commitLimit, &c); e != nil {
		return c, nil, i, e
	}
	if e = c.validate(r.id); e != nil {
		return c, nil, i, e
	}
	if c.BackupUID != uid {
		return c, nil, i, ErrCorrupt
	}
	return c, b, i, nil
}
func (r *Repository) live(ctx context.Context, c Commit, b []byte) error {
	rb, _, e := r.store.Read(ctx, r.backup(c.BackupUID)+"retired.json", smallLimit)
	if s3store.Is(e, s3store.NotFound) {
		return nil
	}
	if e != nil {
		return e
	}
	var ret Retirement
	if strict(rb, smallLimit, &ret) != nil || ret.validate(r.id, c.BackupUID, digest(b)) != nil {
		return ErrCorrupt
	}
	return ErrRetired
}
func (r *Repository) requestFor(ctx context.Context, c Commit) (Request, error) {
	var req Request
	b, _, e := r.store.Read(ctx, r.backup(c.BackupUID)+"request.json", smallLimit)
	if e != nil {
		return req, e
	}
	if strict(b, smallLimit, &req) != nil || req.validate(r.id) != nil || digest(b) != c.RequestSHA256 || req.BackupUID != c.BackupUID || req.RequestedKind != c.Kind {
		return req, ErrCorrupt
	}
	if c.Kind == "differential" && (req.RootBackupUID == nil || *req.RootBackupUID != c.RootBackupUID) {
		return req, ErrCorrupt
	}
	var cl Claim
	b, _, e = r.store.Read(ctx, r.attempt(c.BackupUID, c.AttemptID)+"claim.json", smallLimit)
	if e != nil {
		return req, e
	}
	if strict(b, smallLimit, &cl) != nil || cl.Schema != 1 || cl.RepositoryID != r.id.RepositoryID || cl.BackupUID != c.BackupUID || cl.AttemptID != c.AttemptID || !validID(cl.ProcessID) || cl.RequestSHA256 != c.RequestSHA256 {
		return req, ErrCorrupt
	}
	return req, nil
}
