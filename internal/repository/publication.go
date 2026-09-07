package repository

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Result is the exact winning commit bytes and its S3 publication timestamp,
// never the candidate's clock or content. Bytes are private to the caller.
type Result struct {
	Commit      Commit
	Bytes       []byte
	PublishedAt time.Time
}
type Attempt struct {
	hold    *Hold
	request Request
	claim   Claim
	used    bool
}

// Begin freezes semantic request input, replays any verified winner, or claims
// a fresh attempt namespace. A failed attempt can never be reused.
func (h *Hold) Begin(ctx context.Context, req Request) (*Attempt, *Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return nil, nil, e
	}
	if h.holder.Kind != "backup" || req.BackupUID != h.holder.OperationID {
		return nil, nil, ErrIdentity
	}
	r := h.r
	if e := req.validate(r.id); e != nil {
		return nil, nil, e
	}
	if req.RootBackupUID != nil {
		root := *req.RootBackupUID
		req.RootBackupUID = &root
	}
	b, _ := json.Marshal(req)
	_, e := r.put(ctx, r.backup(req.BackupUID)+"request.json", b, s3store.Condition{Create: true})
	if ambiguous(e) {
		h.uncertain = true
	}
	if e != nil {
		got, _, re := r.store.Read(ctx, r.backup(req.BackupUID)+"request.json", smallLimit)
		if re != nil {
			return nil, nil, e
		}
		var frozen Request
		if strict(got, smallLimit, &frozen) != nil || digest(got) != digest(b) {
			return nil, nil, ErrIdentity
		}
	}
	c, cb, info, e := r.readCommit(ctx, req.BackupUID)
	if e == nil {
		if c.RequestSHA256 != digest(b) {
			return nil, nil, ErrIdentity
		}
		if e = r.verifyWinner(ctx, c, cb); e != nil {
			return nil, nil, e
		}
		return nil, &Result{c, cb, info.Modified}, nil
	}
	if !s3store.Is(e, s3store.NotFound) {
		return nil, nil, e
	}
	if h.uncertain {
		return nil, nil, ErrUncertain
	}
	if req.RootBackupUID != nil {
		p, pb, _, e := r.readCommit(ctx, *req.RootBackupUID)
		if e != nil {
			return nil, nil, e
		}
		if p.Kind != "full" {
			return nil, nil, ErrCorrupt
		}
		if e = r.verifyWinner(ctx, p, pb); e != nil {
			return nil, nil, e
		}
	}
	cl := Claim{Schema: 1, RepositoryID: r.id.RepositoryID, BackupUID: req.BackupUID, AttemptID: UUID(), ProcessID: r.process, RequestSHA256: digest(b)}
	b, _ = json.Marshal(cl)
	_, e = r.put(ctx, r.attempt(cl.BackupUID, cl.AttemptID)+"claim.json", b, s3store.Condition{Create: true})
	if e != nil {
		if ambiguous(e) {
			h.uncertain = true
		}
		return nil, nil, e
	}
	return &Attempt{hold: h, request: req, claim: cl}, nil, nil
}
func (a *Attempt) ID() string            { return a.claim.AttemptID }
func (a *Attempt) RequestSHA256() string { return a.claim.RequestSHA256 }

// Publish consumes this attempt exactly once. The native caller has already
// verified the ORIGINAL manifest/tars/WAL and capture postflight. This module
// enforces transport/raw integrity and parent identity again before commit.
// Files stay caller-owned, immutable and disk-backed for the whole call.
func (a *Attempt) Publish(ctx context.Context, c Commit, manifest *os.File, files []*os.File) (*Result, error) {
	h := a.hold
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return nil, e
	}
	if a.used {
		return nil, ErrClosed
	}
	a.used = true
	r := h.r
	if e := c.validate(r.id); e != nil {
		return nil, e
	}
	if c.BackupUID != a.claim.BackupUID || c.AttemptID != a.claim.AttemptID || c.RequestSHA256 != a.claim.RequestSHA256 || c.Kind != a.request.RequestedKind || len(files) != len(c.Artifacts) {
		return nil, ErrIdentity
	}
	if c.Kind == "differential" && (a.request.RootBackupUID == nil || *a.request.RootBackupUID != c.RootBackupUID) {
		return nil, ErrIdentity
	}
	if e := r.parent(ctx, c); e != nil {
		return nil, e
	}
	if e := verifyFile(ctx, manifest, c.ManifestBytes, c.ManifestSHA256, "none", c.ManifestBytes, c.ManifestSHA256); e != nil {
		return nil, e
	}
	for i, f := range files {
		ar := c.Artifacts[i]
		if e := verifyFile(ctx, f, ar.StoredBytes, ar.StoredSHA256, ar.Compression, ar.RawBytes, ar.RawSHA256); e != nil {
			return nil, e
		}
	}
	// Unique namespace MPU cannot clobber another attempt. Failure leaves its
	// upload to the admitted GC owner; there is no defer-abort/delete.
	_, _, e := r.store.UploadFile(ctx, r.attempt(c.BackupUID, c.AttemptID)+"manifest.pg.json", manifest, s3store.Integrity{Size: c.ManifestBytes, SHA256: c.ManifestSHA256}, nil)
	if e != nil {
		if ambiguous(e) {
			h.uncertain = true
		}
		return nil, e
	}
	for i, f := range files {
		ar := c.Artifacts[i]
		_, _, e = r.store.UploadFile(ctx, r.artifact(c, ar), f, s3store.Integrity{Size: ar.StoredBytes, SHA256: ar.StoredSHA256}, nil)
		if e != nil {
			if ambiguous(e) {
				h.uncertain = true
			}
			return nil, e
		}
	}
	if e = r.verifyPayload(ctx, c); e != nil {
		return nil, e
	}
	if e = r.parent(ctx, c); e != nil {
		return nil, e
	}
	b, _ := json.Marshal(c)
	if int64(len(b)) > commitLimit {
		return nil, ErrCapacity
	}
	_, e = r.put(ctx, r.backup(c.BackupUID)+"commit.json", b, s3store.Condition{Create: true})
	if ambiguous(e) {
		h.uncertain = true
	}
	// Always read the winner, even on a successful response, to obtain the
	// immutable object's actual publication time and verify its references.
	winner, wb, wi, re := r.readCommit(ctx, c.BackupUID)
	if re != nil {
		if e != nil {
			return nil, e
		}
		return nil, re
	}
	if winner.RequestSHA256 != c.RequestSHA256 {
		return nil, ErrIdentity
	}
	if re = r.verifyWinner(ctx, winner, wb); re != nil {
		return nil, re
	}
	return &Result{winner, wb, wi.Modified}, nil
}
func (r *Repository) parent(ctx context.Context, c Commit) error {
	if c.Kind == "full" {
		return nil
	}
	p, e := r.readParent(ctx, c)
	if e != nil {
		return e
	}
	return r.verifyPayload(ctx, p)
}

// readParent validates a differential's exact live full edge and immutable
// request/claim. Payload verification belongs to selection/publication or the
// original input's actual download, not every unrelated differential input.
func (r *Repository) readParent(ctx context.Context, c Commit) (Commit, error) {
	p, b, _, e := r.readCommit(ctx, *c.ParentBackupUID)
	if e != nil {
		return Commit{}, e
	}
	if e = validateParent(c, p); e != nil {
		return Commit{}, e
	}
	if e = r.live(ctx, p, b); e != nil {
		return Commit{}, e
	}
	if _, e = r.requestFor(ctx, p); e != nil {
		return Commit{}, e
	}
	return p, nil
}
func (r *Repository) verifyWinner(ctx context.Context, c Commit, b []byte) error {
	if e := r.live(ctx, c, b); e != nil {
		return e
	}
	if _, e := r.requestFor(ctx, c); e != nil {
		return e
	}
	if e := r.parent(ctx, c); e != nil {
		return e
	}
	return r.verifyPayload(ctx, c)
}
func (r *Repository) verifyPayload(ctx context.Context, c Commit) error {
	if e := r.downloadVerify(ctx, r.attempt(c.BackupUID, c.AttemptID)+"manifest.pg.json", c.ManifestBytes, c.ManifestSHA256, "none", c.ManifestBytes, c.ManifestSHA256); e != nil {
		return e
	}
	for _, a := range c.Artifacts {
		if e := r.downloadVerify(ctx, r.artifact(c, a), a.StoredBytes, a.StoredSHA256, a.Compression, a.RawBytes, a.RawSHA256); e != nil {
			return e
		}
	}
	return nil
}
func (r *Repository) downloadVerify(ctx context.Context, key string, size int64, hash, compression string, raw int64, rawhash string) error {
	f, e := r.temp()
	if e != nil {
		return e
	}
	defer removeFile(f)
	if _, e = r.store.Download(ctx, key, f, s3store.Integrity{Size: size, SHA256: hash}); e != nil {
		return e
	}
	return verifyFile(ctx, f, size, hash, compression, raw, rawhash)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}
func verifyFile(ctx context.Context, f *os.File, size int64, hash, compression string, raw int64, rawhash string) error {
	if f == nil {
		return ErrInvalid
	}
	st, e := f.Stat()
	if e != nil {
		return e
	}
	if !st.Mode().IsRegular() || st.Size() != size {
		return ErrCorrupt
	}
	sh := sha256.New()
	sr := io.NewSectionReader(f, 0, size)
	buf := make([]byte, 128<<10)
	n, e := io.CopyBuffer(sh, contextReader{ctx, sr}, buf)
	if e != nil {
		return e
	}
	if n != size || hex.EncodeToString(sh.Sum(nil)) != hash {
		return ErrCorrupt
	}
	sr = io.NewSectionReader(f, 0, size)
	br := bufio.NewReaderSize(contextReader{ctx, sr}, 128<<10)
	var reader io.Reader = br
	var gz *gzip.Reader
	if compression == "gzip" {
		gz, e = gzip.NewReader(br)
		if e != nil {
			return ErrCorrupt
		}
		defer gz.Close()
		gz.Multistream(false)
		reader = gz
	} else if compression != "none" {
		return ErrInvalid
	}
	rh := sha256.New()
	n, e = io.CopyBuffer(rh, io.LimitReader(reader, raw+1), buf)
	if e != nil || n != raw || hex.EncodeToString(rh.Sum(nil)) != rawhash {
		return ErrCorrupt
	}
	if gz != nil {
		if e = gz.Close(); e != nil {
			return ErrCorrupt
		}
		if _, e = br.ReadByte(); e != io.EOF {
			return ErrCorrupt
		}
	}
	return nil
}

// DownloadInput keeps the holder locked through the entire source read. It
// derives keys from the verified winning commit, never caller-provided paths.
func (h *Hold) DownloadInput(ctx context.Context, uid string, index int, dst *os.File) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return e
	}
	r := h.r
	c, b, _, e := r.readCommit(ctx, uid)
	if e != nil {
		return e
	}
	if e = r.live(ctx, c, b); e != nil {
		return e
	}
	if _, e = r.requestFor(ctx, c); e != nil {
		return e
	}
	if c.Kind == "differential" {
		if _, e = r.readParent(ctx, c); e != nil {
			return e
		}
	}
	key := r.attempt(uid, c.AttemptID) + "manifest.pg.json"
	size, hash, raw, rawhash, comp := c.ManifestBytes, c.ManifestSHA256, c.ManifestBytes, c.ManifestSHA256, "none"
	if index >= 0 {
		if index >= len(c.Artifacts) {
			return ErrInvalid
		}
		a := c.Artifacts[index]
		key = r.artifact(c, a)
		size, hash, raw, rawhash, comp = a.StoredBytes, a.StoredSHA256, a.RawBytes, a.RawSHA256, a.Compression
	} else if index != -1 {
		return ErrInvalid
	}
	if _, e = r.store.Download(ctx, key, dst, s3store.Integrity{Size: size, SHA256: hash}); e != nil {
		return e
	}
	return verifyFile(ctx, dst, size, hash, comp, raw, rawhash)
}
