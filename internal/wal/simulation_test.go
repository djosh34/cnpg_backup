package wal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Controlled storage durability/response completion for real WAL orchestration.
// No scheduler/fault switches exist in the product. Unused repository methods
// deliberately have no implementation: unexpected orchestration fails loudly.
type simulationStore struct {
	repository.Storage
	objects map[string]object
	fault   string
	discard bool
}

func (s *simulationStore) Head(ctx context.Context, key string) (s3store.Info, error) {
	if e := ctx.Err(); e != nil {
		return s3store.Info{}, e
	}
	if s.fault == "transient" {
		return s3store.Info{}, &s3store.Error{Kind: s3store.Transient}
	}
	o, ok := s.objects[key]
	if !ok {
		return s3store.Info{}, &s3store.Error{Kind: s3store.Unknown}
	}
	return s3store.Info{Key: key, Size: int64(len(o.b)), ETag: hash(o.b), Metadata: o.metadata}, nil
}
func (s *simulationStore) Read(ctx context.Context, key string, max int64) ([]byte, s3store.Info, error) {
	if e := ctx.Err(); e != nil {
		return nil, s3store.Info{}, e
	}
	o, ok := s.objects[key]
	if !ok {
		return nil, s3store.Info{}, &s3store.Error{Kind: s3store.NotFound}
	}
	info, e := s.Head(ctx, key)
	if e != nil {
		return nil, info, e
	}
	if int64(len(o.b)) > max {
		return nil, info, ErrCorrupt
	}
	return append([]byte(nil), o.b...), info, nil
}
func (s *simulationStore) PutFile(ctx context.Context, key string, f *os.File, want s3store.Integrity, condition s3store.Condition, metadata map[string]string) (s3store.Info, error) {
	if e := ctx.Err(); e != nil {
		return s3store.Info{}, e
	}
	if !condition.Create || condition.Match != "" {
		return s3store.Info{}, ErrInvalid
	}
	if _, ok := s.objects[key]; ok {
		return s3store.Info{}, &s3store.Error{Kind: s3store.Precondition}
	}
	if s.fault == "killed" {
		return s3store.Info{}, &s3store.Error{Kind: s3store.Canceled, Ambiguous: true}
	}
	b, e := io.ReadAll(io.NewSectionReader(f, 0, want.Size))
	if e != nil {
		return s3store.Info{}, e
	}
	if int64(len(b)) != want.Size || hash(b) != want.SHA256 {
		return s3store.Info{}, ErrCorrupt
	}
	if !s.discard {
		s.objects[key] = object{b: b, metadata: metadata}
	}
	if s.fault == "lost" {
		return s3store.Info{}, &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
	}
	return s3store.Info{Key: key, Size: want.Size, ETag: hash(b), Metadata: metadata}, nil
}
func (s *simulationStore) DownloadWAL(ctx context.Context, key string, f *os.File, want s3store.Integrity) (s3store.Info, error) {
	info, e := s.Head(ctx, key)
	if e != nil {
		return info, e
	}
	o := s.objects[key]
	if info.Size != want.Size || hash(o.b) != want.SHA256 {
		return info, ErrCorrupt
	}
	if e = f.Truncate(0); e != nil {
		return info, e
	}
	if _, e = f.Seek(0, 0); e != nil {
		return info, e
	}
	if _, e = f.Write(o.b); e != nil {
		return info, e
	}
	return info, f.Sync()
}
func simulatedWAL(t testing.TB) (Files, *simulationStore) {
	t.Helper()
	id := repository.Identity{Schema: 1, RepositoryID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", PostgresMajor: 18, SystemIdentifier: "123456", WALSegmentBytes: 1 << 20, WriterClusterUID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", CreatedAt: "2026-09-07T00:00:00Z"}
	identity, _ := json.Marshal(id)
	gate, _ := json.Marshal(repository.Gate{Schema: 1, RepositoryID: id.RepositoryID, Generation: "0", Nonce: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Holders: []repository.Holder{}})
	store := &simulationStore{objects: map[string]object{"v1/" + id.RepositoryID + "/repository.json": {b: identity}, "v1/" + id.RepositoryID + "/gate.json": {b: gate}}}
	workspace := t.TempDir()
	repo, e := repository.Open(context.Background(), store, id, workspace)
	if e != nil {
		t.Fatal(e)
	}
	return Files{repo, store, workspace, "gzip"}, store
}
func TestDeterministicWALDurabilityResponseTrace(t *testing.T) {
	trace := func(seed int64) string {
		w, s := simulatedWAL(t)
		random := rand.New(rand.NewSource(seed))
		oracle := map[string][]byte{}
		var trace bytes.Buffer
		for op := 0; op < 24; op++ {
			name := fmt.Sprintf("%08X.history", 2+random.Intn(8))
			raw := []byte(fmt.Sprintf("1\t0/100000\tseed %d operation %d\n", seed, op))
			prior, exists := oracle[name]
			if exists && random.Intn(2) == 0 {
				raw = prior
			}
			fault := []string{"", "killed", "lost"}[random.Intn(3)]
			s.fault = fault
			e := w.Archive(context.Background(), name, source(t, raw))
			s.fault = ""
			expected := exists && bytes.Equal(prior, raw) || !exists && fault != "killed"
			if (e == nil) != expected {
				t.Fatalf("seed=%d operation=%d: no-early-ack oracle failed", seed, op)
			}
			if e == nil && !exists {
				oracle[name] = append([]byte(nil), raw...)
			}
			fmt.Fprintf(&trace, "%d %s %s %t\n", op, name, fault, e == nil)
			if op%8 == 7 { // restart discards Repository state, preserving durable objects
				repo, e := repository.Open(context.Background(), s, w.Repository.Identity(), w.Workspace)
				if e != nil {
					t.Fatal(e)
				}
				w.Repository = repo
			}
			if expectedRaw, ok := oracle[name]; ok {
				root, dir := local(t)
				if e := w.Restore(context.Background(), name, root, name); e != nil {
					t.Fatal(e)
				}
				actual, _ := os.ReadFile(filepath.Join(dir, name))
				if !bytes.Equal(actual, expectedRaw) {
					t.Fatal("independent restore oracle")
				}
			}
		}
		return trace.String()
	}
	for _, seed := range []int64{19, 1806, 42} {
		first := trace(seed)
		if second := trace(seed); first != second {
			t.Fatal("non-reproducible trace", seed)
		}
	}
}
func TestFalseStorageAcknowledgmentNegativeControl(t *testing.T) {
	w, s := simulatedWAL(t)
	s.discard = true
	if e := w.Archive(context.Background(), "00000002.history", source(t, []byte("not durable"))); e == nil {
		t.Fatal("oracle failed to detect acknowledged-but-absent bytes")
	}
}
