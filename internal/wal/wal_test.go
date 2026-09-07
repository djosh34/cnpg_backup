package wal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type object struct {
	b        []byte
	metadata map[string]string
}
type backend struct {
	mu           sync.Mutex
	objects      map[string]object
	fault        string
	put          int
	endpoint     string
	partsStarted chan struct{}
	releaseParts chan struct{}
}

func hash(b []byte) string               { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (b *backend) setFault(fault string) { b.mu.Lock(); defer b.mu.Unlock(); b.fault = fault }
func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" && r.URL.Query().Has("uploads") {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>%s</Key><UploadId>fixture-upload</UploadId></InitiateMultipartUploadResult>`, strings.TrimPrefix(r.URL.Path, "/bucket/"))
		return
	}
	if r.Method == "PUT" && r.URL.Query().Get("uploadId") != "" {
		b.partsStarted <- struct{}{}
		select {
		case <-b.releaseParts:
		case <-r.Context().Done():
		}
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	fail := func(code int, kind string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(code)
		fmt.Fprintf(w, "<Error><Code>%s</Code><Message>fixture</Message></Error>", kind)
	}
	if b.fault == "tls" {
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
		return
	}
	if b.fault == "auth" {
		fail(403, "AccessDenied")
		return
	}
	if b.fault == "transient" {
		fail(503, "ServiceUnavailable")
		return
	}
	obj, exists := b.objects[key]
	switch r.Method {
	case "PUT":
		b.put++
		if exists {
			fail(412, "PreconditionFailed")
			return
		}
		data, e := io.ReadAll(r.Body)
		if e != nil {
			return
		}
		if r.Header.Get("If-None-Match") != "*" {
			fail(400, "InvalidRequest")
			return
		}
		if b.fault == "killed" {
			c, _, _ := w.(http.Hijacker).Hijack()
			c.Close()
			return
		}
		metadata := map[string]string{}
		for k, v := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
				metadata[strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")] = v[0]
			}
		}
		b.objects[key] = object{data, metadata}
		if b.fault == "lost" {
			c, _, _ := w.(http.Hijacker).Hijack()
			c.Close()
			return
		}
		w.Header().Set("ETag", `"`+hash(data)+`"`)
		w.WriteHeader(200)
	case "HEAD", "GET":
		if !exists {
			fail(404, "NoSuchKey")
			return
		}
		for k, v := range obj.metadata {
			w.Header().Set("X-Amz-Meta-"+k, v)
		}
		w.Header().Set("ETag", `"`+hash(obj.b)+`"`)
		w.Header().Set("Last-Modified", "Mon, 07 Sep 2026 00:00:00 GMT")
		w.Header().Set("Content-Length", fmt.Sprint(len(obj.b)))
		if r.Method == "GET" {
			w.Write(obj.b)
		}
	default:
		fail(400, "InvalidRequest")
	}
}
func setup(t testing.TB, size int64, wrap ...func(http.Handler) http.Handler) (Files, *backend) {
	t.Helper()
	id := repository.Identity{Schema: 1, RepositoryID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", PostgresMajor: 18, SystemIdentifier: "123456", WALSegmentBytes: size, WriterClusterUID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", CreatedAt: "2026-09-07T00:00:00Z"}
	identity, _ := json.Marshal(id)
	gate, _ := json.Marshal(repository.Gate{Schema: 1, RepositoryID: id.RepositoryID, Generation: "0", Nonce: repository.UUID(), Holders: []repository.Holder{}})
	b := &backend{objects: map[string]object{"v1/" + id.RepositoryID + "/repository.json": {b: identity}, "v1/" + id.RepositoryID + "/gate.json": {b: gate}}}
	var handler http.Handler = b
	if len(wrap) == 1 {
		handler = wrap[0](handler)
	}
	server := httptest.NewTLSServer(handler)
	b.endpoint = server.URL
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	store, e := s3store.New(s3store.Config{Endpoint: server.URL, Bucket: "bucket", Signature: "v4", Addressing: "path", Region: "us-east-1", AccessKey: "fixture", SecretKey: "fixture", CA: ca})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(store.Close)
	work := t.TempDir()
	repo, e := repository.Open(context.Background(), store, id, work)
	if e != nil {
		t.Fatal(e)
	}
	return Files{repo, store, work, "gzip"}, b
}
func source(t *testing.T, b []byte) *os.File {
	t.Helper()
	f, e := os.CreateTemp(t.TempDir(), "source")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	if _, e = f.Write(b); e != nil {
		t.Fatal(e)
	}
	return f
}
func local(t *testing.T) (*os.Root, string) {
	t.Helper()
	d := t.TempDir()
	r, e := os.OpenRoot(d)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r, d
}

func TestRoundtripRetryConflictAndConfinement(t *testing.T) {
	for _, compression := range []string{"none", "gzip"} {
		t.Run(compression, func(t *testing.T) {
			w, b := setup(t, 1<<20)
			w.Compression = compression
			for _, name := range []string{"000000010000000000000001", "000000010000000000000001.partial", "00000002.history", "000000010000000000000001.00000028.backup"} {
				data := []byte("1\t0/100000\tfixture promotion\n")
				if len(name) == 24 || strings.HasSuffix(name, ".partial") {
					data = bytes.Repeat([]byte{42}, 1<<20)
				}
				src := source(t, data)
				ctx := context.Background()
				if e := w.Archive(ctx, name, src); e != nil {
					t.Fatal(name, e)
				}
				// Changes in configured compression do not change raw duplicate identity.
				w.Compression = "none"
				if e := w.Archive(ctx, name, src); e != nil {
					t.Fatal("retry", e)
				}
				dst, dir := local(t)
				if e := w.Restore(ctx, name, dst, "RECOVERYXLOG"); e != nil {
					t.Fatal(e)
				}
				actual, e := os.ReadFile(filepath.Join(dir, "RECOVERYXLOG"))
				if e != nil || !bytes.Equal(actual, data) {
					t.Fatal("independent byte oracle", e)
				}
				other := append([]byte(nil), data...)
				other[0] ^= 1
				if e := w.Archive(ctx, name, source(t, other)); e != ErrConflict {
					t.Fatal("different bytes", e)
				}
				outside := source(t, []byte("outside unchanged"))
				if e := os.Symlink(outside.Name(), filepath.Join(dir, name)); e != nil {
					t.Fatal(e)
				}
				if e := w.Restore(ctx, name, dst, name); e != nil {
					t.Fatal(e)
				}
				unchanged, _ := os.ReadFile(outside.Name())
				if string(unchanged) != "outside unchanged" {
					t.Fatal("symlink escape")
				}
				for _, bad := range []string{"../escape", "/escape", "x/../RECOVERYXLOG", "wal/neighbor"} {
					if e := w.Restore(ctx, name, dst, bad); e != ErrInvalid {
						t.Fatal("unsafe destination", bad, e)
					}
				}
				// Metadata alone cannot bless bytes changed independently in storage.
				key, _ := w.Repository.WALKey(name)
				b.mu.Lock()
				o := b.objects[key]
				o.b[0] ^= 1
				b.objects[key] = o
				b.mu.Unlock()
				if e := w.Archive(ctx, name, src); e == nil {
					t.Fatal("corrupted same-hash metadata acknowledged")
				}
			}
		})
	}
}
func TestFaultsNeverAcknowledgeOrPublish(t *testing.T) {
	for _, fault := range []string{"killed", "lost", "transient"} {
		t.Run(fault, func(t *testing.T) {
			w, b := setup(t, 1<<20)
			name := "000000010000000000000001"
			ctx := context.Background()
			src := source(t, bytes.Repeat([]byte{3}, 1<<20))
			b.setFault(fault)
			e := w.Archive(ctx, name, src)
			if fault == "lost" {
				if e != nil {
					t.Fatal("durable lost-response reconciliation", e)
				}
			} else if e == nil {
				t.Fatal("premature success")
			}
			b.setFault("")
			if e = w.Archive(ctx, name, src); e != nil {
				t.Fatal("retry after cleared fault", e)
			}
			root, dir := local(t)
			os.WriteFile(filepath.Join(dir, "RECOVERYXLOG"), []byte("previous"), 0600)
			b.setFault("transient")
			if e = w.Restore(ctx, name, root, "RECOVERYXLOG"); e == nil || s3store.Is(e, s3store.NotFound) {
				t.Fatal("outage became EOF/success", e)
			}
			prior, _ := os.ReadFile(filepath.Join(dir, "RECOVERYXLOG"))
			if string(prior) != "previous" {
				t.Fatal("failed restore published")
			}
		})
	}
}
func TestMissingRetiredLimitsAndExpansion(t *testing.T) {
	w, b := setup(t, 64<<20)
	ctx := context.Background()
	root, _ := local(t)
	name := "000000010000000000000001"
	if e := w.Restore(ctx, name, root, name); !s3store.Is(e, s3store.NotFound) {
		t.Fatal("authenticated absence", e)
	}
	if e := w.Archive(ctx, name, source(t, []byte("short"))); e != ErrInvalid {
		t.Fatal("actual nondefault segment size", e)
	}
	history := "00000002.history"
	src := source(t, []byte("1\t0/100000\tfixture\n"))
	if e := w.Archive(ctx, history, src); e != nil {
		t.Fatal(e)
	}
	key, _ := w.Repository.WALKey(history)
	b.mu.Lock()
	o := b.objects[key]
	o.b = append(o.b, o.b...)
	o.metadata["cnpg-stored-sha256"] = hash(o.b)
	b.objects[key] = o
	b.mu.Unlock()
	if e := w.Restore(ctx, history, root, history); e != ErrCorrupt {
		t.Fatal("multiple gzip members", e)
	}
	b.mu.Lock()
	o.metadata["cnpg-format"] = "wal-retired-v1"
	b.objects[key] = o
	b.mu.Unlock()
	if e := w.Archive(ctx, history, src); e != ErrExpired {
		t.Fatal("tombstone reanimated", e)
	}
	for _, name := range []string{"../escape", "000000000000000000000001", "000000010000000000000040", "000000010000000000000001.04000000.backup", "00000002.history.gz"} {
		if _, _, e := w.Limits(name); e == nil {
			t.Fatal("invalid name", name)
		}
	}
}

// Fixed-operation seeded replay executes real orchestration AND real HTTP SDK.
// The independent oracle keeps original raw bytes, not production metadata.
func TestSeededWALTrace(t *testing.T) {
	for _, seed := range []int64{19, 1806, 42} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			w, b := setup(t, 1<<20)
			random := rand.New(rand.NewSource(seed))
			oracle := map[string][]byte{}
			for op := 0; op < 24; op++ {
				name := fmt.Sprintf("%08X.history", random.Intn(5)+2)
				data := []byte(fmt.Sprintf("1\t0/100000\tseed %d operation %d\n", seed, op))
				old, exists := oracle[name]
				if exists && random.Intn(2) == 0 {
					data = old
				}
				b.setFault("")
				killed := random.Intn(5) == 0
				if killed {
					b.setFault("killed")
				}
				e := w.Archive(context.Background(), name, source(t, data))
				b.setFault("")
				shouldSucceed := exists && bytes.Equal(old, data) || !exists && !killed
				if (e == nil) != shouldSucceed {
					t.Fatalf("seed=%d op=%d killed=%v: %v", seed, op, killed, e)
				}
				if e == nil && !exists {
					oracle[name] = append([]byte(nil), data...)
				}
				if expected, ok := oracle[name]; ok {
					root, dir := local(t)
					if e = w.Restore(context.Background(), name, root, name); e != nil {
						t.Fatal(e)
					}
					actual, _ := os.ReadFile(filepath.Join(dir, name))
					if !bytes.Equal(actual, expected) {
						t.Fatal("oracle caught byte divergence")
					}
				}
			}
		})
	}
}
func FuzzWALNames(f *testing.F) {
	for _, s := range []string{"00000002.history", "../escape", "000000010000000000000001", "000000010000000000000001.partial"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if repository.ValidateWALName(name, 64<<20) != nil {
			return
		}
		if filepath.Base(name) != name || strings.ContainsAny(name, "/\\\\") || !repository.ValidWALFilename(name) {
			t.Fatal("unsafe accepted WAL name", name)
		}
	})
}
