package s3store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func config(endpoint string, ca []byte) Config {
	return Config{Endpoint: endpoint, Bucket: "test-bucket", Prefix: "test", Signature: "v4", Addressing: "path", Region: "us-east-1", AccessKey: "test-access-secret", SecretKey: "test-secret-secret", CA: ca}
}
func testStore(t *testing.T, h http.Handler) (*Store, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	s, e := New(config(server.URL, ca))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	return s, server
}
func spool(t *testing.T, b []byte) (*os.File, Integrity) {
	t.Helper()
	f, e := os.CreateTemp(t.TempDir(), "spool")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	if _, e = f.Write(b); e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(b)
	return f, Integrity{int64(len(b)), hex.EncodeToString(h[:])}
}
func sparse(t *testing.T, size int64) (*os.File, Integrity) {
	t.Helper()
	f, e := os.CreateTemp(t.TempDir(), "sparse")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	if e = f.Truncate(size); e != nil {
		t.Fatal(e)
	}
	h := sha256.New()
	if _, e = io.Copy(h, io.NewSectionReader(f, 0, size)); e != nil {
		t.Fatal(e)
	}
	return f, Integrity{size, hex.EncodeToString(h.Sum(nil))}
}
func mustKind(t *testing.T, e error, k Kind, ambiguous bool) {
	t.Helper()
	var v *Error
	if !errors.As(e, &v) || v.Kind != k || v.Ambiguous != ambiguous {
		t.Fatalf("got %v, want %s ambiguous=%v", e, k, ambiguous)
	}
}
func s3error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test-secret-secret test-access-secret</Message></Error>", code)
}

type fakeObject struct {
	body     []byte
	etag     string
	metadata http.Header
}
type objectServer struct {
	mu                  sync.Mutex
	objects             map[string]fakeObject
	puts, deletes       int
	losePut, loseDelete bool
	ignoreConditions    bool
}

func (o *objectServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.objects == nil {
		o.objects = map[string]fakeObject{}
	}
	v, exists := o.objects[r.URL.Path]
	switch r.Method {
	case "PUT":
		o.puts++
		if !o.ignoreConditions && (r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != "\""+v.etag+"\"")) {
			s3error(w, 412, "PreconditionFailed")
			return
		}
		b, e := io.ReadAll(io.LimitReader(r.Body, 2<<20))
		if e != nil {
			return
		}
		h := sha256.Sum256(b)
		v = fakeObject{b, hex.EncodeToString(h[:16]), r.Header.Clone()}
		o.objects[r.URL.Path] = v
		if o.losePut {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		w.Header().Set("ETag", "\""+v.etag+"\"")
	case "GET", "HEAD":
		if r.URL.Query().Get("list-type") == "2" {
			fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
			for key, v := range o.objects {
				if strings.HasPrefix(key, "/test-bucket/"+r.URL.Query().Get("prefix")) {
					fmt.Fprintf(w, "<Contents><Key>%s</Key><ETag>\"%s\"</ETag><Size>%d</Size><LastModified>2026-09-07T00:00:00Z</LastModified></Contents>", strings.TrimPrefix(key, "/test-bucket/"), v.etag, len(v.body))
				}
			}
			fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		if !exists {
			s3error(w, 404, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", "\""+v.etag+"\"")
		w.Header().Set("Content-Length", strconv.Itoa(len(v.body)))
		w.Header().Set("Last-Modified", "Mon, 07 Sep 2026 00:00:00 GMT")
		for key, values := range v.metadata {
			if strings.HasPrefix(key, "X-Amz-Meta-") {
				w.Header()[key] = values
			}
		}
		if r.Method == "GET" {
			w.Write(v.body)
		}
	case "DELETE":
		o.deletes++
		delete(o.objects, r.URL.Path)
		if o.loseDelete {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		w.WriteHeader(204)
	default:
		s3error(w, 400, "InvalidRequest")
	}
}

func TestConfigAndSnapshot(t *testing.T) {
	s, server := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s3error(w, 403, "AccessDenied") }))
	_ = s
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	tests := []func(*Config){func(c *Config) { c.Endpoint = "http://127.0.0.1" }, func(c *Config) { c.Endpoint = "https://user:password@host" }, func(c *Config) { c.Endpoint += "/path" }, func(c *Config) { c.Endpoint += "?query" }, func(c *Config) { c.Endpoint += "#fragment" }, func(c *Config) { c.AccessKey = "" }, func(c *Config) { c.SecretKey = "" }, func(c *Config) { c.Signature = "auto" },
		func(c *Config) { c.Signature = "v2"; c.Endpoint = "https://s3.amazonaws.com" },
		func(c *Config) { c.Bucket = "bucket--use1-az1--x-s3" },
		func(c *Config) { c.CA = append(append([]byte{}, c.CA...), []byte("garbage")...) }, func(c *Config) { c.Signature = "v2"; c.SessionToken = "token" }, func(c *Config) { c.Addressing = "auto" }, func(c *Config) { c.Region = "" }, func(c *Config) { c.CA = []byte("garbage") }, func(c *Config) { c.Prefix = "../escape" }, func(c *Config) { c.PartWorkers = 3 }, func(c *Config) { c.WALUploads = 3 }, func(c *Config) { c.ConnectTimeout = time.Nanosecond }}
	for i, change := range tests {
		c := config(server.URL, ca)
		change(&c)
		_, e := New(c)
		if !Is(e, Invalid) {
			t.Errorf("case %d: %v", i, e)
		}
	}
	for _, signature := range []string{"v2", "v4"} {
		t.Run(signature, func(t *testing.T) {
			var auth string
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth = r.Header.Get("Authorization")
				s3error(w, 403, "AccessDenied")
			}))
			defer srv.Close()
			roots := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
			c := config(srv.URL, roots)
			c.Signature = signature
			client, e := New(c)
			if e != nil {
				t.Fatal(e)
			}
			defer client.Close()
			c.AccessKey = "changed"
			for i := range roots {
				roots[i] = 0
			}
			_, _, e = client.Read(context.Background(), "key", 100)
			mustKind(t, e, Auth, false)
			prefix := "AWS "
			if signature == "v4" {
				prefix = "AWS4-HMAC-SHA256 "
			}
			if !strings.HasPrefix(auth, prefix) || !strings.Contains(auth, "test-access-secret") {
				t.Fatal("incorrect snapshot signer")
			}
			if strings.Contains(e.Error(), "secret") {
				t.Fatal("credential leak")
			}
		})
	}
}

func TestConditionalAndResponseLoss(t *testing.T) {
	backend := &objectServer{}
	s, _ := testStore(t, backend)
	ctx := context.Background()
	f, expected := spool(t, []byte("original"))
	v, e := s.PutFile(ctx, "wal", f, expected, Condition{Create: true}, map[string]string{"cnpg-stored-sha256": expected.SHA256})
	if e != nil {
		t.Fatal(e)
	}
	f2, expected2 := spool(t, []byte("replacement"))
	_, e = s.PutFile(ctx, "wal", f2, expected2, Condition{Create: true}, nil)
	mustKind(t, e, Precondition, false)
	b, head, e := s.Read(ctx, "wal", 100)
	if e != nil || string(b) != "original" || head.Metadata["cnpg-stored-sha256"] != expected.SHA256 {
		t.Fatalf("read %v %+v", e, head)
	}
	_, e = s.PutFile(ctx, "wal", f2, expected2, Condition{Match: v.ETag}, nil)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.PutFile(ctx, "wal", f, expected, Condition{Match: v.ETag}, nil)
	mustKind(t, e, Precondition, false)
	backend.mu.Lock()
	backend.losePut = true
	before := backend.puts
	backend.mu.Unlock()
	_, e = s.PutFile(ctx, "lost", f, expected, Condition{Create: true}, nil)
	mustKind(t, e, Transient, true)
	backend.mu.Lock()
	if backend.puts != before+1 {
		t.Fatal("hidden PUT retry")
	}
	backend.mu.Unlock()
	b, _, e = s.Read(ctx, "lost", 100)
	if e != nil || string(b) != "original" {
		t.Fatal("remote commit not present", e)
	}
	backend.mu.Lock()
	backend.losePut = false
	backend.loseDelete = true
	backend.mu.Unlock()
	e = s.Delete(ctx, "lost")
	mustKind(t, e, Transient, true)
	backend.mu.Lock()
	if backend.deletes != 1 {
		t.Fatal("hidden DELETE retry")
	}
	backend.mu.Unlock()
	_, _, e = s.Read(ctx, "lost", 100)
	mustKind(t, e, NotFound, false)
}

func TestVerifiedStreamingAndMissing(t *testing.T) {
	backend := &objectServer{}
	s, _ := testStore(t, backend)
	f, expected := spool(t, []byte("data"))
	ctx := context.Background()
	_, e := s.PutFile(ctx, "data", f, expected, Condition{Create: true}, nil)
	if e != nil {
		t.Fatal(e)
	}
	dst, _ := spool(t, nil)
	_, e = s.Download(ctx, "data", dst, expected)
	if e != nil {
		t.Fatal(e)
	}
	expected.SHA256 = strings.Repeat("0", 64)
	_, e = s.Download(ctx, "data", dst, expected)
	mustKind(t, e, Corrupt, false)
	_, _, e = s.Read(ctx, "absent", 100)
	mustKind(t, e, NotFound, false)
	_, e = s.Head(ctx, "absent")
	mustKind(t, e, HeadMissing, false)
	for _, tc := range []struct {
		status int
		body   string
		kind   Kind
	}{{404, "not xml", Unknown}, {404, "<Error><Code>NoSuchBucket</Code></Error>", Unknown}, {403, "<Error><Code>AccessDenied</Code></Error>", Auth}, {200, "x", Corrupt}} {
		t.Run(fmt.Sprint(tc.status, tc.body), func(t *testing.T) {
			store, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			_, _, e := store.Read(ctx, "missing", 100)
			mustKind(t, e, tc.kind, false)
		})
	}
}

func TestTLSRedirectProxyAndRateLimit(t *testing.T) {
	var requests atomic.Int32
	s, srv := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); s3error(w, 429, "SlowDown") }))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, _, e := s.Read(ctx, "key", 100)
	mustKind(t, e, Transient, false)
	if requests.Load() != 5 {
		t.Fatalf("attempts %d", requests.Load())
	}
	untrusted, e := New(config(srv.URL, nil))
	if e != nil {
		t.Fatal(e)
	}
	defer untrusted.Close()
	_, _, e = untrusted.Read(ctx, "key", 100)
	mustKind(t, e, TLS, false)
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	t.Setenv("HTTPS_PROXY", target.URL)
	t.Setenv("HTTP_PROXY", target.URL)
	store, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	_, _, e = store.Read(context.Background(), "key", 100)
	mustKind(t, e, Unsupported, false)
	if redirected.Load() != 0 {
		t.Fatal("redirect/proxy followed")
	}
}

func TestPaginationAndFailures(t *testing.T) {
	for _, mode := range []string{"complete", "partial-error", "repeat-token", "malformed", "capacity", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var pages atomic.Int32
			s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := pages.Add(1)
				if mode == "partial-error" && n == 2 {
					s3error(w, 403, "AccessDenied")
					return
				}
				if mode == "malformed" {
					fmt.Fprint(w, "<wrong/>")
					return
				}
				if n == 1 {
					fmt.Fprint(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>test/a</Key><ETag>etag</ETag><Size>1</Size></Contents></ListBucketResult>")
					return
				}
				if mode == "repeat-token" {
					fmt.Fprint(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>")
					return
				}
				fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>test/b</Key><ETag>etag</ETag><Size>2</Size></Contents></ListBucketResult>")
			}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			count := 0
			max := 10
			if mode == "capacity" {
				max = 1
			}
			e := s.List(ctx, "", max, func(v Info) error {
				count++
				if mode == "cancel" {
					cancel()
				}
				return nil
			})
			switch mode {
			case "complete":
				if e != nil || count != 2 || pages.Load() != 2 {
					t.Fatalf("%v %d %d", e, count, pages.Load())
				}
			case "partial-error":
				mustKind(t, e, Auth, false)
			case "repeat-token", "malformed":
				mustKind(t, e, Corrupt, false)
			case "capacity":
				mustKind(t, e, Limit, false)
			case "cancel":
				mustKind(t, e, Canceled, false)
			}
		})
	}
}

func TestMultipartInterruptedNoAbortAndIndependentWAL(t *testing.T) {
	var active, peak, parts, aborts atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Query().Has("uploads") {
			fmt.Fprint(w, "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test/artifact</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>")
			return
		}
		if r.Method == "PUT" && r.URL.Query().Has("partNumber") {
			parts.Add(1)
			n := active.Add(1)
			defer active.Add(-1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			io.Copy(io.Discard, r.Body)
			s3error(w, 503, "ServiceUnavailable")
			return
		}
		if r.Method == "DELETE" {
			aborts.Add(1)
			w.WriteHeader(204)
			return
		}
		if r.Method == "PUT" {
			io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", "wal-etag")
			return
		}
		t.Error("unexpected request", r.Method, r.URL.RawQuery)
	}))
	f, expected := sparse(t, PartSize*2+1)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	done := make(chan error, 1)
	go func() {
		u, _, e := s.UploadFile(context.Background(), "artifact", f, expected, nil)
		if u.ID != "upload" {
			done <- fmt.Errorf("missing upload ID")
			return
		}
		done <- e
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	wal, integrity := spool(t, []byte("wal"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, e := s.PutFile(ctx, "wal", wal, integrity, Condition{Create: true}, nil); e != nil {
		t.Fatal("WAL starved", e)
	}
	close(release)
	e := <-done
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("134217729-byte artifact, two 64MiB workers: total Go allocations during transfer %d bytes", allocated)
	if allocated > 32<<20 {
		t.Fatalf("object-sized buffering: allocated %d", allocated)
	}
	var outcome *Error
	if !errors.As(e, &outcome) || !outcome.Ambiguous {
		t.Fatalf("multipart error %v", e)
	}
	if peak.Load() != 2 || parts.Load() != 2 || aborts.Load() != 0 {
		t.Fatalf("peak=%d parts=%d aborts=%d", peak.Load(), parts.Load(), aborts.Load())
	}
	if e = s.Abort(context.Background(), Upload{Key: "artifact", ID: "upload"}); e != nil {
		t.Fatal(e)
	}
	if aborts.Load() != 1 {
		t.Fatal("explicit abort missing")
	}
}

func TestCancellationAndBoundedBodies(t *testing.T) {
	entered := make(chan struct{}, 16)
	finish := make(chan struct{})
	defer close(finish)
	var active, peak atomic.Int32
	s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-finish:
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			_, _, e := s.Read(ctx, "key", 100)
			if e == nil {
				t.Error("canceled read succeeded")
			}
		})
	}
	for range 8 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("requests not dispatched")
		}
	}
	cancel()
	wg.Wait()
	if peak.Load() > 8 {
		t.Fatal("unbounded HTTP concurrency")
	}
	store, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "token")
		w.Header().Set("Content-Length", strconv.Itoa(responseLimit+1))
		io.CopyN(w, zeroReader{}, responseLimit+1)
	}))
	e := store.Delete(context.Background(), "key")
	mustKind(t, e, Limit, true)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestCapabilityProbe(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		backend := &objectServer{ignoreConditions: ignore}
		s, _ := testStore(t, backend)
		keys, e := s.CheckConditions(context.Background(), "probes", t.TempDir())
		if len(keys) != 2 {
			t.Fatal("lost cleanup keys")
		}
		if ignore {
			mustKind(t, e, Unsupported, false)
		} else if e != nil {
			t.Fatal(e)
		}
		if backend.deletes != 0 {
			t.Fatal("probe autonomously deleted")
		}
	}
}

func TestBucketChecks(t *testing.T) {
	for _, mode := range []string{"safe", "versioned", "suspended", "lifecycle", "lock", "denied", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "denied" {
					s3error(w, 403, "AccessDenied")
					return
				}
				if mode == "malformed" {
					fmt.Fprint(w, "<wrong/>")
					return
				}
				switch {
				case r.URL.Query().Has("versioning"):
					status := ""
					if mode == "versioned" {
						status = "Enabled"
					}
					if mode == "suspended" {
						status = "Suspended"
					}
					fmt.Fprintf(w, "<VersioningConfiguration><Status>%s</Status></VersioningConfiguration>", status)
				case r.URL.Query().Has("lifecycle"):
					if mode == "lifecycle" {
						fmt.Fprint(w, "<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>")
					} else {
						s3error(w, 404, "NoSuchLifecycleConfiguration")
					}
				case r.URL.Query().Has("object-lock"):
					if mode == "lock" {
						fmt.Fprint(w, "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>")
					} else {
						s3error(w, 404, "ObjectLockConfigurationNotFoundError")
					}
				default:
					t.Error("unexpected bucket request")
				}
			}))
			e := s.CheckBucketSafety(context.Background())
			if mode == "safe" {
				if e != nil {
					t.Fatal(e)
				}
			} else if mode == "denied" {
				mustKind(t, e, Auth, false)
			} else if mode == "malformed" {
				if e == nil {
					t.Fatal("malformed bucket configuration accepted")
				}
			} else {
				mustKind(t, e, Unsupported, false)
			}
		})
	}
}

func TestNoFalseSuccessOnShortMutationResponse(t *testing.T) {
	for _, method := range []string{"PUT", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Length", "20")
				w.Header().Set("ETag", "etag")
				w.WriteHeader(200)
				w.Write([]byte("short"))
			}))
			var e error
			if method == "PUT" {
				f, expected := spool(t, []byte("body"))
				_, e = s.PutFile(context.Background(), "key", f, expected, Condition{Create: true}, nil)
			} else {
				e = s.Delete(context.Background(), "key")
			}
			mustKind(t, e, Transient, true)
			if calls.Load() != 1 {
				t.Fatal("hidden mutation retry")
			}
		})
	}
}

func TestFileInputValidation(t *testing.T) {
	backend := &objectServer{}
	s, _ := testStore(t, backend)
	f, v := spool(t, []byte("abc"))
	v.SHA256 = strings.Repeat("0", 64)
	_, e := s.PutFile(context.Background(), "key", f, v, Condition{Create: true}, nil)
	mustKind(t, e, Corrupt, false)
	v.Size++
	_, e = s.PutFile(context.Background(), "key", f, v, Condition{Create: true}, nil)
	mustKind(t, e, Invalid, false)
	if backend.puts != 0 {
		t.Fatal("invalid file dispatched")
	}
}
