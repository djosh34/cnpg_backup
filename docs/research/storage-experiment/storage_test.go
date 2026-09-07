package experiment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Research only: never use this harness with a non-disposable bucket/endpoint.
type transport struct {
	base                http.RoundTripper
	mu                  sync.Mutex
	requests            []string
	dropMethod, dropKey string
	dropped             bool
}

func (x *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	x.mu.Lock()
	// No authorization headers or credential material enter evidence.
	x.requests = append(x.requests, r.Method+" "+r.URL.Path+" "+r.URL.RawQuery+" none="+r.Header.Get("If-None-Match")+" match="+r.Header.Get("If-Match"))
	drop := !x.dropped && r.Method == x.dropMethod && strings.HasSuffix(r.URL.Path, x.dropKey)
	if drop {
		x.dropped = true
	}
	x.mu.Unlock()
	resp, err := x.base.RoundTrip(r)
	if drop && err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, errors.New("experiment: response lost AFTER remote response")
	}
	return resp, err
}
func (x *transport) arm(method, key string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.dropMethod = method
	x.dropKey = key
	x.dropped = false
}
func client(t *testing.T, sig string, trust bool) (*minio.Client, *transport) {
	t.Helper()
	ca, err := os.ReadFile(os.Getenv("EXPERIMENT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if trust && !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid CA")
	}
	tr := &transport{base: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DisableKeepAlives: true, ResponseHeaderTimeout: 10 * time.Second}}
	ak, sk := os.Getenv("MINIO_ROOT_USER"), os.Getenv("MINIO_ROOT_PASSWORD")
	if ak == "" || sk == "" {
		t.Fatal("missing disposable credentials")
	}
	creds := credentials.NewStaticV4(ak, sk, "")
	if sig == "v2" {
		creds = credentials.NewStaticV2(ak, sk, "")
	}
	c, err := minio.New(os.Getenv("EXPERIMENT_ENDPOINT"), &minio.Options{Creds: creds, Secure: true, Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: tr, MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	return c, tr
}
func opts() minio.PutObjectOptions {
	return minio.PutObjectOptions{DisableMultipart: true, SendContentMd5: true, ContentType: "application/octet-stream"}
}
func create() minio.PutObjectOptions { o := opts(); o.SetMatchETagExcept("*"); return o }
func put(c *minio.Client, b, k string, v []byte, o minio.PutObjectOptions) (minio.UploadInfo, error) {
	return c.PutObject(context.Background(), b, k, bytes.NewReader(v), int64(len(v)), o)
}
func get(t *testing.T, c *minio.Client, b, k string) []byte {
	t.Helper()
	o, e := c.GetObject(context.Background(), b, k, minio.GetObjectOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer o.Close()
	v, e := io.ReadAll(io.LimitReader(o, 32<<20))
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if minio.ToErrorResponse(err).Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestSDK(t *testing.T) {
	if os.Getenv("EXPERIMENT_ENDPOINT") == "" {
		t.Skip("run run.sh for disposable real-MinIO experiment")
	}
	for _, sig := range []string{"v2", "v4"} {
		t.Run(sig, func(t *testing.T) {
			c, tr := client(t, sig, true)
			ctx := context.Background()
			b := "storage-experiment-" + sig
			if err := c.MakeBucket(ctx, b, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
				t.Fatal(err)
			}
			bad, _ := client(t, sig, false)
			_, e := bad.StatObject(ctx, b, "absent", minio.StatObjectOptions{})
			if e == nil || !strings.Contains(e.Error(), "certificate") {
				t.Fatalf("private CA negative control: %v", e)
			}
			one := []byte("first immutable WAL or commit")
			two := []byte("different content")
			u, e := put(c, b, "immutable", one, create())
			if e != nil {
				t.Fatal(e)
			}
			_, e = put(c, b, "immutable", two, create())
			requireCode(t, e, "PreconditionFailed")
			if !bytes.Equal(get(t, c, b, "immutable"), one) {
				t.Fatal("clobber")
			}
			cas := opts()
			cas.SetMatchETag(u.ETag)
			_, e = put(c, b, "immutable", two, cas)
			if e != nil {
				t.Fatal(e)
			}
			_, e = put(c, b, "immutable", one, cas)
			requireCode(t, e, "PreconditionFailed")
			t.Log("PASS private CA rejection/trust, conditional create, ETag CAS and stale CAS")

			var wins atomic.Int32
			var wg sync.WaitGroup
			errs := make(chan error, 16)
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_, e := put(c, b, "competing-backup-uid", []byte(fmt.Sprintf("candidate-%d", i)), create())
					if e == nil {
						wins.Add(1)
					} else if minio.ToErrorResponse(e).Code != "PreconditionFailed" {
						errs <- e
					}
				}(i)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				t.Fatal(e)
			}
			if wins.Load() != 1 {
				t.Fatalf("winners=%d", wins.Load())
			}
			t.Log("PASS 16 concurrent creates, one winner")

			tr.arm("PUT", "lost-put")
			_, e = put(c, b, "lost-put", one, create())
			if e == nil {
				t.Fatal("lost response not injected")
			}
			if !bytes.Equal(get(t, c, b, "lost-put"), one) {
				t.Fatal("missing durable lost-response write")
			}
			_, e = put(c, b, "lost-put", one, create())
			requireCode(t, e, "PreconditionFailed")
			t.Log("PASS remote-success/lost-response reconciliation by GET, retry is 412")

			// Force 3 multipart pieces; inspect wire requests, not just object size.
			data := bytes.Repeat([]byte("0123456789abcdef"), (12<<20)/16)
			mp := minio.PutObjectOptions{PartSize: 5 << 20, NumThreads: 2, SendContentMd5: true, ConcurrentStreamParts: true}
			u, e = put(c, b, "attempt-unique/data", data, mp)
			if e != nil {
				t.Fatal(e)
			}
			if u.Size != int64(len(data)) || sha256.Sum256(get(t, c, b, "attempt-unique/data")) != sha256.Sum256(data) {
				t.Fatal("multipart corruption")
			}
			// A separate unique attempt with an ambiguous successful completion.
			tr.arm("POST", "attempt-lost/data")
			_, e = put(c, b, "attempt-lost/data", data, mp)
			// arm hits initiation first, deliberately separate completion loss below.
			if e == nil {
				t.Fatal("initiation response-loss injection did not fire")
			}
			// Explicit Core gives an upload ID and direct control over completion and abort.
			core := minio.Core{Client: c}
			id, e := core.NewMultipartUpload(ctx, b, "attempt-complete/data", minio.PutObjectOptions{})
			if e != nil {
				t.Fatal(e)
			}
			p, e := core.PutObjectPart(ctx, b, "attempt-complete/data", id, 1, bytes.NewReader(one), int64(len(one)), minio.PutObjectPartOptions{})
			if e != nil {
				t.Fatal(e)
			}
			tr.arm("POST", "attempt-complete/data")
			_, e = core.CompleteMultipartUpload(ctx, b, "attempt-complete/data", id, []minio.CompletePart{{PartNumber: 1, ETag: p.ETag}}, minio.PutObjectOptions{})
			if e == nil {
				t.Fatal("lost completion response did not fire")
			}
			if !bytes.Equal(get(t, c, b, "attempt-complete/data"), one) {
				t.Fatal("ambiguous completed MPU missing")
			}
			id, e = core.NewMultipartUpload(ctx, b, "attempt-abort/data", minio.PutObjectOptions{})
			if e != nil {
				t.Fatal(e)
			}
			if e = core.AbortMultipartUpload(ctx, b, "attempt-abort/data", id); e != nil {
				t.Fatal(e)
			}
			_, e = core.ListObjectParts(ctx, b, "attempt-abort/data", id, 0, 100)
			requireCode(t, e, "NoSuchUpload")
			parts := 0
			tr.mu.Lock()
			for _, r := range tr.requests {
				if strings.Contains(r, "/attempt-unique/data ") && strings.Contains(r, "partNumber=") {
					parts++
				}
			}
			tr.mu.Unlock()
			if parts != 3 {
				t.Fatalf("multipart part requests=%d", parts)
			}
			t.Log("PASS 12MiB/3-part multipart, SHA256 roundtrip, ambiguous initiation/completion, explicit abort")

			// List each page through Core, fail if any object is silently skipped.
			expected := 0
			for o := range c.ListObjects(ctx, b, minio.ListObjectsOptions{Recursive: true}) {
				if o.Err != nil {
					t.Fatal(o.Err)
				}
				expected++
			}
			marker := ""
			seen := 0
			pages := 0
			for {
				p, e := core.ListObjects(b, "", marker, "", 2)
				if e != nil {
					t.Fatal(e)
				}
				seen += len(p.Contents)
				pages++
				if !p.IsTruncated {
					break
				}
				if p.NextMarker == marker || p.NextMarker == "" {
					t.Fatal("pagination did not advance")
				}
				marker = p.NextMarker
			}
			if seen != expected || pages < 2 {
				t.Fatalf("pagination seen=%d expected=%d pages=%d", seen, expected, pages)
			}
			t.Logf("PASS strong immediate read/list and pagination objects=%d pages=%d", seen, pages)
			testDeleteDrain(t, c, b)
		})
	}
}

// A late DELETE is held BEFORE the server sees it. Client cancellation/time
// cannot prove drain; only the observed response completes this request.
type delayedDelete struct {
	base              http.RoundTripper
	sent, allow, done chan struct{}
	once              sync.Once
}

func (x *delayedDelete) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "DELETE" {
		x.once.Do(func() { close(x.sent) })
		<-x.allow
		res, e := x.base.RoundTrip(r)
		close(x.done)
		return res, e
	}
	return x.base.RoundTrip(r)
}
func testDeleteDrain(t *testing.T, c *minio.Client, b string) {
	ctx := context.Background()
	if _, e := put(c, b, "delete-victim", []byte("needed"), create()); e != nil {
		t.Fatal(e)
	}
	gate := []byte(`{"generation":1,"owner":"deleter","holders":[]}`)
	u, e := put(c, b, "gate", gate, create())
	if e != nil {
		t.Fatal(e)
	}
	// Reader cannot admit while owner remains, even when HEAD still sees data.
	if !bytes.Contains(get(t, c, b, "gate"), []byte(`"owner":"deleter"`)) {
		t.Fatal("missing owner")
	}
	original, _ := client(t, "v4", true)
	// Reuse trusted transport and creds but make only DELETE causally late.
	base, _ := minio.DefaultTransport(true)
	pem, _ := os.ReadFile(os.Getenv("EXPERIMENT_CA"))
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	base.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	delay := &delayedDelete{base: base, sent: make(chan struct{}), allow: make(chan struct{}), done: make(chan struct{})}
	dc, e := minio.New(os.Getenv("EXPERIMENT_ENDPOINT"), &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("MINIO_ROOT_USER"), os.Getenv("MINIO_ROOT_PASSWORD"), ""), Secure: true, Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: delay, MaxRetries: 1})
	if e != nil {
		t.Fatal(e)
	}
	_ = original
	result := make(chan error, 1)
	go func() { result <- dc.RemoveObject(ctx, b, "delete-victim", minio.RemoveObjectOptions{}) }()
	<-delay.sent
	if len(get(t, c, b, "delete-victim")) == 0 {
		t.Fatal("victim missing before late request")
	}
	// Gate deliberately remains owned; no admission merely because time passed.
	close(delay.allow)
	if e := <-result; e != nil {
		t.Fatal(e)
	}
	<-delay.done
	cas := opts()
	cas.SetMatchETag(u.ETag)
	u, e = put(c, b, "gate", []byte(`{"generation":2,"owner":null,"holders":[]}`), cas)
	if e != nil {
		t.Fatal(e)
	}
	cas = opts()
	cas.SetMatchETag(u.ETag)
	_, e = put(c, b, "gate", []byte(`{"generation":3,"owner":null,"holders":["restore"]}`), cas)
	if e != nil {
		t.Fatal(e)
	}
	t.Log("PASS delayed remote DELETE drains BEFORE gate release and restore admission")
}
