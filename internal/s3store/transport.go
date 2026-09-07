package s3store

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const responseLimit = 4 << 20

type pageBudgetKey struct{}
type requestTimeoutKey struct{}
type transport struct {
	base           http.RoundTripper
	endpoint       string
	slots          chan struct{}
	metadata, data time.Duration
}

// retryRead is the sole retry loop. No mutation ever enters it. Streaming GET
// retries restart into a private destination; list retries are per-page only.
func retryRead(ctx context.Context, fn func() error) error {
	var err error
	for n := 0; n < 5; n++ {
		if ctx.Err() != nil {
			return failure(Canceled)
		}
		if n > 0 {
			d := 200 * time.Millisecond * time.Duration(1<<(n-1))
			d += time.Duration(rand.Int64N(int64(d / 2)))
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return failure(Canceled)
			}
		}
		err = fn()
		if !Is(err, Transient) {
			return err
		}
	}
	return err
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != t.endpoint {
		return nil, failure(Unsupported)
	}
	if b, ok := r.Context().Value(pageBudgetKey{}).(*atomic.Int64); ok {
		if b.Add(1) > 10001 {
			return nil, failure(Limit)
		}
	}
	dataGet := r.Method == "GET" && r.URL.RawQuery == "" && strings.Count(strings.Trim(r.URL.Path, "/"), "/") >= 1
	var resp *http.Response
	call := func() error { var err error; resp, err = t.once(r, dataGet); return err }
	var err error
	if (r.Method == "GET" && !dataGet) || r.Method == "HEAD" {
		err = retryRead(r.Context(), call)
	} else {
		err = call()
	}
	return resp, err
}

func (t *transport) once(r *http.Request, dataGet bool) (*http.Response, error) {
	timeout := t.metadata
	if dataGet || r.Method == "PUT" && (r.ContentLength > MaxMetadataSize || r.URL.Query().Has("partNumber")) {
		timeout = t.data
	}
	if d, ok := r.Context().Value(requestTimeoutKey{}).(time.Duration); ok {
		timeout = min(timeout, d)
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	if err := acquire(ctx, t.slots); err != nil {
		cancel()
		return nil, err
	}
	release := func() { cancel(); <-t.slots }
	req := r.Clone(ctx)
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		release()
		e := classify(err, false)
		if Is(e, Unknown) {
			e = failure(Transient)
		}
		return nil, e
	}
	fail := func(e error) (*http.Response, error) { resp.Body.Close(); release(); return nil, e }
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return fail(failure(Unsupported))
	}
	if dataGet && resp.StatusCode < 300 && resp.StatusCode != http.StatusOK {
		return fail(failure(Corrupt))
	}
	if dataGet && resp.StatusCode == http.StatusOK {
		_, dateErr := http.ParseTime(resp.Header.Get("Last-Modified"))
		if resp.Header.Get("Content-Encoding") != "" || resp.ContentLength < 0 || !validETag(strings.Trim(resp.Header.Get("ETag"), "\"")) || dateErr != nil {
			return fail(failure(Corrupt))
		}
		resp.Body = &releaseBody{ReadCloser: resp.Body, release: release}
		return resp, nil
	}
	// SDK closeResponse ignores drain failures, including successful DELETE.
	// Consume every terminal control response here before reporting completion.
	b, err := io.ReadAll(io.LimitReader(resp.Body, responseLimit+1))
	closeErr := resp.Body.Close()
	release()
	if err != nil || closeErr != nil {
		return nil, failure(Transient)
	}
	if len(b) > responseLimit {
		return nil, failure(Limit)
	}
	validXML := validXMLDocument(b)
	if resp.StatusCode >= 400 {
		var wire struct {
			XMLName xml.Name
			Code    string
		}
		parsed := validXML && xml.Unmarshal(b, &wire) == nil && wire.XMLName.Local == "Error"
		code := wire.Code
		// HEAD has no XML body and cannot distinguish missing bucket from key.
		if r.Method == "HEAD" && resp.StatusCode == 403 {
			return nil, failure(Auth)
		}
		if parsed {
			switch {
			case code == "NoSuchKey" && resp.StatusCode == 404 && dataGet:
				return nil, failure(NotFound)
			// MinIO rejects If-Match on a missing object with NoSuchKey,
			// not 412. This is a failed CAS, never successful publication
			// or general mutation absence. Keep other operations unknown.
			case code == "NoSuchKey" && resp.StatusCode == 404 && r.Method == "PUT" && r.URL.RawQuery == "" && r.Header.Get("If-Match") != "":
				return nil, failure(Precondition)
			case code == "PreconditionFailed" && resp.StatusCode == 412:
				return nil, failure(Precondition)
			case resp.StatusCode == 409:
				return nil, failure(Conflict)
			case resp.StatusCode == 401 || resp.StatusCode == 403:
				return nil, failure(Auth)
			}
		}
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return nil, failure(Transient)
		}
		// Preserve genuine bucket configuration absence for the SDK/check method.
		if !parsed {
			return nil, failure(Unknown)
		}
	} else if len(b) != 0 && !validXML {
		// Include SDK-decoded responses, notably HTTP 200 MPU completion
		// (success or embedded Error), before the SDK can infer an outcome.
		return nil, failure(Corrupt)
	}
	if resp.StatusCode == 200 && r.Method == "GET" {
		q := r.URL.Query()
		root := ""
		list := false
		switch {
		case q.Get("list-type") == "2":
			root = "ListBucketResult"
			list = true
		case q.Has("uploads"):
			root = "ListMultipartUploadsResult"
			list = true
		case q.Has("uploadId"):
			root = "ListPartsResult"
			list = true
		case q.Has("versioning"):
			root = "VersioningConfiguration"
		case q.Has("lifecycle"):
			root = "LifecycleConfiguration"
		case q.Has("object-lock"):
			root = "ObjectLockConfiguration"
		}
		if root != "" {
			var p struct {
				XMLName   xml.Name
				Truncated *bool      `xml:"IsTruncated"`
				Next      string     `xml:"NextContinuationToken"`
				Contents  []struct{} `xml:"Contents"`
				Prefixes  []struct{} `xml:"CommonPrefixes"`
			}
			if xml.Unmarshal(b, &p) != nil || p.XMLName.Local != root || list && p.Truncated == nil {
				return nil, failure(Corrupt)
			}
			if root == "ListBucketResult" && (len(p.Contents) > 1000 || len(p.Prefixes) > 0 || *p.Truncated && (p.Next == "" || p.Next == q.Get("continuation-token"))) {
				return nil, failure(Corrupt)
			}
		}
	}
	if resp.StatusCode == 200 && r.Method == "POST" && r.URL.Query().Has("uploads") {
		var v struct {
			XMLName     xml.Name
			Bucket, Key string
		}
		// SDK initiation parsing otherwise accepts unrelated well-formed XML.
		if xml.Unmarshal(b, &v) != nil || v.XMLName.Local != "InitiateMultipartUploadResult" || "/"+v.Bucket+"/"+v.Key != r.URL.Path {
			return nil, failure(Corrupt)
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	return resp, nil
}

// validXMLDocument checks the entire already-bounded control body. Decode and
// Unmarshal stop after the first root, accepting leading text or trailing junk.
// S3 needs no DTD; reject directives rather than interpreting entity declarations.
func validXMLDocument(b []byte) bool {
	d := xml.NewDecoder(bytes.NewReader(b))
	depth, roots := 0, 0
	for {
		offset := d.InputOffset()
		token, err := d.Token()
		if err != nil {
			return err == io.EOF && depth == 0 && roots == 1
		}
		switch v := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
				if roots > 1 {
					return false
				}
			}
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			// Outside the root only literal XML whitespace is allowed, not
			// character references or CDATA that happen to decode to it.
			if depth == 0 && len(bytes.Trim(b[offset:d.InputOffset()], " \t\r\n")) != 0 {
				return false
			}
		case xml.ProcInst:
			if strings.EqualFold(v.Target, "xml") && (v.Target != "xml" || offset != 0) {
				return false
			}
		case xml.Directive:
			return false
		}
	}
}

type releaseBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (b *releaseBody) Close() error { err := b.ReadCloser.Close(); b.once.Do(b.release); return err }
