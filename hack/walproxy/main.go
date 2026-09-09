// Test-only streaming fault proxy for disposable CNPG/MinIO acceptance. This
// binary is never shipped or called by product code. No credentials are logged.
package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type faults struct {
	sync.Mutex
	mode        string
	match       string
	blocked     int
	active      int
	transferred int64
	principals  map[string]int
	release     chan struct{}
}

func main() {
	backend, e := url.Parse(os.Getenv("FIXTURE_BACKEND"))
	if e != nil || backend.Scheme != "https" {
		os.Exit(2)
	}
	ca, e := os.ReadFile("/certs/ca.crt")
	if e != nil {
		os.Exit(2)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		os.Exit(2)
	}
	proxy := httputil.NewSingleHostReverseProxy(backend)
	proxy.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DisableKeepAlives: true, DisableCompression: true}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) { w.WriteHeader(502) }
	state := &faults{release: make(chan struct{})}
	proxy.ModifyResponse = func(response *http.Response) error {
		state.Lock()
		mode, release := state.mode, state.release
		if mode == "corrupt-wal-get" && response.Request.Method == "GET" && response.StatusCode == 200 && strings.Contains(response.Request.URL.Path, "/wal/") && (state.match == "" || strings.HasSuffix(response.Request.URL.Path, "/"+state.match)) {
			state.blocked++
			response.Body = &corruptBody{ReadCloser: response.Body}
		}
		holdCommit := mode == "hold-commit-response" && response.Request.Method == "PUT" && strings.HasSuffix(response.Request.URL.Path, "/commit.json")
		holdWAL := mode == "hold-wal-get-response" && response.Request.Method == "GET" && strings.Contains(response.Request.URL.Path, "/wal/") && (state.match == "" || strings.HasSuffix(response.Request.URL.Path, "/"+state.match))
		holdInput := mode == "hold-artifact-get-response" && response.Request.Method == "GET" && strings.Contains(response.Request.URL.Path, "/attempts/")
		if (holdCommit || holdWAL || holdInput) && response.StatusCode == 200 {
			state.blocked++
			state.Unlock()
			select {
			case <-release:
				return nil
			case <-response.Request.Context().Done():
				return response.Request.Context().Err()
			}
		}
		state.Unlock()
		return nil
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fixture-control" {
			state.Lock()
			defer state.Unlock()
			if r.Method == "POST" {
				mode := r.URL.Query().Get("mode")
				if !validMode(mode) || (r.URL.Query().Get("name") != "" && !regexp.MustCompile(`^[0-9A-F]{24}$`).MatchString(r.URL.Query().Get("name"))) {
					w.WriteHeader(400)
					return
				}
				close(state.release)
				state.release = make(chan struct{})
				state.mode = mode
				state.match = r.URL.Query().Get("name")
				state.blocked = 0
			}
			json.NewEncoder(w).Encode(map[string]any{"mode": state.mode, "name": state.match, "blocked": state.blocked, "active_transfers": state.active, "transferred_bytes": state.transferred, "principal_requests": state.principals})
			return
		}
		state.Lock()
		mode, release, match := state.mode, state.release, state.match
		// Bounded hashed principal observations for the VALID overlap fixture;
		// never retain/log Authorization, credentials or session tokens.
		if fields := strings.SplitN(r.Header.Get("Authorization"), "Credential=", 2); len(fields) == 2 {
			if end := strings.IndexByte(fields[1], '/'); end > 0 && end <= 128 {
				sum := sha256.Sum256([]byte(fields[1][:end]))
				key := hex.EncodeToString(sum[:])
				if state.principals == nil {
					state.principals = map[string]int{}
				}
				if _, exists := state.principals[key]; exists || len(state.principals) < 16 {
					state.principals[key]++
				}
			}
		}
		state.Unlock()
		if strings.Contains(r.URL.Path, "/wal/") && (match == "" || strings.HasSuffix(r.URL.Path, "/"+match)) {
			if (r.Method == "GET" || r.Method == "HEAD") && injectWAL(w, r, mode, state) {
				return
			}
			if mode == "fail-wal-get" && (r.Method == "GET" || r.Method == "HEAD") {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(503)
				io.WriteString(w, "<Error><Code>ServiceUnavailable</Code></Error>")
				return
			}
			if mode == "hold-wal-put" && r.Method == "PUT" && r.ContentLength > 1 {
				// Forward half the real compressed body to MinIO before blocking. The
				// barrier proves a live upload, not just a sleep or HEAD request delay.
				r.Body = &partialBody{ReadCloser: r.Body, remaining: r.ContentLength / 2, state: state, release: release, done: r.Context().Done()}
			}
		}
		if mode == "hold-artifact-put" && strings.Contains(r.URL.Path, "/attempts/") && strings.Contains(r.URL.Path, "/data/") && r.Method == "PUT" && r.ContentLength > 1 {
			r.Body = &partialBody{ReadCloser: r.Body, remaining: r.ContentLength / 2, state: state, release: release, done: r.Context().Done()}
		}
		if mode == "slow-artifact-put" && strings.Contains(r.URL.Path, "/attempts/") && strings.Contains(r.URL.Path, "/data/") && r.Method == "PUT" && r.ContentLength > 1 {
			state.Lock()
			state.active++
			state.Unlock()
			defer func() { state.Lock(); state.active--; state.Unlock() }()
			r.Body = &slowBody{ReadCloser: r.Body, state: state, release: release, done: r.Context().Done()}
		}
		proxy.ServeHTTP(w, r)
	})
	server := http.Server{Addr: ":9000", Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 300 * time.Second, WriteTimeout: 300 * time.Second, MaxHeaderBytes: 64 << 10}
	// Disposable source TLS fault, not an HTTP status mislabeled as TLS. Control
	// uses localhost SNI; actual sidecars use the in-cluster MinIO Service name.
	server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair("/certs/public.crt", "/certs/private.key")
		return &cert, err
	}, GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		state.Lock()
		defer state.Unlock()
		if state.mode == "tls-wal-get" && hello.ServerName != "localhost" {
			state.blocked++
			return nil, errors.New("test-only TLS handshake rejection")
		}
		return nil, nil
	}}
	server.SetKeepAlivesEnabled(false)
	if server.ListenAndServeTLS("/certs/public.crt", "/certs/private.key") != nil {
		os.Exit(2)
	}
}

// A bounded streaming rate limit, not an early-ack queue. Counters measure real
// body bytes read toward MinIO; final durable success still comes from MinIO.
// Clearing the mode drains the original request rather than abandoning it.
type slowBody struct {
	io.ReadCloser
	state         *faults
	release, done <-chan struct{}
}

func (b *slowBody) Read(p []byte) (int, error) {
	timer := time.NewTimer(75 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-b.done:
		return 0, io.ErrClosedPipe
	case <-b.release:
	case <-timer.C:
	}
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	n, err := b.ReadCloser.Read(p)
	b.state.Lock()
	b.state.transferred += int64(n)
	b.state.Unlock()
	return n, err
}

func validMode(mode string) bool {
	switch mode {
	case "", "hold-wal-put", "fail-wal-get", "hold-artifact-put", "hold-commit-response", "slow-artifact-put",
		"missing-wal-get", "auth-wal-get", "corrupt-wal-get", "reset-wal-get", "tls-wal-get", "hold-wal-get-response", "hold-artifact-get-response":
		return true
	}
	return false
}

// These faults affect only selected synthetic archive reads, never gate/list
// requests. A counter establishes an effective injection, not just a requested
// mode. The underlying intact object remains available to distinguishing runs.
func injectWAL(w http.ResponseWriter, r *http.Request, mode string, state *faults) bool {
	switch mode {
	case "missing-wal-get", "auth-wal-get", "reset-wal-get":
	default:
		return false
	}
	state.Lock()
	state.blocked++
	state.Unlock()
	switch mode {
	case "missing-wal-get":
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
	case "auth-wal-get":
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
	case "reset-wal-get":
		if hijacker, ok := w.(http.Hijacker); ok {
			conn, _, e := hijacker.Hijack()
			if e == nil {
				conn.Close()
				return true
			}
		}
		panic(http.ErrAbortHandler)
	}
	return true
}

// Keep the actual authenticated metadata and exact response length; only the
// streamed payload is damaged. This distinguishes checksum failure from HEAD
// metadata rejection, without buffering a whole segment or changing MinIO data.
type corruptBody struct {
	io.ReadCloser
	flipped bool
}

func (b *corruptBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 && !b.flipped {
		p[0] ^= 0xff
		b.flipped = true
	}
	return n, e
}

type partialBody struct {
	io.ReadCloser
	remaining int64
	state     *faults
	release   <-chan struct{}
	done      <-chan struct{}
	notified  bool
}

func (b *partialBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		if !b.notified {
			b.state.Lock()
			b.state.blocked++
			b.state.Unlock()
			b.notified = true
		}
		select {
		case <-b.release:
		case <-b.done:
			return 0, io.ErrClosedPipe
		}
		return b.ReadCloser.Read(p)
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, e := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, e
}
