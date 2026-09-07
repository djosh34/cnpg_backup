// Test-only streaming fault proxy for disposable CNPG/MinIO acceptance. This
// binary is never shipped or called by product code. No credentials are logged.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type faults struct {
	sync.Mutex
	mode    string
	blocked int
	release chan struct{}
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
		if mode == "hold-commit-response" && response.Request.Method == "PUT" && strings.HasSuffix(response.Request.URL.Path, "/commit.json") && response.StatusCode == 200 {
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
				if mode != "" && mode != "hold-wal-put" && mode != "fail-wal-get" && mode != "hold-artifact-put" && mode != "hold-commit-response" {
					w.WriteHeader(400)
					return
				}
				close(state.release)
				state.release = make(chan struct{})
				state.mode = mode
				state.blocked = 0
			}
			json.NewEncoder(w).Encode(map[string]any{"mode": state.mode, "blocked": state.blocked})
			return
		}
		state.Lock()
		mode, release := state.mode, state.release
		state.Unlock()
		if strings.Contains(r.URL.Path, "/wal/") {
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
		proxy.ServeHTTP(w, r)
	})
	server := http.Server{Addr: ":9000", Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10}
	if server.ListenAndServeTLS("/certs/public.crt", "/certs/private.key") != nil {
		os.Exit(2)
	}
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
