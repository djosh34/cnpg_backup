package cnpgi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestRecoveryWatchOutlivesRequestTimeoutAndCancelsExplicitly(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") != "true" {
			t.Error("not a watch request")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"one","uid":"uid","resourceVersion":"1"}}}`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL, Timeout: 50 * time.Millisecond}
	client, err := newRecoveryWatchClient(config)
	if err != nil {
		t.Fatal(err)
	}
	if config.Timeout != 50*time.Millisecond {
		t.Fatal("ordinary request timeout changed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Resource(coreResource("pods")).Namespace("test").Watch(ctx, meta.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Stop()
	select {
	case _, ok := <-stream.ResultChan():
		if !ok {
			t.Fatal("watch never opened")
		}
	case <-time.After(time.Second):
		t.Fatal("watch never delivered first event")
	}
	select {
	case <-closed:
		t.Fatal("ordinary HTTP client deadline killed lifetime watch")
	case <-stream.ResultChan():
		t.Fatal("watch ended at ordinary request deadline")
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("explicit manager cancellation did not close HTTP stream")
	}
}
