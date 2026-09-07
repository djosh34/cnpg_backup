package s3store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// Keep diagnostic output bounded and redact the generated test credentials.
// The first C CI failure discarded this log on t.TempDir cleanup, making the
// exact setup error impossible to distinguish from the retained artifact.
func retainMinIOLog(t *testing.T, path string, secrets ...string) {
	t.Helper()
	f, e := os.Open(path)
	if e != nil {
		t.Log("MinIO setup log unavailable")
		return
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 128<<10))
	if e != nil {
		t.Log("MinIO setup log unreadable")
		return
	}
	text := string(b)
	for _, s := range secrets {
		if s != "" {
			text = strings.ReplaceAll(text, s, "<REDACTED>")
		}
	}
	if t.Failed() {
		t.Logf("bounded redacted MinIO server log (setup/product phase is reported separately):\n%s", text)
	}
	dir := os.Getenv("CNPG_S3_ARTIFACT_DIR")
	if dir != "" {
		if !filepath.IsAbs(dir) {
			t.Error("artifact directory must be absolute")
			return
		}
		if e = os.MkdirAll(dir, 0700); e != nil {
			t.Error("artifact directory unavailable")
			return
		}
		if e = os.WriteFile(filepath.Join(dir, "adapter-minio-server.log"), []byte(text), 0600); e != nil {
			t.Error("cannot retain MinIO log")
		}
	}
}
func setupError(err error) string {
	r := minio.ToErrorResponse(err)
	code := r.Code
	if !regexp.MustCompile(`^[A-Za-z0-9_]{0,80}$`).MatchString(code) {
		code = "redacted"
	}
	return fmt.Sprintf("category=%v sdk_code=%s http_status=%d", classify(err, false), code, r.StatusCode)
}

// Distinguishes transport handling of the standard MakeBucket success envelope
// from an actual MinIO readiness/auth/storage error. Not a real-MinIO result.
func TestBucketSetupSuccessEnvelope(t *testing.T) {
	s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" || r.URL.Path != "/test-bucket/" {
			t.Errorf("wrong setup request: method=%s path=%q", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	if e := s.core.MakeBucket(context.Background(), s.bucket, minio.MakeBucketOptions{Region: "us-east-1"}); e != nil {
		t.Fatal(setupError(e))
	}
}
