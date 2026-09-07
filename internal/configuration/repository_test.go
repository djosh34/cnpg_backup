package configuration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func validSpec() Spec {
	s := Defaults()
	s.RepositoryID = "11111111-1111-4111-8111-111111111111"
	s.S3.Endpoint = "https://minio.test"
	s.S3.Bucket = "test-bucket"
	s.S3.Prefix = "test/repository"
	s.S3.AccessKeySecret = Selector{"auth", "access"}
	s.S3.SecretKeySecret = Selector{"auth", "secret"}
	s.Workspace.StorageClassName = "bounded-disk"
	return s
}
func TestRepositoryDefaultsAndStrictValidation(t *testing.T) {
	input := []byte(`{"repositoryID":"11111111-1111-4111-8111-111111111111","s3":{"endpoint":"https://minio.test","bucket":"test-bucket","prefix":"test/repository","accessKeySecret":{"name":"auth","key":"access"},"secretKeySecret":{"name":"auth","key":"secret"}},"workspace":{"storageClassName":"bounded-disk"}}`)
	spec, err := DecodeSpec(input)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Hash() != validSpec().Hash() {
		t.Fatal("defaults diverged")
	}
	for _, input := range []string{`{"repositoryID":"a","repositoryID":"b"}`, `{} {}`, `{"unsafeTLS":true}`, `{"s3":{"forceClear":true}}`} {
		if _, err := DecodeSpec([]byte(input)); err == nil {
			t.Fatal("accepted unsafe JSON", input)
		}
	}
	for name, mutate := range map[string]func(*Spec){
		"http": func(s *Spec) { s.S3.Endpoint = "http://minio.test" }, "userinfo": func(s *Spec) { s.S3.Endpoint = "https://secret@minio.test" },
		"query": func(s *Spec) { s.S3.Endpoint += "?unsafe" }, "prefix": func(s *Spec) { s.S3.Prefix = "../outside" },
		"v2token":   func(s *Spec) { s.S3.Signature = "v2"; s.S3.SessionTokenSecret = &Selector{"auth", "token"} },
		"workspace": func(s *Spec) { s.Workspace.Size = "9Ti" }, "emptyClass": func(s *Spec) { s.Workspace.StorageClassName = "" },
		"native": func(s *Spec) { s.Native.MaxRestoredBytes = 1025 * GiB }, "concurrency": func(s *Spec) { s.IO.WALUploads = 3 },
		"retention": func(s *Spec) { s.Retention.Enabled = true }, "timeout": func(s *Spec) { s.IO.OperationTimeout = "0s" },
		"resources": func(s *Spec) { delete(s.Resources.Limits, "memory") },
	} {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			mutate(&s)
			if s.Validate() == nil {
				t.Fatal("invalid spec accepted")
			}
		})
	}
}
func publish(t *testing.T, dir, generation string, files map[string][]byte) {
	t.Helper()
	root := filepath.Join(dir, generation)
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(dir, "..next")
	if err := os.Symlink(generation, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(link, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
}
func TestSecretSnapshotsFailClosedOnRotation(t *testing.T) {
	dir := t.TempDir()
	spec := validSpec()
	data, _ := json.Marshal(spec)
	files := map[string][]byte{"destination/repository.json": data, "destination/accessKey": []byte("old-access"), "destination/secretKey": []byte("old-secret")}
	publish(t, dir, "one", files)
	old, err := LoadSnapshot(dir, "destination")
	if err != nil {
		t.Fatal(err)
	}
	files["destination/secretKey"] = nil
	publish(t, dir, "invalid", files)
	if _, err := LoadSnapshot(dir, "destination"); err == nil {
		t.Fatal("stale credentials used after invalid update")
	}
	if string(old.SecretKey) != "old-secret" {
		t.Fatal("running snapshot mutated")
	}
	files["destination/secretKey"] = []byte("new-secret")
	publish(t, dir, "two", files)
	next, err := LoadSnapshot(dir, "destination")
	if err != nil || string(next.SecretKey) != "new-secret" {
		t.Fatal("valid rotation rejected", err)
	}
	if _, err := LoadSnapshot(dir, "source"); err == nil {
		t.Fatal("source fell back to destination")
	}
	// A root already opened remains on one generation during publication.
	root, err := Projection(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	files["destination/secretKey"] = []byte("third-secret")
	publish(t, dir, "three", files)
	retained, err := Read(root, "destination/secretKey", 64<<10)
	if err != nil || string(retained) != "new-secret" {
		t.Fatal("mixed projection generations", err)
	}
}
func FuzzRepositorySpec(f *testing.F) {
	data, _ := json.Marshal(validSpec())
	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := DecodeSpec(data)
		if err == nil && s.Validate() != nil {
			t.Fatal("decoded invalid spec")
		}
	})
}
