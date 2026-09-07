package configuration

import (
	"crypto/x509"
	"encoding/json"
	"testing"
)

func TestNativeSnapshotRoleDistinctCAsAndInvalidRotation(t *testing.T) {
	clientCA, serverCA := newCA(t), newCA(t)
	cert, key := clientCA.leaf(t, "streaming_replica", x509.ExtKeyUsageClientAuth)
	data, _ := json.Marshal(validSpec())
	files := map[string][]byte{"destination/repository.json": data, "destination/accessKey": []byte("access"), "destination/secretKey": []byte("secret"), "native/tls.crt": cert, "native/tls.key": key, "native/ca.crt": serverCA.pem, "native/client-ca.crt": clientCA.pem}
	dir := t.TempDir()
	publish(t, dir, "valid", files)
	snapshot, err := LoadCaptureSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	files["native/tls.key"] = []byte("broken rotation")
	publish(t, dir, "broken", files)
	if _, err := LoadCaptureSnapshot(dir); err == nil {
		t.Fatal("invalid rotation used old identity")
	}
	if string(snapshot.Key) != string(key) {
		t.Fatal("retained operation snapshot mutated")
	}
	files["native/tls.crt"], files["native/tls.key"] = clientCA.leaf(t, "cnpg_streaming_replica", x509.ExtKeyUsageClientAuth)
	publish(t, dir, "wrong-role", files)
	if _, err := LoadCaptureSnapshot(dir); err == nil {
		t.Fatal("ident map accepted as replication role")
	}
	files["native/tls.crt"], files["native/tls.key"] = clientCA.leaf(t, "streaming_replica", x509.ExtKeyUsageServerAuth)
	publish(t, dir, "wrong-usage", files)
	if _, err := LoadCaptureSnapshot(dir); err == nil {
		t.Fatal("server cert accepted as replication identity")
	}
	files["native/tls.crt"], files["native/tls.key"] = cert, key
	files["native/client-ca.crt"] = serverCA.pem
	publish(t, dir, "wrong-ca", files)
	if _, err := LoadCaptureSnapshot(dir); err == nil {
		t.Fatal("untrusted client chain accepted")
	}
}
