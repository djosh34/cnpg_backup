package cnpgi

import (
	"context"
	"strings"
	"testing"
)

// The actual post-recovery instance needs its own CNPG certificates, not source
// credentials and not a wildcard Secret grant. Regression from G-SPEC-2.
func TestRecoveryThenInstanceRequiresExactNativeSecrets(t *testing.T) {
	api, c, pod := fixture(t, true)
	api.SecretNames[c.Metadata.Namespace] = []string{"auth"}
	if _, err := Place(context.Background(), api, c, raw(pod), "test-image"); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Containers[0].Command = []string{"/controller/manager", "instance", "run"}
	if _, err := Place(context.Background(), api, c, raw(pod), "test-image"); err == nil || !strings.Contains(err.Error(), "native replication Secret") {
		t.Fatal("S3-only allowlist must not authorize normal native placement", err)
	}
	api.SecretNames[c.Metadata.Namespace] = []string{"auth", "database-replication", "database-ca"}
	if _, err := Place(context.Background(), api, c, raw(pod), "test-image"); err != nil {
		t.Fatal(err)
	}
}
