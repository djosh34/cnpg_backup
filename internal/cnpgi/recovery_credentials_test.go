package cnpgi

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestHelperPlanDoesNotCopyUnrelatedInlineCredentials(t *testing.T) {
	_, c, _ := fixture(t, true)
	c.Metadata.Annotations = map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "inline-secret-sentinel"}
	c.Spec.PostgreSQL.Parameters = map[string]string{"primary_conninfo": "password=inline-secret-sentinel"}
	c.Spec.Plugins = append(c.Spec.Plugins, Plugin{Name: "unrelated", Parameters: map[string]string{"token": "inline-secret-sentinel"}})
	originalOperation := c.OperationUID()
	sanitized := recoveryCluster(c)
	if sanitized.OperationUID() != originalOperation {
		t.Fatal("sanitization changed admitted bootstrap")
	}
	b := raw(sanitized)
	if bytes.Contains(b, []byte("inline-secret-sentinel")) {
		t.Fatal("helper projection leaked unrelated inline credential")
	}
	if _, _, e := sanitized.Repositories(); e != nil {
		t.Fatal("sanitization lost required validated source/destination fields", e)
	}
	p := recoveryPlanFixture(t)
	p.ClusterDefinition = json.RawMessage(raw(c))
	if e := p.validate(); e == nil {
		t.Fatal("accepted unsanitized durable helper Cluster")
	}
}
