package cnpgi

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/configuration"
)

func TestConsumerExamplesUseSupportedConfiguration(t *testing.T) {
	b, err := os.ReadFile("../../config/repository-example.json")
	if err != nil {
		t.Fatal(err)
	}
	var object struct {
		Spec json.RawMessage `json:"spec"`
	}
	if err = json.Unmarshal(b, &object); err != nil {
		t.Fatal(err)
	}
	spec, err := configuration.DecodeSpec(object.Spec)
	if err != nil {
		t.Fatal(err)
	}
	b, err = os.ReadFile("../../config/cluster-example.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseCluster(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placementCapacity(c, spec); err != nil {
		t.Fatal(err)
	}
	if spec.Retention.Enabled || !spec.Retention.DryRun {
		t.Fatal("consumer example unexpectedly authorizes deletion")
	}
}
