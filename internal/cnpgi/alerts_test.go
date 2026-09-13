package cnpgi

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

type alertRule struct {
	Alert       string            `json:"alert"`
	Expr        string            `json:"expr"`
	For         string            `json:"for"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

func alertRules(t *testing.T, path string) (string, []alertRule) {
	t.Helper()
	if os.Getenv("CNPG_PROMTOOL") == "" {
		t.Skip("set CNPG_PROMTOOL to test Prometheus rules")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Spec struct {
			Groups []struct {
				Name  string      `json:"name"`
				Rules []alertRule `json:"rules"`
			} `json:"groups"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Spec.Groups) != 1 {
		t.Fatal("expected one alert group")
	}
	// Strip the Kubernetes envelope without dropping rule-engine fields.
	var document map[string]any
	if err := yaml.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "rules.json")
	writeAlertJSON(t, path, document["spec"])
	return path, manifest.Spec.Groups[0].Rules
}

func writeAlertJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func testAlerts(t *testing.T, rules string, tests []any) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tests.json")
	writeAlertJSON(t, path, map[string]any{"rule_files": []string{rules}, "evaluation_interval": "1m", "tests": tests})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, os.Getenv("CNPG_PROMTOOL"), "test", "rules", path).CombinedOutput()
	if err != nil {
		t.Fatalf("promtool: %v\n%s", err, out)
	}
}
