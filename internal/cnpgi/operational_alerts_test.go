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

func TestOperationalAlertsPromtool(t *testing.T) {
	tool := os.Getenv("CNPG_PROMTOOL")
	if tool == "" {
		t.Skip("set CNPG_PROMTOOL for real alert engine")
	}
	b, err := os.ReadFile("../../config/operational-alerts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Spec struct {
			Groups []struct {
				Name  string `json:"name"`
				Rules []struct {
					Alert, Expr, For    string
					Labels, Annotations map[string]string
				} `json:"rules"`
			} `json:"groups"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(b, &manifest); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	rules := filepath.Join(dir, "rules.json")
	// Preserve Prometheus's lower-case field names through the YAML conversion.
	var document map[string]any
	if err := yaml.Unmarshal(b, &document); err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(document["spec"])
	if err := os.WriteFile(rules, b, 0600); err != nil {
		t.Fatal(err)
	}
	tests := []any{}
	metrics := map[string]string{"CNPGBackupRetentionBlocked": "cnpg_backup_retention_blocked", "CNPGBackupRepositoryAdmissionBlocked": "cnpg_backup_repository_admission_blocked", "CNPGBackupRetentionWorkspacePressure": "cnpg_backup_retention_workspace_available", "CNPGBackupRestoreUncertain": "cnpg_backup_restore_uncertain", "CNPGBackupRestoreObservationUnknown": "cnpg_backup_restore_observation_known", "CNPGBackupRestoreLifetimeReleasePending": "cnpg_backup_restore_lifetime_release_pending"}
	if len(manifest.Spec.Groups) != 1 || len(manifest.Spec.Groups[0].Rules) != len(metrics) {
		t.Fatal("missing operational rules")
	}
	for _, rule := range manifest.Spec.Groups[0].Rules {
		metric, ok := metrics[rule.Alert]
		if !ok {
			t.Fatal(rule.Alert)
		}
		for _, mode := range []string{"positive", "negative", "absent"} {
			inputs, expected := []any{}, []any{}
			labels := map[string]string{"namespace": "test", "cluster": "database"}
			if mode != "absent" {
				value := "1"
				if metric == "cnpg_backup_retention_workspace_available" || metric == "cnpg_backup_restore_observation_known" {
					value = "0"
				}
				if mode == "negative" {
					if value == "1" {
						value = "0"
					} else {
						value = "1"
					}
				}
				inputs = append(inputs, map[string]any{"series": metric + `{namespace="test",cluster="database"}`, "values": value + "+0x10"})
			}
			if mode == "positive" {
				for k, v := range rule.Labels {
					labels[k] = v
				}
				expected = append(expected, map[string]any{"exp_labels": labels, "exp_annotations": rule.Annotations})
			}
			tests = append(tests, map[string]any{"name": rule.Alert + "-" + mode, "interval": "1m", "input_series": inputs, "alert_rule_test": []any{map[string]any{"eval_time": "10m", "alertname": rule.Alert, "exp_alerts": expected}}})
		}
	}
	b, _ = json.Marshal(map[string]any{"rule_files": []string{rules}, "evaluation_interval": "1m", "tests": tests})
	input := filepath.Join(dir, "tests.json")
	if err := os.WriteFile(input, b, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, tool, "test", "rules", input).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	t.Logf("%s (18 operational positive/negative/absent cases)", out)
}
