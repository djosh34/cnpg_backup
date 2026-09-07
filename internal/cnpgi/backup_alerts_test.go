package cnpgi

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

// Real PromQL evaluation, not a hand-written approximation of the shipped
// expressions. Optional test tool only; never linked into the manager image.
func TestBackupAlertsPromtool(t *testing.T) {
	tool := os.Getenv("CNPG_PROMTOOL")
	if tool == "" {
		t.Skip("set CNPG_PROMTOOL to run actual Prometheus rule positive/negative controls")
	}
	data, err := os.ReadFile("../../config/backup-alerts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Spec struct {
			Groups []struct {
				Name  string `json:"name"`
				Rules []struct {
					Alert       string            `json:"alert"`
					Expr        string            `json:"expr"`
					For         string            `json:"for"`
					Labels      map[string]string `json:"labels"`
					Annotations map[string]string `json:"annotations"`
				} `json:"rules"`
			} `json:"groups"`
		} `json:"spec"`
	}
	// Kubernetes envelope fields are deliberately not rule-engine inputs.
	if err = yaml.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Spec.Groups) != 1 || len(manifest.Spec.Groups[0].Rules) != 8 {
		t.Fatal("missing per-type rules")
	}
	dir := t.TempDir()
	rulesFile := filepath.Join(dir, "rules.json")
	b, _ := json.Marshal(manifest.Spec)
	if err = os.WriteFile(rulesFile, b, 0600); err != nil {
		t.Fatal(err)
	}
	tests := []any{}
	for _, kind := range []string{"full", "differential"} {
		for _, scenario := range []string{"never", "stale", "failed", "healthy", "unknown", "unscheduled", "other-type-only", "reset"} {
			labels := map[string]string{"repository_id": "repository", "namespace": "test", "cluster": "database", "backup_type": kind}
			suffix := `{repository_id="repository",namespace="test",cluster="database",backup_type="` + kind + `"}`
			inputs := []any{}
			add := func(name, value string) {
				inputs = append(inputs, map[string]any{"series": name + suffix, "values": value})
			}
			known := "1+0x10"
			if scenario == "unknown" {
				known = "0+0x10"
			}
			add("cnpg_backup_success_history_known", known)
			if scenario != "unscheduled" && scenario != "other-type-only" {
				add("cnpg_backup_freshness_max_age_seconds", "60+0x10")
			}
			if scenario == "other-type-only" {
				other := "full"
				if kind == "full" {
					other = "differential"
				}
				inputs = append(inputs, map[string]any{"series": "cnpg_backup_freshness_max_age_seconds" + strings.Replace(suffix, `backup_type="`+kind+`"`, `backup_type="`+other+`"`, 1), "values": "60+0x10"})
			}
			switch scenario {
			case "stale", "unscheduled", "other-type-only":
				add("cnpg_backup_last_success_timestamp_seconds", "1+0x10")
			case "healthy", "failed", "reset":
				add("cnpg_backup_last_success_timestamp_seconds", "590+0x10")
			}
			counter := "0+0x10"
			if scenario == "failed" || scenario == "unscheduled" || scenario == "other-type-only" {
				counter = "0+0x4 1+0x5"
			}
			if scenario == "reset" {
				counter = "5+0x4 0+0x5"
			}
			add("cnpg_backup_failures_total", counter)
			checks := []any{}
			for _, rule := range manifest.Spec.Groups[0].Rules {
				expected := []any{}
				typeName := "Full"
				if kind == "differential" {
					typeName = "Differential"
				}
				positive := map[string]string{"never": "NeverSuccessful", "stale": "Stale", "failed": "Failing", "unknown": "HistoryUnknown"}[scenario]
				if positive != "" && rule.Alert == "CNPGBackup"+typeName+positive {
					expLabels := map[string]string{}
					for k, v := range labels {
						expLabels[k] = v
					}
					for k, v := range rule.Labels {
						expLabels[k] = v
					}
					expected = append(expected, map[string]any{"exp_labels": expLabels, "exp_annotations": rule.Annotations})
				}
				checks = append(checks, map[string]any{"eval_time": "10m", "alertname": rule.Alert, "exp_alerts": expected})
			}
			tests = append(tests, map[string]any{"name": kind + "-" + scenario, "interval": "1m", "input_series": inputs, "alert_rule_test": checks})
		}
	}
	input := map[string]any{"rule_files": []string{rulesFile}, "evaluation_interval": "1m", "tests": tests}
	b, _ = json.Marshal(input)
	testFile := filepath.Join(dir, "tests.json")
	if err = os.WriteFile(testFile, b, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, tool, "test", "rules", testFile).CombinedOutput()
	if err != nil {
		t.Fatalf("real alert engine: %v\n%s", err, output)
	}
	t.Logf("promtool: %s (16 cases, 128 alert assertions)", output)
}
