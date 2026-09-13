package cnpgi

import "testing"

func TestOperationalAlertsPromtool(t *testing.T) {
	rulesFile, rules := alertRules(t, "../../config/operational-alerts.yaml")
	tests := []any{}
	metrics := map[string]string{"CNPGBackupRetentionBlocked": "cnpg_backup_retention_blocked", "CNPGBackupRepositoryAdmissionBlocked": "cnpg_backup_repository_admission_blocked", "CNPGBackupRetentionWorkspacePressure": "cnpg_backup_retention_workspace_available", "CNPGBackupRestoreUncertain": "cnpg_backup_restore_uncertain", "CNPGBackupRestoreObservationUnknown": "cnpg_backup_restore_observation_known", "CNPGBackupRestoreLifetimeReleasePending": "cnpg_backup_restore_lifetime_release_pending"}
	if len(rules) != len(metrics) {
		t.Fatal("missing operational rules")
	}
	for _, rule := range rules {
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
	testAlerts(t, rulesFile, tests)
}
