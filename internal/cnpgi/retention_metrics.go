package cnpgi

import (
	"fmt"
	"io"
	"sort"
	"time"
)

type retentionSeries struct {
	Blocked, Admission, Observed          bool
	Holders                               int
	Checked                               time.Time
	WorkspaceObserved, WorkspaceAvailable bool
}

func (m *backupMetrics) retention(labels backupLabels, blocked, admission, observed bool, holders int, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	labels.Type = ""
	// Bounded diagnostic cache; expired/deleted configurations cannot accumulate.
	for k, v := range m.retentionSeries {
		if now.Sub(v.Checked) > 48*time.Hour {
			delete(m.retentionSeries, k)
		}
	}
	if m.retentionSeries == nil {
		m.retentionSeries = map[backupLabels]retentionSeries{}
	}
	if _, ok := m.retentionSeries[labels]; !ok && len(m.retentionSeries) >= 4096 {
		return
	}
	m.retentionSeries[labels] = retentionSeries{Blocked: blocked, Admission: admission, Observed: observed, Holders: holders, Checked: now}
}
func (m *backupMetrics) retentionWorkspace(labels backupLabels, observed, available bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	labels.Type = ""
	if s, ok := m.retentionSeries[labels]; ok {
		s.WorkspaceObserved, s.WorkspaceAvailable = observed, available
		m.retentionSeries[labels] = s
	}
}

func (m *backupMetrics) writeRetention(w io.Writer) {
	m.mu.Lock()
	values := make(map[backupLabels]retentionSeries, len(m.retentionSeries))
	for k, v := range m.retentionSeries {
		if time.Since(v.Checked) <= 48*time.Hour {
			values[k] = v
		}
	}
	m.mu.Unlock()
	labels := []backupLabels{}
	for k := range values {
		labels = append(labels, k)
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].text() < labels[j].text() })
	for _, name := range []string{"cnpg_backup_retention_blocked", "cnpg_backup_repository_admission_blocked", "cnpg_backup_repository_holders", "cnpg_backup_retention_workspace_available", "cnpg_backup_retention_checked_timestamp_seconds"} {
		fmt.Fprintf(w, "# HELP %s Last periodic gate/retention observation; diagnostics never authorize admission.\n# TYPE %s gauge\n", name, name)
		for _, l := range labels {
			s := values[l]
			if (name == "cnpg_backup_repository_admission_blocked" || name == "cnpg_backup_repository_holders") && !s.Observed {
				continue
			}
			if name == "cnpg_backup_retention_workspace_available" && !s.WorkspaceObserved {
				continue
			}
			v := 0
			switch name {
			case "cnpg_backup_retention_blocked":
				if s.Blocked {
					v = 1
				}
			case "cnpg_backup_repository_admission_blocked":
				if s.Admission {
					v = 1
				}
			case "cnpg_backup_retention_workspace_available":
				if s.WorkspaceAvailable {
					v = 1
				}
			case "cnpg_backup_retention_checked_timestamp_seconds":
				v = int(s.Checked.Unix())
			default:
				v = s.Holders
			}
			fmt.Fprintf(w, "%s{repository_id=%q,namespace=%q,cluster=%q} %d\n", name, l.Repository, l.Namespace, l.Cluster, v)
		}
	}
}
