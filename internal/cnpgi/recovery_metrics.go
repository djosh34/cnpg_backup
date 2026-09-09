package cnpgi

import (
	"fmt"
	"io"
	"sort"
	"time"
)

type restoreLabels struct{ Namespace, Cluster string }
type restoreSeries struct {
	State                   string
	Known, LifetimeReleased bool
	Checked                 time.Time
}

// One bounded entry per target Cluster, not per attempt/Pod/operation. Missing
// or stale observations are Unknown, never an assertion that a hold is absent.
func (m *backupMetrics) restore(c Cluster, state recoveryOperation, known bool, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.restoreSeries == nil {
		m.restoreSeries = map[restoreLabels]restoreSeries{}
	}
	for k, v := range m.restoreSeries {
		if now.Sub(v.Checked) > 48*time.Hour {
			delete(m.restoreSeries, k)
		}
	}
	labels := restoreLabels{c.Metadata.Namespace, c.Metadata.Name}
	if _, ok := m.restoreSeries[labels]; !ok && len(m.restoreSeries) >= 4096 {
		return
	}
	m.restoreSeries[labels] = restoreSeries{state.State, known, state.LifetimeReleased, now}
}

func (m *backupMetrics) writeRestore(w io.Writer) {
	m.mu.Lock()
	values := make(map[restoreLabels]restoreSeries, len(m.restoreSeries))
	for k, v := range m.restoreSeries {
		if time.Since(v.Checked) <= 48*time.Hour {
			values[k] = v
		}
	}
	m.mu.Unlock()
	labels := make([]restoreLabels, 0, len(values))
	for k := range values {
		labels = append(labels, k)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].Namespace != labels[j].Namespace {
			return labels[i].Namespace < labels[j].Namespace
		}
		return labels[i].Cluster < labels[j].Cluster
	})
	for _, name := range []string{"observation_known", "active", "uncertain", "lifetime_release_pending"} {
		metric := "cnpg_backup_restore_" + name
		fmt.Fprintf(w, "# HELP %s Recent durable recovery operation observation; never authorizes source deletion or target reuse.\n# TYPE %s gauge\n", metric, metric)
		for _, l := range labels {
			s := values[l]
			known := s.Known && time.Since(s.Checked) <= backupHistoryTTL
			if name != "observation_known" && !known {
				continue
			}
			value := false
			switch name {
			case "observation_known":
				value = known
			case "active":
				value = s.State == "active"
			case "uncertain":
				value = s.State == "uncertain"
			case "lifetime_release_pending":
				value = s.State == "completed" && !s.LifetimeReleased
			}
			n := 0
			if value {
				n = 1
			}
			fmt.Fprintf(w, "%s{namespace=%q,cluster=%q} %d\n", metric, l.Namespace, l.Cluster, n)
		}
	}
}
