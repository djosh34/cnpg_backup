// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

const backupHistoryTTL = 5 * time.Minute

type backupLabels struct{ Repository, Namespace, Cluster, Type string }
type backupSeries struct {
	Failures    uint64
	Success     time.Time
	Known       bool
	Checked     time.Time
	MaxAge      time.Duration
	LastWarning time.Time
}

// Only the manager owns these series. Scrapes never perform API or S3 I/O.
// UID observation state lives separately and is never a metric label.
type backupMetrics struct {
	mu              sync.Mutex
	series          map[backupLabels]backupSeries
	retentionSeries map[backupLabels]retentionSeries
}

func newBackupMetrics() *backupMetrics {
	return &backupMetrics{series: map[backupLabels]backupSeries{}, retentionSeries: map[backupLabels]retentionSeries{}}
}
func (m *backupMetrics) configure(base backupLabels, f configuration.Freshness) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range []struct{ kind, age string }{{"full", f.FullMaxAge}, {"differential", f.DifferentialMaxAge}} {
		base.Type = v.kind
		s := m.series[base]
		s.MaxAge, _ = time.ParseDuration(v.age)
		m.series[base] = s
	}
}
func (m *backupMetrics) history(base backupLabels, h repository.BackupHistory, err error, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, kind := range []string{"full", "differential"} {
		base.Type = kind
		s := m.series[base]
		s.Checked, s.Known, s.Success = now, err == nil, time.Time{}
		if err == nil {
			s.Success = h.Full
			if kind == "differential" {
				s.Success = h.Differential
			}
		}
		m.series[base] = s
	}
}
func (m *backupMetrics) failure(labels backupLabels, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.series[labels]
	s.Failures++
	warn := s.LastWarning.IsZero() || !now.Before(s.LastWarning.Add(5*time.Minute))
	if warn {
		s.LastWarning = now
	}
	m.series[labels] = s
	return warn
}
func (m *backupMetrics) snapshot() map[backupLabels]backupSeries {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[backupLabels]backupSeries, len(m.series))
	for k, v := range m.series {
		result[k] = v
	}
	return result
}
func (l backupLabels) text() string {
	return fmt.Sprintf("repository_id=%q,namespace=%q,cluster=%q,backup_type=%q", l.Repository, l.Namespace, l.Cluster, l.Type)
}
func (m *backupMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m.writeRetention(w)
	values := m.snapshot()
	labels := make([]backupLabels, 0, len(values))
	for k := range values {
		labels = append(labels, k)
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].text() < labels[j].text() })
	for _, metric := range []struct{ name, kind, help string }{
		{"cnpg_backup_failures_total", "counter", "Observed terminal failed Backup UID transitions in this manager lifetime; initial failures are baseline."},
		{"cnpg_backup_success_history_known", "gauge", "One only after a recent complete valid permanent commit history scan."},
		{"cnpg_backup_last_success_timestamp_seconds", "gauge", "Latest durable commit S3 publication time, including retired history; absent when unknown or never successful."},
		{"cnpg_backup_freshness_max_age_seconds", "gauge", "Explicit per-type schedule freshness budget; absent when not configured."},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", metric.name, metric.help, metric.name, metric.kind)
		for _, l := range labels {
			s := values[l]
			known := s.Known && time.Since(s.Checked) <= backupHistoryTTL
			switch metric.name {
			case "cnpg_backup_failures_total":
				fmt.Fprintf(w, "%s{%s} %d\n", metric.name, l.text(), s.Failures)
			case "cnpg_backup_success_history_known":
				v := 0
				if known {
					v = 1
				}
				fmt.Fprintf(w, "%s{%s} %d\n", metric.name, l.text(), v)
			case "cnpg_backup_last_success_timestamp_seconds":
				if known && !s.Success.IsZero() {
					fmt.Fprintf(w, "%s{%s} %d\n", metric.name, l.text(), s.Success.Unix())
				}
			case "cnpg_backup_freshness_max_age_seconds":
				if s.MaxAge > 0 {
					fmt.Fprintf(w, "%s{%s} %g\n", metric.name, l.text(), s.MaxAge.Seconds())
				}
			}
		}
	}
}
