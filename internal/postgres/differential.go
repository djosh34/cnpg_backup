// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
)

// EligibleFull is deliberately conservative across restart/checksum/timeline
// transitions. Native PostgreSQL, not a summary-file parser, checks actual WAL
// summary coverage when it establishes the differential's start LSN.
func (c *Capture) EligibleFull(root repository.Commit, podUID string) error {
	return c.eligibleFullAt(root, podUID, c.before.Clock)
}

func (c *Capture) eligibleFullAt(root repository.Commit, podUID, clock string) error {
	age, e := time.ParseDuration(c.Snapshot.Repository.Spec.Native.MaxReferenceAge)
	start, e1 := time.Parse(time.RFC3339Nano, root.StartedAt)
	stop, e2 := time.Parse(time.RFC3339Nano, root.StoppedAt)
	now, e3 := time.Parse(time.RFC3339Nano, clock)
	if e != nil || e1 != nil || e2 != nil || e3 != nil || age <= 0 || now.Sub(start) > age || stop.Before(start) || stop.After(now) || root.Kind != "full" || root.PostgresMajor != 18 || root.ToolVersion != "18.6" || root.SystemIdentifier != c.control.System || root.Timeline != c.control.Timeline || root.ChecksumVersion != c.control.Checksum || podUID == "" || root.CaptureInstanceUID != podUID || root.PostmasterStartedAt != c.before.Postmaster {
		return errors.New("differential requires an eligible original full on the same primary/postmaster/checksum/timeline; request a new full explicitly")
	}
	return nil
}

// Differential downloads the exact protected original manifest; neither a
// synthesized manifest nor an earlier differential can enter the native argv.
func (c *Capture) Differential(ctx context.Context, podUID string, hold *repository.Hold, root repository.Commit) (*Captured, error) {
	if e := c.EligibleFull(root, podUID); e != nil {
		return nil, e
	}
	p := filepath.Join(c.Directory, "reference_manifest")
	f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	if e = hold.DownloadInput(ctx, root.BackupUID, -1, f); e != nil {
		return nil, e
	}
	hash, e := fileHash(ctx, f)
	if e != nil {
		return nil, e
	}
	st, e := f.Stat()
	if e != nil || st.Size() != root.ManifestBytes || hash != root.ManifestSHA256 {
		return nil, ErrInput
	}
	if _, e = f.Seek(0, 0); e != nil {
		return nil, e
	}
	m, e := ScanManifest(contextInput{ctx, f})
	if e != nil || strconv.FormatUint(m.SystemIdentifier, 10) != root.SystemIdentifier || len(m.Ranges) != 1 || m.Ranges[0].Timeline != root.Timeline || m.Ranges[0].StartLSN != root.StartLSN || m.Ranges[0].EndLSN != root.StopLSN {
		return nil, ErrInput
	}
	if e = f.Sync(); e != nil {
		return nil, e
	}
	return c.capture(ctx, podUID, &root, p)
}
