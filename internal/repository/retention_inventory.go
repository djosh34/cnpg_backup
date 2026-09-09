package repository

import (
	"context"
	"strconv"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// WALObject carries validated permanent slot metadata, never a caller-supplied
// storage key. Retired slots remain in coverage inventories (a required one is
// a gap), and auxiliary partial/history files are never deletion candidates.
type WALObject struct {
	Name, ETag, SHA256 string
	RawBytes           int64
	Retired            bool
}

func (g *GCOwner) ArchiveInventory(ctx context.Context, visit func(WALObject) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.check(ctx); e != nil {
		return e
	}
	if visit == nil {
		return ErrInvalid
	}
	prefix := g.r.root + "wal/"
	return g.r.store.List(ctx, prefix, MaxCatalogRecords, func(i s3store.Info) error {
		parts := strings.Split(strings.TrimPrefix(i.Key, prefix), "/")
		if !strings.HasPrefix(i.Key, prefix) || len(parts) != 2 {
			return ErrCorrupt
		}
		name := parts[1]
		key, e := g.r.WALKey(name)
		if e != nil || key != i.Key {
			return ErrCorrupt
		}
		info, e := g.r.store.Head(ctx, key)
		if e != nil {
			return e
		}
		if info.ETag == "" || len(info.ETag) > 256 {
			return ErrCorrupt
		}
		o := WALObject{Name: name, ETag: info.ETag}
		m := info.Metadata
		if m["cnpg-format"] == "wal-retired-v1" {
			b, _, e := g.r.store.Read(ctx, key, smallLimit)
			if e != nil {
				return e
			}
			var ret WALRetirement
			if strict(b, smallLimit, &ret) != nil || ret.Schema != 1 || ret.State != "retired" || ret.RepositoryID != g.r.id.RepositoryID || ret.Name != name || len(name) != 24 || ret.RawBytes != g.r.id.WALSegmentBytes || !hashRE.MatchString(ret.RawSHA256) || !validID(ret.GCOperationID) {
				return ErrCorrupt
			}
			o.Retired, o.RawBytes, o.SHA256 = true, ret.RawBytes, ret.RawSHA256
		} else {
			max := int64(1 << 20)
			segment := len(name) == 24 || strings.HasSuffix(name, ".partial")
			if segment {
				max = g.r.id.WALSegmentBytes
			}
			raw, e := strconv.ParseInt(m["cnpg-raw-bytes"], 10, 64)
			if e != nil || raw < 1 || raw > max || segment && raw != max || strconv.FormatInt(raw, 10) != m["cnpg-raw-bytes"] || info.Size < 1 || info.Size > max+(1<<20) || m["cnpg-format"] != "wal-v1" || m["cnpg-system-id"] != g.r.id.SystemIdentifier || !hashRE.MatchString(m["cnpg-raw-sha256"]) || !hashRE.MatchString(m["cnpg-stored-sha256"]) || (m["cnpg-compression"] != "none" && m["cnpg-compression"] != "gzip") {
				return ErrCorrupt
			}
			if m["cnpg-compression"] == "none" && info.Size != raw {
				return ErrCorrupt
			}
			o.RawBytes, o.SHA256 = raw, m["cnpg-raw-sha256"]
		}
		return visit(o)
	})
}

// Plan binds an already computed decision to this invocation, never a restart
// token. Execute still validates every victim and permanent parent exclusion.
func (g *GCOwner) Plan(cutoff string, policy Policy, victims []Victim) GCPlan {
	return GCPlan{1, g.r.id.RepositoryID, g.owner.OperationID, g.owner.ProcessID, cutoff, policy, victims}
}

// Cleanup inventories ALL payloads and MPUs before returning a bounded prefix
// of eligible victims. No age heuristic, partial-list entitlement or unclaimed
// namespace cleanup. New batches resume permanent retirements, never old owners.
func (g *GCOwner) Cleanup(ctx context.Context, limit int) ([]Victim, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.check(ctx); e != nil {
		return nil, e
	}
	if limit < 1 || limit > 128 {
		return nil, ErrInvalid
	}
	victims := []Victim{}
	eligible := func(uid, attempt string) (bool, error) {
		c, b, _, e := g.r.readCommit(ctx, uid)
		if s3store.Is(e, s3store.NotFound) {
			return true, nil
		}
		if e != nil {
			return false, e
		}
		e = g.r.live(ctx, c, b)
		if e != nil && e != ErrRetired {
			return false, e
		}
		return e == ErrRetired || c.AttemptID != attempt, nil
	}
	candidate := func(key, upload string) error {
		uid, _, ok := g.r.catalogKey(key)
		if !ok {
			return ErrCorrupt
		}
		if e := g.r.validateListedMetadata(ctx, key); e != nil {
			return e
		}
		parts := strings.Split(strings.TrimPrefix(key, g.r.root+"backups/"), "/")
		if len(parts) < 4 {
			return ErrCorrupt
		}
		attempt := parts[2]
		yes, e := eligible(uid, attempt)
		if e != nil {
			return e
		}
		if !yes {
			return nil
		}
		cl, e := g.r.readClaim(ctx, uid, attempt)
		if e != nil {
			return e
		}
		v := Victim{BackupUID: &uid, AttemptID: &attempt, SHA256: cl.RequestSHA256}
		if parts[3] == "manifest.pg.json" {
			v.Kind = "delete-manifest"
		} else if parts[3] == "data" && len(parts) == 5 {
			name := parts[4]
			comp := "none"
			if strings.HasSuffix(name, ".gz") {
				comp = "gzip"
				name = strings.TrimSuffix(name, ".gz")
			}
			n, ok := decimal(strings.TrimSuffix(name, ".tar"))
			if !ok || n >= 66 {
				return ErrCorrupt
			}
			i := int(n)
			v.Kind, v.Index, v.Compression = "delete-artifact", &i, &comp
		} else {
			return ErrCorrupt
		}
		if upload != "" {
			v.Kind = strings.Replace(v.Kind, "delete-", "abort-", 1)
			v.UploadID = &upload
		}
		if e := g.validateVictim(ctx, v, map[string]bool{}); e != nil {
			return e
		}
		if len(victims) < limit {
			victims = append(victims, v)
		}
		return nil
	}
	e := g.r.store.List(ctx, g.r.root+"backups/", MaxCatalogRecords, func(i s3store.Info) error {
		_, _, ok := g.r.catalogKey(i.Key)
		if !ok {
			return ErrCorrupt
		}
		if strings.HasSuffix(i.Key, "/manifest.pg.json") || strings.Contains(i.Key, "/data/") {
			return candidate(i.Key, "")
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	e = g.r.store.ListUploads(ctx, g.r.root+"backups/", MaxCatalogRecords, func(u s3store.Upload) error {
		if !validText(u.ID, 2048) {
			return ErrCorrupt
		}
		return candidate(u.Key, u.ID)
	})
	if e != nil {
		return nil, e
	}
	return victims, nil
}

// ArchivePosition is numeric and respects the immutable physical segment size.
func ArchivePosition(name string, size int64) (timeline uint32, start uint64, err error) {
	timeline, err = walTimeline(name, size)
	if err != nil || len(name) != 24 {
		return 0, 0, ErrInvalid
	}
	log, _ := strconv.ParseUint(name[8:16], 16, 32)
	seg, _ := strconv.ParseUint(name[16:24], 16, 32)
	return timeline, log<<32 | seg*uint64(size), nil
}
