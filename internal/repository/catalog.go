package repository

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type CatalogLimits struct {
	MaxRecords    int
	MaxSpoolBytes int64
}

func (l CatalogLimits) validate() error {
	if l.MaxRecords < 1 || l.MaxRecords > MaxCatalogRecords || l.MaxSpoolBytes < commitLimit || l.MaxSpoolBytes > 1<<40 {
		return ErrCapacity
	}
	return nil
}

type Entry struct {
	Commit       Commit
	CommitSHA256 string
	PublishedAt  time.Time
	Retired      bool
}

// Catalog is a private, disk-spooled complete snapshot. No caller sees partial
// LIST output. It retains no in-memory graph/index; full-parent edges are exact
// bounded GETs. A Hold must still be admitted when visiting a selection catalog.
type Catalog struct {
	mu     sync.Mutex
	hold   *Hold
	file   *os.File
	count  int
	closed bool
}

func (h *Hold) Catalog(ctx context.Context, limits CatalogLimits) (*Catalog, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return nil, e
	}
	if e := limits.validate(); e != nil {
		return nil, e
	}
	f, e := h.r.spoolCatalog(ctx, limits, true)
	if e != nil {
		return nil, e
	}
	return &Catalog{hold: h, file: f}, nil
}
func (r *Repository) spoolCatalog(ctx context.Context, l CatalogLimits, verify bool) (f *os.File, err error) {
	f, err = r.temp()
	if err != nil {
		return nil, err
	}
	owned := f
	defer func() {
		if err != nil {
			removeFile(owned)
		}
	}()
	enc := json.NewEncoder(f)
	count := 0
	size := int64(0)
	err = r.store.List(ctx, r.root+"backups/", MaxCatalogRecords, func(i s3store.Info) error {
		uid, kind, ok := r.catalogKey(i.Key)
		if !ok {
			return ErrCorrupt
		}
		if kind != "commit" {
			return nil
		}
		count++
		if count > l.MaxRecords {
			return ErrCapacity
		}
		c, b, info, e := r.readCommit(ctx, uid)
		if e != nil {
			return e
		}
		if _, e = r.requestFor(ctx, c); e != nil {
			return e
		}
		retired := false
		if e = r.live(ctx, c, b); e == ErrRetired {
			retired = true
		} else if e != nil {
			return e
		}
		if !retired {
			if e = r.parent(ctx, c); e != nil {
				return e
			}
			if verify {
				if e = r.verifyPayload(ctx, c); e != nil {
					return e
				}
			}
		}
		entry := Entry{c, digest(b), info.Modified, retired}
		eb, e := json.Marshal(entry)
		if e != nil {
			return e
		}
		size += int64(len(eb) + 1)
		if size > l.MaxSpoolBytes {
			return ErrCapacity
		}
		return enc.Encode(entry)
	})
	if err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	_, err = f.Seek(0, 0)
	return f, err
}

// Validate every enumerated backup key, including uncommitted attempts. Unknown
// format/path entries fail the COMPLETE catalog, not just the selected backup.
func (r *Repository) catalogKey(key string) (uid, kind string, ok bool) {
	p := strings.Split(strings.TrimPrefix(key, r.root+"backups/"), "/")
	if !strings.HasPrefix(key, r.root+"backups/") || len(p) < 2 || !validID(p[0]) {
		return "", "", false
	}
	uid = p[0]
	if len(p) == 2 {
		switch p[1] {
		case "commit.json":
			return uid, "commit", true
		case "request.json", "retired.json":
			return uid, "metadata", true
		}
		return "", "", false
	}
	if len(p) < 4 || p[1] != "attempts" || !validID(p[2]) {
		return "", "", false
	}
	if len(p) == 4 && (p[3] == "claim.json" || p[3] == "manifest.pg.json") {
		return uid, "attempt", true
	}
	if len(p) == 5 && p[3] == "data" {
		name := p[4]
		if strings.HasSuffix(name, ".gz") {
			name = strings.TrimSuffix(name, ".gz")
		}
		if !strings.HasSuffix(name, ".tar") {
			return "", "", false
		}
		n, ok := decimal(strings.TrimSuffix(name, ".tar"))
		return uid, "artifact", ok && n < 66
	}
	return "", "", false
}
func (c *Catalog) Visit(ctx context.Context, visit func(Entry) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || visit == nil {
		return ErrClosed
	}
	h := c.hold
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return e
	}
	return scanEntries(c.file, visit)
}
func scanEntries(f *os.File, visit func(Entry) error) error {
	if _, e := f.Seek(0, 0); e != nil {
		return e
	}
	d := json.NewDecoder(f)
	for {
		var v Entry
		e := d.Decode(&v)
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		if e = visit(v); e != nil {
			return e
		}
	}
}
func (c *Catalog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	name := c.file.Name()
	e := c.file.Close()
	re := os.Remove(name)
	if e != nil {
		return e
	}
	return re
}
