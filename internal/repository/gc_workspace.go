package repository

// GC holds one catalog spool and downloads one manifest at a time (including
// parents). History retrieval needs at most 3MiB; plans/control writes at most
// 4MiB. 384MiB covers that aggregate plus allocation/metadata headroom. These
// are Go-bounded files, not an emptyDir eviction limit used to stop native tools.
const (
	GCCatalogBytes   int64 = 256 << 20
	GCWorkspaceBytes int64 = 384 << 20
)
