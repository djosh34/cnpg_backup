# PostgreSQL backup and recovery

Terms for physical backups of CloudNativePG-managed PostgreSQL clusters stored in S3-compatible object storage.

## Language

**Full backup**:
A physical base backup containing the complete backed-up database cluster without a dependency on another backup.

**Differential backup**:
A physical backup whose reference is a full backup. Recovery requires that full and the selected differential, not intervening differentials.
_Avoid_: Diff, incremental chain

**Incremental backup**:
A physical backup whose omitted data depends on an earlier backup, which may itself be full or incremental. A differential is the special case whose reference is full.

**Backup chain**:
The full backup and any dependent backup needed to reconstruct a selected backup. In this product, a chain is one full or one full plus one differential.

**Repository lineage**:
An independently owned history of physical backups and archived WAL. A restored cluster has its own lineage even when its PostgreSQL system identifier matches its source.
_Avoid_: Cluster name, system identifier alone

**WAL archive**:
Remotely preserved PostgreSQL write-ahead log files used for recovery, with the timeline history needed to interpret them.
_Avoid_: Backup, when referring only to WAL

**Bundled WAL**:
WAL preserved with a physical backup to make that backup consistent. It is distinct from archived WAL needed to recover beyond the backup.

**Archive frontier**:
The end of the known archived WAL considered for a recovery plan. It does not include unarchived source transactions or prove that all preceding WAL is present.

**Recovery target**:
The PostgreSQL state to which recovery proceeds, identified by a supported time, LSN, transaction ID, named restore point, or consistency boundary.

**Recovery window**:
The interval of recovery targets supported by usable backups and continuous WAL along the relevant timeline history.
_Avoid_: Object age, configured retention period alone

**Backup retirement**:
The permanent withdrawal of a backup from recovery selection, distinct from removal of its stored bytes.

**Restore lifetime holder**:
Repository-wide deletion protection for an entire restore operation, including replay after materialization.

**Process-reader holder**:
Repository-wide deletion protection for one restore reader's admitted work. It is separate from the restore lifetime holder.

**Poisoned target**:
A recovery volume whose previous writer's termination or write completion is uncertain. It is not safe for another recovery attempt to reuse.
