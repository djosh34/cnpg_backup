# PostgreSQL backup and recovery

Language for physical backups of CloudNativePG-managed PostgreSQL clusters stored in S3-compatible object storage.

## Language

**Full backup**:
A physical base backup containing the complete backed-up database cluster rather than references to data omitted in favor of another backup.

**Differential backup**:
A physical backup whose reference is a full backup; recovering it requires that full backup and the selected differential, not intervening differentials.
_Avoid_: Diff (ambiguous between filesystem comparison and a backup type)

**Incremental backup**:
A physical backup whose omitted data depends on an earlier backup, which may itself be full or incremental. Differential is the special case whose reference is full.

**WAL archive**:
The remotely preserved PostgreSQL write-ahead log segments and relevant history files used for recovery.
_Avoid_: Backup (when referring only to WAL)

**Recovery target**:
The PostgreSQL state to which recovery should proceed, identified by supported target criteria such as time, LSN or a named restore point.

**Recovery window**:
The interval of recovery targets supported by available usable backups and the required continuous WAL along the relevant timeline history.
_Avoid_: Object age (not a measure of recoverability)

**Backup chain**:
A full backup and the dependent backups required to reconstruct a selected backup.
