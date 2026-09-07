package configuration

import (
	"github.com/djosh34/cnpg_backup/internal/s3store"
	"time"
)

// Store constructs a client for exactly this already-validated generation. No
// ambient credentials or subsequent projection reads occur during the operation.
func (s *Snapshot) Store() (*s3store.Store, error) {
	if e := s.Spec.Validate(); e != nil {
		return nil, e
	}
	duration := func(v string) time.Duration { d, _ := time.ParseDuration(v); return d }
	c, i := s.Spec.S3, s.Spec.IO
	return s3store.New(s3store.Config{Endpoint: c.Endpoint, Bucket: c.Bucket, Prefix: c.Prefix, Signature: c.Signature, Addressing: c.Addressing, Region: c.Region, AccessKey: string(s.AccessKey), SecretKey: string(s.SecretKey), SessionToken: string(s.SessionToken), CA: s.CA,
		ConnectTimeout: duration(i.ConnectTimeout), MetadataTimeout: duration(i.MetadataTimeout), DataRequestTimeout: duration(i.DataRequestTimeout), OperationTimeout: duration(i.OperationTimeout), WALUploadTimeout: duration(i.WALUploadTimeout), ArtifactUploads: i.ArtifactUploads, PartWorkers: i.PartWorkers, WALUploads: i.WALUploads})
}
