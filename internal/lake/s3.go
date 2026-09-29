package lake

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/garm-ai/sink/internal/drain"
)

// Uploader puts Parquet files on any S3-compatible store.
type Uploader struct {
	client *minio.Client
	bucket string
}

// NewUploader connects to an S3-compatible endpoint. An empty accessKey means
// anonymous (unsigned) requests — the default for a local dev stack with no
// IAM configured, and never something to run against a real store.
func NewUploader(endpoint, accessKey, secretKey string, useSSL bool, bucket string) (*Uploader, error) {
	opts := &minio.Options{Secure: useSSL}
	if accessKey != "" {
		opts.Creds = credentials.NewStaticV4(accessKey, secretKey, "")
	} else {
		opts.Creds = credentials.NewStatic("", "", "", credentials.SignatureAnonymous)
	}
	c, err := minio.New(endpoint, opts)
	if err != nil {
		return nil, err
	}
	return &Uploader{client: c, bucket: bucket}, nil
}

func (u *Uploader) EnsureBucket(ctx context.Context) error {
	ok, err := u.client.BucketExists(ctx, u.bucket)
	if err != nil {
		return fmt.Errorf("bucket check: %w", err)
	}
	if ok {
		return nil
	}
	return u.client.MakeBucket(ctx, u.bucket, minio.MakeBucketOptions{})
}

// UploadTree walks the hive tree WriteParquet produced and uploads each file
// under keyPrefix, renaming data files to instance-unique names so concurrent
// sink instances can never collide.
//
// The name carries the stream-sequence range, so an object can be traced back
// to the messages it came from without opening it.
func (u *Uploader) UploadTree(ctx context.Context, localDir, keyPrefix, instanceID string, firstSeq, lastSeq uint64) ([]string, error) {
	var keys []string
	n := 0
	err := filepath.WalkDir(localDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		rel, err := filepath.Rel(localDir, p)
		if err != nil {
			return err
		}
		name := fmt.Sprintf("part-%s-%d-%d-%d.parquet", instanceID, firstSeq, lastSeq, n)
		n++
		key := path.Join(keyPrefix, filepath.ToSlash(filepath.Dir(rel)), name)
		if _, err := u.client.FPutObject(ctx, u.bucket, key, p, minio.PutObjectOptions{
			ContentType: "application/vnd.apache.parquet",
		}); err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

// ParquetSink is the production drain.Sink: DuckDB writes the hive-partitioned
// Parquet locally, the tree is uploaded, and the temp dir is removed.
//
// Flush is all-or-nothing from the drain's point of view. A partial upload
// returns an error, the whole batch is naked and redelivered, and the objects
// that did land are written again under a new name — duplicate ROWS, which the
// lake is designed to hold, rather than a gap, which it is not.
type ParquetSink struct {
	Uploader   *Uploader
	KeyPrefix  string
	InstanceID string
}

func (s *ParquetSink) Flush(ctx context.Context, b drain.Batch) error {
	if len(b.Rows) == 0 {
		return nil
	}
	dir, err := os.MkdirTemp("", "sinkd-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := WriteParquet(dir, b.Rows); err != nil {
		return err
	}
	_, err = s.Uploader.UploadTree(ctx, dir, s.KeyPrefix, s.InstanceID, b.FirstSeq, b.LastSeq)
	return err
}
