package lake

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Uploader is the Destination that puts Parquet on any S3-compatible store.
type Uploader struct {
	client   *minio.Client
	endpoint string
	bucket   string
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
	return &Uploader{client: c, endpoint: endpoint, bucket: bucket}, nil
}

func (u *Uploader) Describe() string { return "s3://" + u.bucket + " at " + u.endpoint }

// Ensure creates the bucket if it is missing.
func (u *Uploader) Ensure(ctx context.Context) error {
	ok, err := u.client.BucketExists(ctx, u.bucket)
	if err != nil {
		return fmt.Errorf("bucket check: %w", err)
	}
	if ok {
		return nil
	}
	return u.client.MakeBucket(ctx, u.bucket, minio.MakeBucketOptions{})
}

// Publish uploads the hive tree WriteParquet produced, each file under its key.
//
// There is no half-written object to guard against: S3 makes a PUT visible
// only when it completes, so a torn upload leaves no key at all. The local
// destination has to arrange the same thing for itself — see DirStore.
func (u *Uploader) Publish(ctx context.Context, localDir, keyPrefix, instanceID string, firstSeq, lastSeq uint64) ([]string, error) {
	return walkParts(localDir, keyPrefix, instanceID, firstSeq, lastSeq, func(p, key string) error {
		if _, err := u.client.FPutObject(ctx, u.bucket, key, p, minio.PutObjectOptions{
			ContentType: "application/vnd.apache.parquet",
		}); err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}
		return nil
	})
}
