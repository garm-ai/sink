package lake

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// The environment variables this destination is configured by. They are the
// AWS-standard names, which is the whole point of having them here as
// constants: the AWS CLI, every SDK, DuckDB's httpfs, the other components on
// this platform and the compose file that runs them all read exactly these, so
// a person who has exported them for one tool has exported them for sinkd too.
//
// They live in this package rather than in the command because the errors that
// name them are raised here, and a name spelled in two places is a name that
// will be misspelled in one of them.
//
// AWS_ENDPOINT_URL has no counterpart for the bucket: AWS addresses a bucket
// in the URL, so there is no standard variable to read one from. The bucket's
// variable, S3_BUCKET, stays ours and the flag help says so.
const (
	EnvAccessKey     = "AWS_ACCESS_KEY_ID"
	EnvSecretKey     = "AWS_SECRET_ACCESS_KEY"
	EnvSessionToken  = "AWS_SESSION_TOKEN"
	EnvRegion        = "AWS_REGION"
	EnvDefaultRegion = "AWS_DEFAULT_REGION"
	EnvEndpoint      = "AWS_ENDPOINT_URL"
)

// Uploader is the Destination that puts Parquet on any S3-compatible store.
type Uploader struct {
	client    *minio.Client
	endpoint  string
	bucket    string
	region    string
	anonymous bool
}

// S3Config is everything the object-store destination needs.
//
// It is a struct rather than a parameter list because four of its fields are
// strings: NewUploader(endpoint, access, secret, region, bucket) is one
// careless edit away from signing requests with a bucket name, and the
// compiler would have nothing to say about it. Same reason the INSERT in
// parquet.go builds its arguments from a table rather than by position.
type S3Config struct {
	// Endpoint is host:port, or a URL with a scheme. AWS_ENDPOINT_URL is
	// spelled with one — the stack's compose sets http://seaweedfs:8333 —
	// and minio wants it without, so both are accepted here.
	Endpoint string
	Bucket   string

	// AccessKey and SecretKey are AWS_ACCESS_KEY_ID and
	// AWS_SECRET_ACCESS_KEY. Both empty is a configuration only Anonymous
	// makes valid.
	AccessKey string
	SecretKey string

	// SessionToken is AWS_SESSION_TOKEN, empty for a long-lived key. It is
	// read because temporary credentials without it are refused by the store
	// as a bad signature — the same unhelpful failure, from the same cause:
	// a standard variable this binary did not happen to read.
	SessionToken string

	// Region is AWS_REGION, or AWS_DEFAULT_REGION. Empty means the client
	// asks the store where the bucket lives, which is what this did when the
	// region was not read at all.
	Region string

	// UseSSL is the --s3-ssl flag. A scheme on Endpoint overrides it, because
	// a person who typed https:// has said which they meant.
	UseSSL bool

	// Anonymous sends unsigned requests, for a store with no IAM configured.
	// It is a deliberate choice with a flag of its own, not what an unset
	// variable decays into.
	Anonymous bool
}

// NewUploader connects to an S3-compatible endpoint.
//
// It refuses a configuration it cannot sign rather than quietly sending
// unsigned requests. That fallback was the expensive bug: with no credentials
// the client signed nothing, the store answered the first call with "Access
// Denied", and nothing in that sentence mentioned a credential or a variable —
// so the search started at the store, which was fine, and reached the variable
// names last. An unset variable and a store with no IAM are different
// situations, and they now have different spellings: the variables, or
// --s3-anonymous.
func NewUploader(cfg S3Config) (*Uploader, error) {
	host, secure, err := SplitEndpoint(cfg.Endpoint, cfg.UseSSL)
	if err != nil {
		return nil, err
	}
	opts := &minio.Options{Secure: secure, Region: cfg.Region}
	switch {
	case cfg.Anonymous && cfg.AccessKey != "":
		return nil, errors.New("--s3-anonymous with " + EnvAccessKey + " set: unset one of them, " +
			"because there is no way to tell which was meant")
	case cfg.Anonymous:
		opts.Creds = credentials.NewStatic("", "", "", credentials.SignatureAnonymous)
	case cfg.AccessKey == "":
		return nil, errors.New("no S3 credentials: export " + EnvAccessKey + " and " + EnvSecretKey +
			", or pass --s3-anonymous for a store with no IAM configured")
	case cfg.SecretKey == "":
		return nil, errors.New(EnvAccessKey + " is set and " + EnvSecretKey + " is empty: " +
			"a request signed with an empty secret is refused by the store as a bad signature")
	default:
		opts.Creds = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)
	}
	c, err := minio.New(host, opts)
	if err != nil {
		return nil, err
	}
	return &Uploader{
		client: c, endpoint: host, bucket: cfg.Bucket,
		region: cfg.Region, anonymous: cfg.Anonymous,
	}, nil
}

// SplitEndpoint turns what an operator or the environment supplies into the
// host:port minio takes, and decides TLS.
//
// AWS_ENDPOINT_URL is a URL by definition and the stack's compose sets
// http://seaweedfs:8333, while minio.New takes a bare host:port and rejects
// anything with a scheme. Rather than make the operator strip it, the scheme
// is read: it is the clearest statement of intent available about TLS, so it
// beats --s3-ssl, and a bare host:port leaves --s3-ssl in charge.
func SplitEndpoint(raw string, ssl bool) (host string, secure bool, err error) {
	host = strings.TrimSuffix(strings.TrimSpace(raw), "/")
	secure = ssl
	if scheme, rest, found := strings.Cut(host, "://"); found {
		switch scheme {
		case "http":
			secure = false
		case "https":
			secure = true
		default:
			return "", false, fmt.Errorf("s3 endpoint %q: scheme %q is not http or https", raw, scheme)
		}
		host = rest
	}
	if host == "" {
		return "", false, fmt.Errorf("s3 endpoint %q names no host", raw)
	}
	if strings.Contains(host, "/") {
		return "", false, fmt.Errorf("s3 endpoint %q names a path; it must be host:port", raw)
	}
	return host, secure, nil
}

// Describe names the destination for the startup line — and, since v0.5.0, how
// it authenticates.
//
// The credential half is here because the startup line is where an operator
// looks first and because "anonymous" is the state that used to be invisible
// until the store refused a call. A drain that is sending unsigned requests
// says so in its first line of log, in the same field that says which bucket
// it is filling.
func (u *Uploader) Describe() string {
	who := "signed with " + EnvAccessKey
	if u.anonymous {
		who = "anonymous, unsigned (--s3-anonymous)"
	}
	where := "region " + u.region
	if u.region == "" {
		where = "no region set (" + EnvRegion + " and " + EnvDefaultRegion + " unset)"
	}
	return "s3://" + u.bucket + " at " + u.endpoint + " (" + where + ", " + who + ")"
}

// Ensure creates the bucket if it is missing.
func (u *Uploader) Ensure(ctx context.Context) error {
	ok, err := u.client.BucketExists(ctx, u.bucket)
	if err != nil {
		return fmt.Errorf("bucket check on %s: %w", u.Describe(), err)
	}
	if ok {
		return nil
	}
	return u.client.MakeBucket(ctx, u.bucket, minio.MakeBucketOptions{Region: u.region})
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
