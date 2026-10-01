package lake_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/garm-ai/sink/internal/lake"
)

// fakeS3 is enough of the S3 API for the three calls this package makes:
// HEAD a bucket, PUT a bucket, PUT an object.
//
// It is an httptest server rather than a mocked client, so the code under test
// builds real requests and real object keys — which is the only part of the
// upload path worth asserting here. What it deliberately does NOT cover is
// written down in KNOWN-GAPS: multipart, retries, and the behaviour of any
// specific store.
type fakeS3 struct {
	mu      sync.Mutex
	buckets map[string]bool
	objects map[string][]byte
	failPut bool
	// denyBucket answers the bucket check with 403, the way a store answers a
	// request it cannot attribute to anyone.
	denyBucket bool
	// token is the last X-Amz-Security-Token, which temporary credentials
	// must carry and long-lived ones must not.
	token string
	// auth is the Authorization header of the last request. It is the only
	// place the credentials and the region are visible as the client actually
	// used them: SigV4 puts them in the credential scope,
	// AWS4-HMAC-SHA256 Credential=<key>/<date>/<region>/s3/aws4_request.
	auth string
}

func newFakeS3(t *testing.T, buckets ...string) (*fakeS3, string) {
	t.Helper()
	f := &fakeS3{buckets: map[string]bool{}, objects: map[string][]byte{}}
	for _, b := range buckets {
		f.buckets[b] = true
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, strings.TrimPrefix(srv.URL, "http://")
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth = r.Header.Get("Authorization")
	f.token = r.Header.Get("X-Amz-Security-Token")
	p := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(p, "/")

	if r.URL.Query().Has("location") {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` +
			`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
		return
	}
	switch {
	case r.Method == http.MethodHead && key == "":
		if f.denyBucket {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if !f.buckets[bucket] {
			w.WriteHeader(http.StatusNotFound)
		}
	case r.Method == http.MethodPut && key == "":
		f.buckets[bucket] = true
	case r.Method == http.MethodPut:
		if f.failPut {
			// 403 rather than 500: minio-go retries a 500 with backoff, and
			// a test that spends five seconds proving an error is reported
			// is a test people start skipping.
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>no</Message></Error>`))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		f.objects[bucket+"/"+key] = body
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// objects is every key the store holds and its bytes, so a test can
// materialise what landed and read it with DuckDB.
func (f *fakeS3) snapshotObjects() map[string][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]byte, len(f.objects))
	for k, v := range f.objects {
		out[k] = v
	}
	return out
}

func (f *fakeS3) hasBucket(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buckets[name]
}

// lastAuth is the Authorization header the store last saw.
func (f *fakeS3) lastAuth() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auth
}

func (f *fakeS3) lastToken() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.token
}

// uploader is the anonymous one: these tests are about keys and failures, and
// an unsigned request is the cheapest way to reach the store. Anonymous is
// spelled out because it has to be — NewUploader refuses a configuration with
// no credentials and no --s3-anonymous, which is the whole point of the
// change that introduced this field.
func uploader(t *testing.T, endpoint, bucket string) *lake.Uploader {
	t.Helper()
	u, err := lake.NewUploader(lake.S3Config{Endpoint: endpoint, Bucket: bucket, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestEnsureBucketCreatesWhatIsMissing(t *testing.T) {
	fake, endpoint := newFakeS3(t)
	if err := uploader(t, endpoint, "garm-lake").Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fake.hasBucket("garm-lake") {
		t.Fatal("the bucket was not created")
	}
}

func TestEnsureBucketLeavesAnExistingBucketAlone(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	if err := uploader(t, endpoint, "garm-lake").Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fake.hasBucket("garm-lake") {
		t.Fatal("the bucket disappeared")
	}
}

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// The hive path is preserved and the file name carries the instance and the
// stream-sequence range. Two sinks sharing one work queue must never write the
// same key: the second write would silently replace the first, and the rows in
// it would be gone with every ack already sent.
func TestAnUploadedObjectKeepsItsHivePathAndGetsAnInstanceUniqueName(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	dir := writeTree(t, map[string]string{
		"date=2026-09-22/app=svc-a/data_0.parquet": "a",
		"date=2026-09-22/app=svc-b/data_0.parquet": "b",
	})
	keys, err := uploader(t, endpoint, "garm-lake").
		Publish(context.Background(), dir, "ledger", "box-77", 100, 250)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ledger/date=2026-09-22/app=svc-a/part-box-77-100-250-0.parquet",
		"ledger/date=2026-09-22/app=svc-b/part-box-77-100-250-1.parquet",
	}
	sort.Strings(keys)
	if strings.Join(keys, "\n") != strings.Join(want, "\n") {
		t.Fatalf("keys:\n%s\nwant:\n%s", strings.Join(keys, "\n"), strings.Join(want, "\n"))
	}
	for _, k := range want {
		if !containsKey(fake.keys(), "garm-lake/"+k) {
			t.Errorf("%q was reported as uploaded but never arrived", k)
		}
	}
}

// Anything that is not a .parquet file is left alone. DuckDB's COPY can leave
// other entries in the tree, and uploading them would put files in the lake
// that a reader's glob would then try to parse.
func TestOnlyParquetFilesAreUploaded(t *testing.T) {
	_, endpoint := newFakeS3(t, "garm-lake")
	dir := writeTree(t, map[string]string{
		"date=2026-09-22/app=svc-a/data_0.parquet": "a",
		"date=2026-09-22/app=svc-a/.DS_Store":      "junk",
	})
	keys, err := uploader(t, endpoint, "garm-lake").
		Publish(context.Background(), dir, "ledger", "box-77", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("uploaded %v, want only the parquet file", keys)
	}
}

// A store that refuses a write must fail the whole flush. Reporting success
// for a partial upload acks messages whose rows are not in the lake, and
// nothing will ever send them again.
func TestARefusedPutFailsTheWholeUpload(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	fake.failPut = true
	dir := writeTree(t, map[string]string{"date=2026-09-22/app=svc-a/data_0.parquet": "a"})
	if _, err := uploader(t, endpoint, "garm-lake").
		Publish(context.Background(), dir, "ledger", "box-77", 1, 2); err == nil {
		t.Fatal("a 500 from the store was reported as a successful upload")
	}
}

func containsKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

// The S3 half of the same startup line: the bucket and the endpoint, so two
// drains pointed at different stores are told apart in a log.
func TestDescribeNamesTheBucketAndTheEndpoint(t *testing.T) {
	_, endpoint := newFakeS3(t, "garm-lake")
	got := uploader(t, endpoint, "garm-lake").Describe()
	for _, want := range []string{"garm-lake", endpoint} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe() = %q, which does not name %q", got, want)
		}
	}
}

// The credentials and the region reach the store under the AWS-standard
// names. SigV4 puts both in the credential scope of the Authorization header,
// so this asserts what the store was actually sent rather than what the
// struct was filled with.
//
// The region is the half that had no code at all before v0.5.0: Options.Region
// was never set, so every client asked the store where the bucket lived and
// AWS_DEFAULT_REGION meant nothing here while meaning something to every other
// tool on the same machine.
func TestTheCredentialsAndTheRegionReachTheStore(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	u, err := lake.NewUploader(lake.S3Config{
		Endpoint: endpoint, Bucket: "garm-lake",
		AccessKey: "garmdev", SecretKey: "garmdevsecret", Region: "eu-west-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := u.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	auth := fake.lastAuth()
	for _, want := range []string{"AWS4-HMAC-SHA256", "Credential=garmdev/", "/eu-west-3/s3/aws4_request"} {
		if !strings.Contains(auth, want) {
			t.Errorf("the store was sent %q, which does not carry %q", auth, want)
		}
	}
}

// An unsigned request is what --s3-anonymous asks for, and nothing else gets
// it. The anonymous client signs nothing at all.
func TestAnonymousSendsNoSignature(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	if err := uploader(t, endpoint, "garm-lake").Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if auth := fake.lastAuth(); auth != "" {
		t.Errorf("an anonymous client signed a request: %q", auth)
	}
}

// A configuration that cannot be signed is refused where it is built, with the
// variable to export in the message.
//
// This is the bug in one test. Before, an empty access key silently became an
// anonymous client, the store answered the first call with "Access Denied",
// and the operator went looking at the store — which was fine — because
// nothing in the failure named a credential or a variable.
func TestAConfigurationThatCannotBeSignedIsRefusedByName(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  lake.S3Config
		want []string
	}{
		"nobody set anything": {
			cfg:  lake.S3Config{Endpoint: "127.0.0.1:9000", Bucket: "garm-lake"},
			want: []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "--s3-anonymous"},
		},
		"key without a secret": {
			cfg:  lake.S3Config{Endpoint: "127.0.0.1:9000", Bucket: "garm-lake", AccessKey: "garmdev"},
			want: []string{"AWS_SECRET_ACCESS_KEY"},
		},
		"anonymous with credentials": {
			cfg: lake.S3Config{Endpoint: "127.0.0.1:9000", Bucket: "garm-lake",
				AccessKey: "garmdev", SecretKey: "garmdevsecret", Anonymous: true},
			want: []string{"--s3-anonymous", "AWS_ACCESS_KEY_ID"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			u, err := lake.NewUploader(tc.cfg)
			if err == nil {
				t.Fatalf("built %v, which no store can be expected to accept", u.Describe())
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not name %q: %v", want, err)
				}
			}
		})
	}
}

// The startup line says how the drain authenticates, because "anonymous" was
// the state that used to be invisible until the store refused a call.
func TestDescribeSaysHowTheDrainAuthenticates(t *testing.T) {
	signed, err := lake.NewUploader(lake.S3Config{
		Endpoint: "127.0.0.1:28333", Bucket: "garm-lake",
		AccessKey: "garmdev", SecretKey: "garmdevsecret", Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"AWS_ACCESS_KEY_ID", "us-east-1"} {
		if !strings.Contains(signed.Describe(), want) {
			t.Errorf("the startup line %q does not name %q", signed.Describe(), want)
		}
	}
	if strings.Contains(signed.Describe(), "garmdevsecret") {
		t.Errorf("the startup line carries the secret: %q", signed.Describe())
	}

	anon, err := lake.NewUploader(lake.S3Config{Endpoint: "127.0.0.1:28333", Bucket: "garm-lake", Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"anonymous", "--s3-anonymous"} {
		if !strings.Contains(anon.Describe(), want) {
			t.Errorf("the startup line %q does not say it is unsigned", anon.Describe())
		}
	}
}

// A refused bucket check is the failure a human actually meets, so it carries
// the destination and the credential mode. "Access Denied" on its own sent one
// person into SeaweedFS for an hour over an environment variable.
func TestARefusedBucketCheckNamesTheDestinationAndTheCredentials(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	fake.denyBucket = true
	err := uploader(t, endpoint, "garm-lake").Ensure(context.Background())
	if err == nil {
		t.Fatal("a 403 on the bucket check was reported as success")
	}
	for _, want := range []string{"garm-lake", endpoint, "anonymous"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the failure does not name %q: %v", want, err)
		}
	}
}

// AWS_ENDPOINT_URL is a URL and minio.New takes a host:port, so both are
// accepted and the scheme decides TLS. The stack's compose sets
// http://seaweedfs:8333; a person types 127.0.0.1:28333.
func TestSplitEndpointTakesAURLOrAHostPort(t *testing.T) {
	for name, tc := range map[string]struct {
		raw      string
		ssl      bool
		wantHost string
		wantTLS  bool
		wantErr  string
	}{
		"host and port":        {raw: "127.0.0.1:28333", wantHost: "127.0.0.1:28333"},
		"host and port, --ssl": {raw: "s3.example:9000", ssl: true, wantHost: "s3.example:9000", wantTLS: true},
		"http url":             {raw: "http://seaweedfs:8333", wantHost: "seaweedfs:8333"},
		"https url":            {raw: "https://s3.example", wantHost: "s3.example", wantTLS: true},
		"http url beats --ssl": {raw: "http://seaweedfs:8333", ssl: true, wantHost: "seaweedfs:8333"},
		"trailing slash":       {raw: "http://seaweedfs:8333/", wantHost: "seaweedfs:8333"},
		"a path":               {raw: "http://seaweedfs:8333/garm-lake", wantErr: "path"},
		"another scheme":       {raw: "s3://seaweedfs:8333", wantErr: "scheme"},
		"nothing":              {raw: "", wantErr: "no host"},
	} {
		t.Run(name, func(t *testing.T) {
			host, secure, err := lake.SplitEndpoint(tc.raw, tc.ssl)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("SplitEndpoint(%q) = %q, %v; want an error naming %q", tc.raw, host, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if host != tc.wantHost || secure != tc.wantTLS {
				t.Fatalf("SplitEndpoint(%q, %v) = %q, %v; want %q, %v", tc.raw, tc.ssl, host, secure, tc.wantHost, tc.wantTLS)
			}
		})
	}
}

// Temporary credentials carry a session token, and a store refuses a request
// signed without one — the same bad-signature failure, from the same cause: a
// standard variable this binary did not happen to read.
func TestASessionTokenReachesTheStoreWhenThereIsOne(t *testing.T) {
	fake, endpoint := newFakeS3(t, "garm-lake")
	cfg := lake.S3Config{
		Endpoint: endpoint, Bucket: "garm-lake",
		AccessKey: "ASIAEXAMPLE", SecretKey: "s", Region: "us-east-1", SessionToken: "tok-123",
	}
	u, err := lake.NewUploader(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := u.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fake.lastToken(); got != "tok-123" {
		t.Errorf("the store was sent session token %q, want the one that was configured", got)
	}

	cfg.SessionToken = ""
	u, err = lake.NewUploader(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := u.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fake.lastToken(); got != "" {
		t.Errorf("a long-lived key was sent session token %q", got)
	}
}
