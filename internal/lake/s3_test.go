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

func uploader(t *testing.T, endpoint, bucket string) *lake.Uploader {
	t.Helper()
	u, err := lake.NewUploader(endpoint, "", "", false, bucket)
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
