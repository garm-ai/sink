package lake_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/garm-ai/sink/internal/drain"
	"github.com/garm-ai/sink/internal/lake"
	"github.com/garm-ai/sink/internal/row"
)

// destCase is one destination and a way to see what arrived at it.
//
// The sink's behaviour — which partitions get a file, what the file is called,
// that an empty batch writes nothing — is the same promise whichever
// destination is configured, so these tests run against both rather than
// against S3 with the directory taken on trust. The `keys` of the two are
// compared directly in TestBothDestinationsReceiveTheSameKeys.
type destCase struct {
	name string
	dest lake.Destination
	// keys is what landed, as keys: the store's object keys with the bucket
	// stripped, or the paths under the lake directory.
	keys func() []string
	// tree is a directory holding what landed, laid out by key, so DuckDB can
	// read either destination's result the same way.
	tree func(t *testing.T) string
}

func destinations(t *testing.T) []destCase {
	t.Helper()
	fake, endpoint := newFakeS3(t, "garm-lake")
	up := uploader(t, endpoint, "garm-lake")

	root := t.TempDir()
	dir, err := lake.NewDirStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	return []destCase{
		{
			name: "s3",
			dest: up,
			keys: func() []string {
				out := []string{}
				for _, k := range fake.keys() {
					out = append(out, strings.TrimPrefix(k, "garm-lake/"))
				}
				sort.Strings(out)
				return out
			},
			tree: func(t *testing.T) string {
				t.Helper()
				d := t.TempDir()
				for k, body := range fake.snapshotObjects() {
					p := filepath.Join(d, filepath.FromSlash(strings.TrimPrefix(k, "garm-lake/")))
					if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(p, body, 0o644); err != nil {
						t.Fatal(err)
					}
				}
				return d
			},
		},
		{
			name: "dir",
			dest: dir,
			keys: func() []string { return parquetKeys(t, root) },
			tree: func(*testing.T) string { return root },
		},
	}
}

// parquetKeys lists every .parquet under root as a slash-separated key, which
// is exactly the form the object store reports.
func parquetKeys(t *testing.T, root string) []string {
	t.Helper()
	out := []string{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func twoAppBatch(day time.Time) drain.Batch {
	return drain.Batch{
		FirstSeq: 10, LastSeq: 12,
		Rows: []row.Row{
			{EventID: "e1", Time: day, Tenant: "acme", App: "svc-a", InputTokens: 11, TagsJSON: "{}"},
			{EventID: "e2", Time: day, Tenant: "acme", App: "svc-b", InputTokens: 22, TagsJSON: "{}"},
		},
	}
}

// The two halves together: rows in, files at their keys. This is the seam
// `drain` depends on — it acks a batch because Flush returned nil — so "the
// Parquet was written but nothing was published" has to be impossible rather
// than merely unlikely.
func TestTheParquetSinkWritesTheBatchAndPublishesEveryPartition(t *testing.T) {
	for _, tc := range destinations(t) {
		t.Run(tc.name, func(t *testing.T) {
			day := time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)
			sink := &lake.ParquetSink{Dest: tc.dest, KeyPrefix: "ledger", InstanceID: "box-77"}
			if err := sink.Flush(context.Background(), twoAppBatch(day)); err != nil {
				t.Fatal(err)
			}
			got := tc.keys()
			if len(got) != 2 {
				t.Fatalf("published %v, want one file per app partition", got)
			}
			for _, want := range []string{
				"ledger/date=2026-09-22/app=svc-a/part-box-77-10-12-",
				"ledger/date=2026-09-22/app=svc-b/part-box-77-10-12-",
			} {
				found := false
				for _, k := range got {
					found = found || strings.HasPrefix(k, want)
				}
				if !found {
					t.Errorf("nothing under %q; published %v", want, got)
				}
			}
			// The scratch directory is removed even on the happy path: these
			// are tens of megabytes each, every few minutes, forever.
			if entries, _ := filepath.Glob(filepath.Join(os.TempDir(), "sinkd-*")); len(entries) > 0 {
				t.Errorf("the sink left scratch directories behind: %v", entries)
			}
		})
	}
}

// An empty batch must not write a file. An empty Parquet file in the lake is a
// file every reader's glob still opens.
func TestAnEmptyBatchPublishesNothing(t *testing.T) {
	for _, tc := range destinations(t) {
		t.Run(tc.name, func(t *testing.T) {
			sink := &lake.ParquetSink{Dest: tc.dest, KeyPrefix: "ledger", InstanceID: "box-77"}
			if err := sink.Flush(context.Background(), drain.Batch{}); err != nil {
				t.Fatal(err)
			}
			if got := tc.keys(); len(got) != 0 {
				t.Fatalf("an empty batch published %v", got)
			}
		})
	}
}

// The promise of the local destination in one assertion: the same batch
// produces the same keys on both, so a directory written by --lake-dir can be
// copied into a bucket and read by the queries already written against it.
func TestBothDestinationsReceiveTheSameKeys(t *testing.T) {
	day := time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)
	seen := map[string][]string{}
	for _, tc := range destinations(t) {
		sink := &lake.ParquetSink{Dest: tc.dest, KeyPrefix: "ledger", InstanceID: "box-77"}
		if err := sink.Flush(context.Background(), twoAppBatch(day)); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		seen[tc.name] = tc.keys()
	}
	if strings.Join(seen["s3"], "\n") != strings.Join(seen["dir"], "\n") {
		t.Fatalf("the destinations disagree about the keys:\ns3:\n%s\ndir:\n%s",
			strings.Join(seen["s3"], "\n"), strings.Join(seen["dir"], "\n"))
	}
	if len(seen["dir"]) == 0 {
		t.Fatal("neither destination received anything; the comparison proved nothing")
	}
}

// And the same rows, read back by the engine anyone will point at the result.
//
// A key that matches is not enough: the bytes under it have to be the same
// Parquet. This drains a batch to both destinations, reads each with
// read_parquet over a hive glob, and compares the result sets.
func TestTheLocalLakeReadsBackTheSameRowsAsTheObjectStore(t *testing.T) {
	day := time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)
	b := drain.Batch{
		FirstSeq: 10, LastSeq: 12,
		Rows: []row.Row{
			{EventID: "e1", Time: day, Tenant: "acme", App: "svc-a", InputTokens: 11, Outcome: "ok", TagsJSON: "{}"},
			{EventID: "e2", Time: day, Tenant: "acme", App: "svc-b", InputTokens: 22, Outcome: "denied", TagsJSON: "{}"},
			{EventID: "e3", Time: day.AddDate(0, 0, 1), Tenant: "acme", App: "svc-a", InputTokens: 33, Outcome: "ok", TagsJSON: "{}"},
		},
	}
	got := map[string]string{}
	for _, tc := range destinations(t) {
		sink := &lake.ParquetSink{Dest: tc.dest, KeyPrefix: "ledger", InstanceID: "box-77"}
		if err := sink.Flush(context.Background(), b); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got[tc.name] = readLake(t, tc.tree(t))
	}
	if got["dir"] != got["s3"] {
		t.Fatalf("the two destinations hold different rows:\ndir:\n%s\ns3:\n%s", got["dir"], got["s3"])
	}
	// The comparison is only worth something if it read the rows that went in.
	for _, want := range []string{"e1|acme|svc-a|11|ok|2026-09-22", "e3|acme|svc-a|33|ok|2026-09-23"} {
		if !strings.Contains(got["dir"], want) {
			t.Errorf("the lake does not hold %q:\n%s", want, got["dir"])
		}
	}
}
