package lake_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/garm-ai/sink/internal/drain"
	"github.com/garm-ai/sink/internal/lake"
	"github.com/garm-ai/sink/internal/row"
)

// readLake is the query the README tells a developer to run, against whatever
// directory it is given: the hive glob, hive partitioning on, and a
// deterministic order so two destinations can be compared as text.
func readLake(t *testing.T, root string) string {
	t.Helper()
	db := openDuck(t)
	glob := filepath.Join(root, "ledger", "**", "*.parquet")
	res, err := db.Query(fmt.Sprintf(
		`SELECT event_id, tenant, app, input_tokens, outcome, CAST(date AS VARCHAR)
		 FROM read_parquet('%s', hive_partitioning=true) ORDER BY event_id`, glob))
	if err != nil {
		t.Fatalf("reading %s: %v", glob, err)
	}
	defer res.Close()
	var lines []string
	for res.Next() {
		var id, tenant, app, outcome, date string
		var in int64
		if err := res.Scan(&id, &tenant, &app, &in, &outcome, &date); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, strings.Join([]string{id, tenant, app, fmt.Sprint(in), outcome, date}, "|"))
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

func dirStore(t *testing.T, root string) *lake.DirStore {
	t.Helper()
	d, err := lake.NewDirStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}

func oneRowBatch(day time.Time, app string) drain.Batch {
	return drain.Batch{
		FirstSeq: 1, LastSeq: 1,
		Rows: []row.Row{{EventID: "e1", Time: day, Tenant: "acme", App: app, InputTokens: 1, Outcome: "ok", TagsJSON: "{}"}},
	}
}

// --lake-dir needs no credentials. The S3 environment variables are the S3
// destination's alone, and a lake on disk must not care whether they are set,
// unset, or wrong.
func TestALakeOnDiskNeedsNoCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	root := t.TempDir()
	sink := &lake.ParquetSink{Dest: dirStore(t, root), KeyPrefix: "ledger", InstanceID: "box-77"}
	if err := sink.Flush(context.Background(), oneRowBatch(time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC), "agentd")); err != nil {
		t.Fatal(err)
	}
	if got := readLake(t, root); !strings.HasPrefix(got, "e1|acme|agentd|1|ok|2026-09-29") {
		t.Fatalf("the lake reads back as %q", got)
	}
}

// Ensure creates the whole path. A developer passing --lake-dir ./lake has not
// created ./lake, and failing at the first flush rather than at startup means
// the failure arrives with a batch already unacked.
func TestEnsureCreatesTheLakeDirectoryIncludingItsParents(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b", "lake")
	dirStore(t, root)
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		t.Fatalf("stat %s: err=%v", root, err)
	}
}

func TestADirStoreNeedsAPath(t *testing.T) {
	if _, err := lake.NewDirStore(""); err == nil {
		t.Fatal("an empty --lake-dir was accepted; the lake would land in the working directory")
	}
}

// The root is absolute, so a relative --lake-dir keeps pointing where the
// operator typed it however the process's working directory moves.
func TestTheLakeRootIsAbsolute(t *testing.T) {
	d, err := lake.NewDirStore("lake")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(d.Root()) {
		t.Fatalf("Root() = %q, which is relative", d.Root())
	}
}

// A publish that fails leaves nothing a reader would pick up — no part file,
// and no half-written temporary either.
//
// The drain acks a batch because Flush returned nil. When it returns an error
// the whole batch is redelivered, so the lake must hold either the files or
// nothing; a truncated file at a real key would be read by every query from
// then on, and nothing would ever overwrite it.
func TestAFailedPublishLeavesNothingAReaderWouldPickUp(t *testing.T) {
	root := t.TempDir()
	d := dirStore(t, root)
	// A regular file where the key prefix's directory has to go: the first
	// MkdirAll under it fails, mid-flush, after DuckDB has written the
	// Parquet and before anything has landed.
	if err := os.WriteFile(filepath.Join(root, "ledger"), []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := &lake.ParquetSink{Dest: d, KeyPrefix: "ledger", InstanceID: "box-77"}
	err := sink.Flush(context.Background(), oneRowBatch(time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC), "agentd"))
	if err == nil {
		t.Fatal("a publish that could not create its directory reported success; the batch would have been acked")
	}
	if got := parquetKeys(t, root); len(got) != 0 {
		t.Fatalf("the failed flush left %v behind", got)
	}
	if got := incompleteFiles(t, root); len(got) != 0 {
		t.Fatalf("the failed flush left temporary files behind: %v", got)
	}
}

// A publish that fails part way through leaves whole files, never a prefix of
// one. The batch is redelivered and written again under a new name — duplicate
// rows, which the lake is designed to hold.
func TestAPublishThatFailsPartWayLeavesOnlyWholeFiles(t *testing.T) {
	root := t.TempDir()
	d := dirStore(t, root)
	day := time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC)
	// svc-b's partition directory cannot be created: a regular file is
	// already at that path. svc-a sorts first, so it lands and svc-b does not.
	blocked := filepath.Join(root, "ledger", "date=2026-09-29", "app=svc-b")
	if err := os.MkdirAll(filepath.Dir(blocked), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := &lake.ParquetSink{Dest: d, KeyPrefix: "ledger", InstanceID: "box-77"}
	if err := sink.Flush(context.Background(), twoAppBatch(day)); err == nil {
		t.Fatal("a partial publish reported success; half the batch would have been acked")
	}
	if got := incompleteFiles(t, root); len(got) != 0 {
		t.Fatalf("a failed publish left temporary files behind: %v", got)
	}
	// What did land is a whole Parquet file: DuckDB reads it rather than
	// failing on a truncated footer.
	if got := readLake(t, root); !strings.HasPrefix(got, "e1|acme|svc-a|11||2026-09-29") {
		t.Fatalf("what landed does not read back as a whole file: %q", got)
	}
}

// A torn write on a crash leaves a temporary file, not a corrupt part.
//
// This is the crash the atomic rename exists for: the process dies with bytes
// half written. The half-written name ends in .parquet.incomplete, which a
// reader's `**/*.parquet` glob does not match, so the next query returns the
// rows that were already there and does not fail on a truncated footer.
func TestATornWriteLeavesATemporaryFileAndNotACorruptPart(t *testing.T) {
	root := t.TempDir()
	d := dirStore(t, root)
	day := time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC)
	sink := &lake.ParquetSink{Dest: d, KeyPrefix: "ledger", InstanceID: "box-77"}
	if err := sink.Flush(context.Background(), oneRowBatch(day, "agentd")); err != nil {
		t.Fatal(err)
	}
	good := readLake(t, root)

	// Half of a real Parquet file, under the name a crash would have left it.
	whole := parquetKeys(t, root)
	if len(whole) != 1 {
		t.Fatalf("expected one part file, got %v", whole)
	}
	bytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(whole[0])))
	if err != nil {
		t.Fatal(err)
	}
	torn := filepath.Join(root, filepath.FromSlash(whole[0]))
	torn = filepath.Join(filepath.Dir(torn), "part-box-77-9-9-0.parquet"+lake.IncompleteSuffix)
	if err := os.WriteFile(torn, bytes[:len(bytes)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	if got := readLake(t, root); got != good {
		t.Fatalf("a torn write changed what a reader sees:\ngot:  %q\nwant: %q", got, good)
	}
	// And the sweep the README names finds it.
	if got := incompleteFiles(t, root); len(got) != 1 {
		t.Fatalf("the documented sweep found %v, want the one torn file", got)
	}
}

// incompleteFiles is the sweep the README documents:
// find <lake-dir> -name '*.parquet.incomplete'
func incompleteFiles(t *testing.T, root string) []string {
	t.Helper()
	out := []string{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet"+lake.IncompleteSuffix) {
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
	return out
}

// The part is written under a temporary name and renamed onto its key, rather
// than written at the key.
//
// On the happy path the difference is invisible, and on a crash it is the
// whole point, so it is pinned here by something only a rename can do:
// os.Rename replaces a read-only file, where opening that same path for
// writing is refused. Change copyThenRename to write straight to the key and
// this test fails with "permission denied".
func TestAPartIsRenamedOntoItsKeyRatherThanWrittenAtIt(t *testing.T) {
	root := t.TempDir()
	d := dirStore(t, root)
	day := time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC)

	// The key the flush below will produce, occupied by a file nothing is
	// allowed to open for writing.
	key := filepath.Join(root, "ledger", "date=2026-09-29", "app=agentd", "part-box-77-1-1-0.parquet")
	if err := os.MkdirAll(filepath.Dir(key), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte("stale"), 0o444); err != nil {
		t.Fatal(err)
	}

	sink := &lake.ParquetSink{Dest: d, KeyPrefix: "ledger", InstanceID: "box-77"}
	if err := sink.Flush(context.Background(), oneRowBatch(day, "agentd")); err != nil {
		t.Fatalf("the part was not renamed onto its key: %v", err)
	}
	if got := readLake(t, root); !strings.HasPrefix(got, "e1|acme|agentd|1|ok|2026-09-29") {
		t.Fatalf("the key still holds the old file: %q", got)
	}
}

// A temporary name must never be something a reader's glob matches. This is
// the whole reason the suffix is a suffix and not a hidden directory: DuckDB's
// `**` matches dot-directories, so hiding the staging area would not have
// hidden it from a query.
func TestTheTemporaryNameIsNotMatchedByAReadersGlob(t *testing.T) {
	if strings.HasSuffix(lake.IncompleteSuffix, ".parquet") || lake.IncompleteSuffix == "" {
		t.Fatalf("IncompleteSuffix = %q; a reader's **/*.parquet would open a half-written file", lake.IncompleteSuffix)
	}
}

// Ensure fails at startup when the root cannot be a directory, rather than at
// the first flush with a batch already unacked.
func TestEnsureFailsWhenTheLakeRootCannotBeADirectory(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "lake")
	if err := os.WriteFile(blocked, []byte("a file, not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := lake.NewDirStore(blocked)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Ensure(context.Background()); err == nil {
		t.Fatal("a lake rooted at a regular file started anyway")
	}
}

// Describe is the startup line: it has to name the path, because "which
// destination is this drain using" is the first question an operator asks of
// a log they did not start themselves.
func TestDescribeNamesThePath(t *testing.T) {
	root := t.TempDir()
	if got := dirStore(t, root).Describe(); !strings.Contains(got, root) {
		t.Fatalf("Describe() = %q, which does not name %q", got, root)
	}
}
