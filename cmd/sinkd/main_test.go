package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/garm-ai/sink/internal/drain"
)

// A parent that prints help and exits 0 is a green deploy that moved no data.
// This binary runs unattended; `sinkd drain $STREAM` with the variable
// unset has to fail loudly.
func TestEveryParentCommandRefusesInsteadOfPrintingHelp(t *testing.T) {
	for _, path := range [][]string{nil, {"drain"}, {"tail"}} {
		cmd, _, err := newRoot().Find(path)
		if err != nil {
			t.Fatalf("%v: %v", path, err)
		}
		if cmd.Run != nil {
			t.Errorf("%v runs something of its own; it must only dispatch", path)
		}
		if cmd.RunE == nil {
			t.Fatalf("%v has no RunE: it would print help and exit 0", path)
		}
		if err := cmd.RunE(cmd, nil); err == nil {
			t.Errorf("%v with no subcommand succeeded", path)
		}
		if err := cmd.RunE(cmd, []string{"nosuch"}); err == nil || !strings.Contains(err.Error(), "nosuch") {
			t.Errorf("%v did not quote the unknown subcommand: %v", path, err)
		}
	}
}

func TestBothStreamsHaveADrainAndATailSubcommand(t *testing.T) {
	for _, verb := range []string{"drain", "tail"} {
		for _, name := range []string{"ledger", "audit"} {
			if cmd, rest, err := newRoot().Find([]string{verb, name}); err != nil || len(rest) > 0 {
				t.Fatalf("%s %s does not resolve: cmd=%v rest=%q err=%v", verb, name, cmd, rest, err)
			}
		}
	}
}

// A tail has no durable, no S3 and no batch: the flags that would make it a
// drain must not be there, and the ones that make it a reader must.
func TestATailHasAReadersFlagsAndNotADrains(t *testing.T) {
	for _, origin := range []string{"ledger", "audit"} {
		f := flags(t, "tail", origin)
		for _, must := range []string{"since", "from-start", "follow", "filter", "pretty", "no-detail", "nats-url"} {
			if _, ok := f[must]; !ok {
				t.Errorf("tail %s has no --%s", origin, must)
			}
		}
		for _, mustNot := range []string{"durable", "s3-endpoint", "lake-dir", "batch-max-rows"} {
			if _, ok := f[mustNot]; ok {
				t.Errorf("tail %s has --%s; a tail is not a drain", origin, mustNot)
			}
		}
		if f["since"] != "100" {
			t.Errorf("tail %s --since defaults to %q, want the last 100 messages", origin, f["since"])
		}
	}
}

func flags(t *testing.T, path ...string) map[string]string {
	t.Helper()
	cmd, _, err := newRoot().Find(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) { out[f.Name] = f.DefValue })
	return out
}

// The defaults are a design decision, not a preference, so they are pinned.
//
// 1000 rows every 30s — the monorepo's — hive-partitioned by (date, app)
// produces up to 2,880 files per day per app, each a few hundred kilobytes.
// Below roughly 100k rows a Parquet file is mostly footer and column
// statistics, every query pays a LIST over thousands of keys, and object
// stores bill per request.
func TestTheDefaultsProduceParquetFilesWorthQuerying(t *testing.T) {
	if drain.DefaultBatchMaxRows < 100_000 {
		t.Errorf("DefaultBatchMaxRows = %d; below ~100k the per-file overhead dominates the data",
			drain.DefaultBatchMaxRows)
	}
	if drain.DefaultBatchInterval < time.Minute {
		t.Errorf("DefaultBatchInterval = %s; a sub-minute interval is a job for the stream, not the lake",
			drain.DefaultBatchInterval)
	}
	for _, origin := range []string{"ledger", "audit"} {
		f := flags(t, "drain", origin)
		if f["batch-max-rows"] != "250000" || f["batch-interval"] != "5m0s" {
			t.Errorf("drain %s: batch-max-rows=%q batch-interval=%q", origin, f["batch-max-rows"], f["batch-interval"])
		}
	}
}

// The durable name is a thing in NATS, not in this binary: two drains sharing
// one would split each other's stream, and renaming one orphans the consumer
// and replays from the beginning.
func TestEachDrainHasItsOwnDurableAndKeyPrefix(t *testing.T) {
	ledger, audit := flags(t, "drain", "ledger"), flags(t, "drain", "audit")
	if ledger["durable"] == audit["durable"] {
		t.Errorf("both drains share the durable %q; they would split each other's stream", ledger["durable"])
	}
	if ledger["key-prefix"] == audit["key-prefix"] {
		t.Errorf("both drains write under %q; the two record types would be one table", ledger["key-prefix"])
	}
}

func TestTheEnvironmentFallbacksAreRead(t *testing.T) {
	t.Setenv("NATS_URL", "nats://example:4222")
	t.Setenv("S3_ENDPOINT", "s3.example:9000")
	t.Setenv("S3_BUCKET", "other-bucket")
	t.Setenv("LAKE_DIR", "/srv/lake")
	f := flags(t, "drain", "ledger")
	for name, want := range map[string]string{
		"nats-url":    "nats://example:4222",
		"s3-endpoint": "s3.example:9000",
		"s3-bucket":   "other-bucket",
		"lake-dir":    "/srv/lake",
	} {
		if f[name] != want {
			t.Errorf("flag %q default = %q, want the environment value %q", name, f[name], want)
		}
	}
}

// AckWait must outlast a whole cycle. Left at some fixed two minutes it
// expires mid-flush under a five-minute interval: JetStream redelivers the
// whole batch to another instance and both write the same rows, continuously
// rather than on failure.
func TestAckWaitOutlastsAWholeBatchCycle(t *testing.T) {
	cfg := drain.Config{BatchInterval: 5 * time.Minute, FlushTimeout: 5 * time.Minute}
	if got := ackWait(cfg); got <= cfg.BatchInterval+cfg.FlushTimeout {
		t.Errorf("ackWait = %s, which expires during a batch that waited %s and flushed for %s",
			got, cfg.BatchInterval, cfg.FlushTimeout)
	}
	// A zero config means the defaults, not a zero AckWait.
	if got := ackWait(drain.Config{}); got <= drain.DefaultBatchInterval {
		t.Errorf("ackWait on an empty config = %s", got)
	}
}

// instance-id must never be empty or hostname-only: Parquet object names
// depend on it to stay unique across the instances sharing one work queue,
// which is what lets you run any number of sinks without leader election.
func TestInstanceID(t *testing.T) {
	for name, tc := range map[string]struct{ host, want string }{
		"plain":     {"box", "box-77"},
		"fqdn":      {"box.internal.example", "box_internal_example-77"},
		"empty":     {"", "unknown-77"},
		"only dots": {"...", "___-77"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := instanceID(tc.host, 77); got != tc.want {
				t.Fatalf("instanceID(%q, 77) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

func TestDefaultInstanceIDIsNonEmpty(t *testing.T) {
	if id := defaultInstanceID(); id == "" || !strings.Contains(id, "-") {
		t.Fatalf("defaultInstanceID() = %q; object names would collide", id)
	}
}

// --dry-run prints what would be applied without connecting to anything, so
// the difference between the two streams can be read before it is created.
func TestProvisionDryRunShowsTheOppositeDiscardPolicies(t *testing.T) {
	root := newRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"provision", "--dry-run"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"GARM_LEDGER", "GARM_AUDIT", "GARM_SINK_DEAD",
		"discard=DiscardOld", "discard=DiscardNew",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("--dry-run output does not mention %q:\n%s", want, got)
		}
	}
}

// Neither destination is a default, and the two are mutually exclusive.
//
// The old default was 127.0.0.1:9000 and a bucket named garm-lake, which meant
// a forgotten flag started a drain, took a durable consumer, and failed five
// minutes later with a batch already unacked. There are two destinations now
// and no way to guess which was meant, so the command refuses at startup and
// names both.
func TestADrainTakesOneDestinationAndRefusesBothOrNeither(t *testing.T) {
	for name, tc := range map[string]struct {
		o        drainOpts
		wantType string
		wantErr  []string
	}{
		"neither": {
			o:       drainOpts{},
			wantErr: []string{"--lake-dir", "--s3-endpoint", "--s3-bucket"},
		},
		"both": {
			o:       drainOpts{lakeDir: "./lake", s3Endpoint: "127.0.0.1:9000", s3Bucket: "garm-lake"},
			wantErr: []string{"--lake-dir", "--s3-endpoint", "two different destinations"},
		},
		"lake dir and a stray bucket": {
			o:       drainOpts{lakeDir: "./lake", s3Bucket: "garm-lake"},
			wantErr: []string{"two different destinations"},
		},
		"endpoint without a bucket": {
			o:       drainOpts{s3Endpoint: "127.0.0.1:9000"},
			wantErr: []string{"--s3-bucket"},
		},
		"bucket without an endpoint": {
			o:       drainOpts{s3Bucket: "garm-lake"},
			wantErr: []string{"--s3-endpoint"},
		},
		"lake dir": {
			o:        drainOpts{lakeDir: "./lake"},
			wantType: "*lake.DirStore",
		},
		"s3": {
			o:        drainOpts{s3Endpoint: "127.0.0.1:9000", s3Bucket: "garm-lake"},
			wantType: "*lake.Uploader",
		},
	} {
		t.Run(name, func(t *testing.T) {
			dest, err := destinationOf(tc.o)
			if tc.wantType == "" {
				if err == nil {
					t.Fatalf("accepted %+v and chose %T", tc.o, dest)
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal does not name %q: %v", want, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("%+v: %v", tc.o, err)
			}
			if got := fmt.Sprintf("%T", dest); got != tc.wantType {
				t.Fatalf("chose %s, want %s", got, tc.wantType)
			}
		})
	}
}

// The startup line says where the rows are going. An operator reading the
// first line of a log should not have to infer the destination from which
// flags they think they passed.
func TestTheStartupLineNamesTheDestination(t *testing.T) {
	for name, tc := range map[string]struct {
		o    drainOpts
		want []string
	}{
		"lake dir": {drainOpts{lakeDir: "./lake"}, []string{"dir", "lake"}},
		"s3":       {drainOpts{s3Endpoint: "s3.example:9000", s3Bucket: "garm-lake"}, []string{"garm-lake", "s3.example:9000"}},
	} {
		t.Run(name, func(t *testing.T) {
			dest, err := destinationOf(tc.o)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range tc.want {
				if !strings.Contains(dest.Describe(), want) {
					t.Errorf("the startup line %q does not name %q", dest.Describe(), want)
				}
			}
		})
	}
}

// --lake-dir needs no credentials. S3_ACCESS_KEY and S3_SECRET_KEY belong to
// the S3 destination alone: a lake on disk has nothing to authenticate to, and
// reading them for it would make an unset variable look relevant to a failure
// that has nothing to do with it.
func TestALakeDirDestinationIsBuiltWithoutS3Credentials(t *testing.T) {
	t.Setenv("S3_ACCESS_KEY", "")
	t.Setenv("S3_SECRET_KEY", "")
	dest, err := destinationOf(drainOpts{lakeDir: t.TempDir()})
	if err != nil {
		t.Fatalf("a lake on disk asked for credentials: %v", err)
	}
	if err := dest.Ensure(context.Background()); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
}
