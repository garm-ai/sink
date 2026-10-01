package lake

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/garm-ai/sink/internal/drain"
)

// Destination is where a flush lands: an S3-compatible store, or a directory
// on this machine.
//
// There are two of them because a developer running the platform from one
// binary has no object store, and a lake they cannot produce locally is a lake
// they only meet in production. The FORMAT is not what varies — both
// destinations receive the same ZSTD Parquet under the same hive-partitioned
// keys, written by the same `COPY` — only where the bytes come to rest. A
// directory written by --lake-dir can be copied into a bucket later and read
// without changing a query.
type Destination interface {
	// Ensure makes the destination usable before the first flush: the bucket
	// exists, or the directory does.
	Ensure(ctx context.Context) error

	// Publish lands every Parquet file under localDir at its key and returns
	// the keys. It is all-or-nothing from the caller's point of view: an
	// error means the whole batch is naked and redelivered.
	Publish(ctx context.Context, localDir, keyPrefix, instanceID string, firstSeq, lastSeq uint64) ([]string, error)

	// Describe names the destination for the startup line, so an operator can
	// see from the first log line where the rows are going.
	Describe() string
}

// partName is the object's base name: instance-unique, and carrying the
// stream-sequence range so an object can be traced back to the messages it
// came from without opening it.
//
// Two sinks share one work queue with no leader election, which only works
// because neither can produce the other's name.
func partName(instanceID string, firstSeq, lastSeq uint64, n int) string {
	return fmt.Sprintf("part-%s-%d-%d-%d.parquet", instanceID, firstSeq, lastSeq, n)
}

// walkParts walks the hive tree WriteParquet produced and calls land with each
// Parquet file's local path and the key it must arrive at.
//
// Both destinations go through this, so the key an object gets on S3 and the
// path it gets under --lake-dir are the same string by construction rather
// than by two implementations agreeing. That is the whole promise of the local
// destination: copy the directory into a bucket and the queries do not change.
//
// Anything that is not a .parquet file is left where it is. DuckDB's COPY can
// leave other entries in the tree, and publishing them would put files in the
// lake that a reader's glob would then try to parse.
func walkParts(localDir, keyPrefix, instanceID string, firstSeq, lastSeq uint64,
	land func(localPath, key string) error) ([]string, error) {
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
		key := path.Join(keyPrefix, filepath.ToSlash(filepath.Dir(rel)), partName(instanceID, firstSeq, lastSeq, n))
		n++
		if err := land(p, key); err != nil {
			return err
		}
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

// ParquetSink is the production drain.Sink: DuckDB writes the hive-partitioned
// Parquet into a scratch directory, the destination takes the tree, and the
// scratch directory is removed.
//
// Flush is all-or-nothing from the drain's point of view. A partial publish
// returns an error, the whole batch is naked and redelivered, and the objects
// that did land are written again under a new name — duplicate ROWS, which the
// lake is designed to hold, rather than a gap, which it is not.
//
// The scratch directory is an OS temp directory for both destinations, so the
// DuckDB statement is identical whichever one is configured. Only what happens
// to the finished files afterwards differs: a PUT, or a rename.
type ParquetSink struct {
	Dest       Destination
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
	_, err = s.Dest.Publish(ctx, dir, s.KeyPrefix, s.InstanceID, b.FirstSeq, b.LastSeq)
	return err
}
