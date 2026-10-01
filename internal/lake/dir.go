package lake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// IncompleteSuffix is appended to a part file while its bytes are still being
// written. It is the reason a reader can glob a lake on disk at any moment and
// never open a half-written file: `**/*.parquet` does not match a name ending
// in `.parquet.incomplete`, and the rename that removes the suffix is atomic.
//
// It is also what a crash leaves behind. A torn write is a leftover file, not
// a corrupt part, and the sweep is one command:
//
//	find <lake-dir> -name '*.parquet.incomplete' -delete
const IncompleteSuffix = ".incomplete"

// DirStore is the Destination that writes the lake to a directory on this
// machine: the same keys an object store would receive, as paths under a root.
//
// It exists so that a developer running the platform from one binary — an
// embedded NATS server and no SeaweedFS — still gets a lake they can query.
// Nothing about the FORMAT changes, so the directory can be copied into a
// bucket later and read by exactly the queries that were written against it.
//
// It takes no credentials, because there is nothing to authenticate to: the
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY reading belongs to the S3
// destination alone.
type DirStore struct {
	root string
}

// NewDirStore roots a lake at dir. The path is made absolute here rather than
// at every use, so a relative --lake-dir stays pointing where the operator
// typed it even if something later changes the working directory.
func NewDirStore(dir string) (*DirStore, error) {
	if dir == "" {
		return nil, errors.New("lake: --lake-dir needs a path")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("lake dir %q: %w", dir, err)
	}
	return &DirStore{root: abs}, nil
}

// Root is the absolute directory the keys hang under.
func (d *DirStore) Root() string { return d.root }

func (d *DirStore) Describe() string { return "dir " + d.root }

// Ensure creates the root directory. It is the counterpart of creating the
// bucket, and it happens at startup for the same reason: a destination that
// cannot be written to should fail before the first message is fetched, not
// five minutes later with a batch already unacked.
func (d *DirStore) Ensure(_ context.Context) error {
	if err := os.MkdirAll(d.root, 0o755); err != nil {
		return fmt.Errorf("lake dir %s: %w", d.root, err)
	}
	return nil
}

// Publish moves the hive tree WriteParquet produced under the root, one file
// per key.
//
// Each part is copied to `<key>.incomplete` in its final directory, fsynced,
// and then renamed onto the key. The rename is atomic, so a reader globbing
// the tree sees either no file at that path or the whole file — never the
// prefix of one. That is what S3 gives for free and a filesystem does not.
//
// The copy is a copy rather than a rename from the scratch directory because
// the scratch directory is an OS temp directory, which may be a different
// filesystem; a rename across one is not atomic and on most systems is not
// even permitted.
func (d *DirStore) Publish(ctx context.Context, localDir, keyPrefix, instanceID string, firstSeq, lastSeq uint64) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return walkParts(localDir, keyPrefix, instanceID, firstSeq, lastSeq, func(p, key string) error {
		final := filepath.Join(d.root, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
			return fmt.Errorf("lake dir %s: %w", filepath.Dir(final), err)
		}
		if err := copyThenRename(p, final); err != nil {
			return fmt.Errorf("place %s: %w", key, err)
		}
		return nil
	})
}

// copyThenRename writes src to dst+IncompleteSuffix, flushes it to the disk,
// and renames it onto dst.
//
// The fsync is not decoration. Without it the rename can be durable while the
// bytes are not, and the drain has already acked the messages those bytes came
// from: the lake would hold a file of zeros where JetStream has nothing left
// to redeliver.
func copyThenRename(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	tmp := dst + IncompleteSuffix
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	// Two cleanups, because the happy path renames the file out from under
	// this one: a failed Remove of a name that is no longer there is not an
	// error worth reporting, and a failed copy must not leave the partial
	// file behind for a sweep to find when we can remove it now.
	defer func() {
		out.Close()
		os.Remove(tmp)
	}()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
