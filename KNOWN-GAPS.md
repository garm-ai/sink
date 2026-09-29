# Known gaps

What this repository does not check, and what it does on purpose that will
surprise you — most of it about the lake, which outlives every release that
wrote into it. Not an inventory of what works: the code says that, and a file
that repeats it goes stale in a way the code cannot.

## What the contract-drift test does not cover

`internal/row/contract_test.go` walks `garm.ledger.v1.Event`, `Usage` and
`Batch` and fails when a field is neither a lake column nor on a named ignore
list with a reason beside it — the check `execution_subject` needed and did not
have for the nine releases it sat on the wire unread. It checks NAMES, in both
directions, and nothing else. A field whose type or cardinality changed under
the same name passes: the DuckDB type map in `internal/lake` is a separate
list, and a `string` that became `repeated string` would flatten differently
and still be spelled the same. Field numbers are unchecked except
`execution_subject`'s, pinned at 71 because reusing 70 is the mistake the
contract's own comment exists to prevent. And it proves a column EXISTS, not
that a value reaches it — the plumbing runs through the contract's shared
`ledger.FromProto`, so there is one place to get it right rather than two, but
no test here reads that place.

## What the tests do not reach

**No live object store.** `Uploader` is tested against an `httptest` server
speaking enough S3 for HEAD-bucket, PUT-bucket and PUT-object — enough to cover
what this repository decides: object keys, the hive path, which files are
uploaded, that a refusal fails the whole flush, and (since v0.5.0) that the
configuration reaches the wire, since SigV4 puts the access key and region in
the `Authorization` credential scope. It does not cover multipart uploads
(nothing here triggers one), minio's retry behaviour, or any real store's
quirks. Those want a MinIO or SeaweedFS in a compose-based job.

**No end-to-end run from a NATS message to an OBJECT.** `--lake-dir` has one —
a message on the embedded broker, through the drain, into a temporary
directory, read back with `read_parquet` over the hive glob. The S3 half does
not: the PUT is the single step no test exercises.

**Producer/consumer agreement is untested.** That a real garmd publishes what
this decodes. The faithful version needs the dependency CI exists to refuse, so
it belongs in a cross-repository test where both sides exist — not in a fake
here, and not in a skip.

**`runDrain` and `runTail` themselves are untested.** The flag surface,
`ackWait`, `destinationOf` and the tail flag set are; the two functions that
wire a NATS connection to a signal handler are not, and what is worth testing
inside them is tested where it lives. The other holes are the error paths that
need a failing disk, a failing store, a closed stdout, or a `--since <duration>`
against a live broker.

## Not built

**A lake on disk has no lifecycle and no replication.** `--lake-dir` gives the
format and the keys, not the operational surface: no expiry, no tiering, no
versioning, no server-side encryption, no second copy. A directory is exactly
as durable as the disk it is on. It is a development destination, which is why
the two produce identical trees.

**The rename is atomic; the directory entry is not fsynced.** A part is written
to `<name>.parquet.incomplete`, fsynced, and renamed onto its key, so a reader
never sees a prefix of a file. The parent directory is not fsynced afterwards,
so a power loss between the rename and the filesystem's own flush can lose a
part whose messages the drain has already acked. On S3 the PUT is durable before
the ack. Closing it is one `fsync` per flush and a test that needs a crashing
machine.

**Nothing sweeps `*.parquet.incomplete`.** A crash mid-flush leaves one. No
query will read it — the suffix exists so `**/*.parquet` does not match — but
nothing deletes it either. The README gives the `find`; a startup sweep would
be three lines and is not there.

**Compaction.** Five-minute files at low volume are small files, on both
destinations. Nothing rewrites a day's partition into a few large files. Better
than 2,880 files per day per app, not as good as one.

**Deduplication.** The lake holds duplicate rows by design and the reader
deduplicates on `event_id`. Nothing writes a deduplicated view, a manifest, or
an Iceberg table. The README's `QUALIFY` clause is the whole story.

**Retention and the `error_detail` column.** It may carry unsanitized free text
and is documented as inheriting the access grade of the most sensitive field in
the registry that produced it. Nothing here enforces that: in the Parquet file
it is a column like any other. Splitting it into a separately-governed table,
or encrypting it, is not built.

**Nothing drains the dead-letter stream.** Records land in `GARM_SINK_DEAD` and
stay. There is no command to list them, re-drive them after a publisher is
fixed, or alert on the stream filling — and it is `DiscardNew`, so a full stream
fails the sink's own dead-letter writes, which naks every batch holding a bad
record. The safe direction, and a loud one, but a cliff rather than a slope.

**`tail` redacts nothing.** It prints rows as they are on the stream.
`--no-detail` drops `error_detail`; there is no column allow-list, no per-tenant
view and no audit of who tailed what. It is a debugging eye for someone already
entitled to read the stream, and NATS credentials are the only thing checking
that they are.

**Only the environment variables, not the whole AWS credential chain.** The S3
side reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_REGION`/`AWS_DEFAULT_REGION` and `AWS_ENDPOINT_URL`. It does not read
`~/.aws/credentials`, `AWS_PROFILE`, a web-identity token file, or the instance
metadata service, so a drain under an EC2 or EKS role with nothing exported is
refused rather than picking the role up. minio-go ships the chain; wiring it in
costs the property v0.5.0 was about — that "no credentials" is decidable at
startup. A deliberate order of work.

**No metrics.** Rows per flush, flush latency, dead letters by reason and
consumer lag are a log line at best.

**`provision` cannot fix anything.** It reports a policy mismatch and exits
non-zero, deliberately, but there is no `--force` and no guided path from "the
audit stream is wrong" to a corrected stream. That is `nats stream edit` and a
human.

**sink does not need DuckDB to write.** Writing Parquet is the drain's job and
reading it is the user's, so nothing here has to link a query engine: a pure-Go
writer (`parquet-go`) would drop cgo and tens of megabytes of static library,
shrink the binary, speed the build, and let `garmstack` embed sink rather than
ask a developer to run it beside. The risk is schema fidelity — DuckDB's `COPY`
decides the Parquet types today, and the timestamps, nullable strings, derived
`date` partition, ZSTD settings and row-group statistics would all have to come
out indistinguishable. That is a test matrix against the existing writer, not a
rewrite.
