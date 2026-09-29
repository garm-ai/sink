# Known gaps

## Built

- `internal/row` — `garm.ledger.v1.Batch` and `Event` to Parquet rows, and the
  rejects that never become one. Field mapping goes through the contract's own
  `ledger.FromProto`, so a field added there is forgotten in one place rather
  than two.
- `internal/drain` — the consume loop. Ack after a successful flush, nak for
  redelivery, dead-letter then terminate, and a shutdown flush on a context
  derived with `WithoutCancel`.
- `internal/lake` — DuckDB writes hive-partitioned ZSTD Parquet and a
  `Destination` lands it: minio puts the tree on an S3-compatible store —
  configured by the AWS-standard variable names, and refusing a configuration
  it cannot sign rather than sending unsigned requests — or
  `DirStore` renames it into a directory on this machine (`--lake-dir`). Both
  build their keys from one walk, so the path on disk and the object key are
  the same string by construction. Instance-unique names, so any number of
  drains share one work queue with no leader election.
- `internal/streams` — the two streams' configurations and `provision`, which
  creates what is missing and refuses what exists with the wrong policy.
- `internal/tail` — `sinkd tail ledger|audit`: an ephemeral `AckNone`
  consumer, deleted on exit, printing one JSON object per event under the
  lake's column names (`row.Columns`, the one list the Parquet writer also
  builds from). `--since` (count or duration), `--from-start`, `--follow`,
  repeatable client-side `--filter column=value`, `--pretty`, `--no-detail`.
  Tested against the embedded broker, including that a half-acked durable on
  the same stream does not move.
- `cmd/sinkd` — `provision`, `drain ledger`, `drain audit`, `tail ledger`,
  `tail audit`.

## Not covered by tests, and why

**No live object store.** `Uploader` is tested against an `httptest` server
speaking enough S3 for HEAD-bucket, PUT-bucket and PUT-object. That covers what
this repository actually decides — object keys, the hive path, which files are
uploaded, and that a refusal fails the whole flush. It does not cover multipart
uploads (nothing here triggers one: `FPutObject` on a file under the threshold
is a single PUT), minio's retry behaviour, or the quirks of any particular
store. Those need a real MinIO or SeaweedFS, which belongs in a compose-based
integration job rather than in `go test`.

What that server does cover, since v0.5.0, is the configuration reaching the
wire: SigV4 puts the access key and the region in the credential scope of the
`Authorization` header, so the test asserts what the store was sent rather than
what the struct was filled with. It was the absence of exactly that assertion
that let `S3_ACCESS_KEY` go on being the one spelling nothing else on the
machine used.

**No end-to-end run from a NATS message to an OBJECT.** The local destination
now has one — a message on the embedded broker, through the drain, into a
temporary directory, read back with `read_parquet` over the hive glob — because
`--lake-dir` needs nothing stood up for it. The S3 half still does not: the PUT
is the only step that test does not exercise, and standing up a real store for
it belongs in a compose-based integration job.

**Producer/consumer agreement is untested.** That a real garmd publishes what
this decodes. The faithful version needs the dependency CI exists to refuse, so
it belongs in a cross-repository test where both sides exist — not in a fake
here, and not in a skip.

## Not built

**A lake on disk has no lifecycle and no replication.** `--lake-dir` gives the
format and the keys, not the operational surface around them. There is no
expiry, no tiering, no versioning, no server-side encryption and no second
copy: a directory is exactly as durable as the disk it is on. It is a
development destination, and a production lake still wants an object store —
which is why the two produce identical trees.

**The rename is atomic; the directory entry is not fsynced.** A part is written
to `<name>.parquet.incomplete`, fsynced, and renamed onto its key, so a reader
never sees a prefix of a file. The parent directory is not fsynced afterwards,
so a power loss in the window between the rename and the filesystem's own
flush can lose a part whose messages the drain has already acked. On S3 the PUT
is durable before the ack. Closing it is one `fsync` on the directory handle
per flush and a test that cannot be written without a crashing machine, so it
is written down rather than half-done.

**Nothing sweeps `*.parquet.incomplete`.** A crash mid-flush leaves one, and no
query will ever read it — the suffix is there so a reader's `**/*.parquet` does
not match it — but nothing deletes it either. The README gives the `find`
command; a drain that swept its own leftovers at startup would be three lines
and is not there.

**Compaction.** Five-minute files at low volume are still small files. This is
true of both destinations: neither the bucket nor the directory gets a job that
rewrites a day's partition. A lake
wants a periodic job that rewrites a day's partition into a few large files and
deletes the parts. Nothing here does that, and until it exists the small-file
problem is deferred rather than solved: better than 2,880 files per day per app,
not as good as one.

**Deduplication.** The lake holds duplicate rows by design and the reader is
expected to deduplicate on `event_id`. Nothing writes a deduplicated view, a
manifest, or an Iceberg table. The README shows the `QUALIFY` clause; that is
currently the whole story.

**Retention and the `error_detail` column.** `error_detail` may carry
unsanitized free text and is documented as inheriting the access grade of the
most sensitive field in the registry that produced it. Nothing here enforces
that: it is a column like any other in the Parquet file. Splitting it into a
separately-governed table, or encrypting it, is not built.

**Nothing drains the dead-letter stream.** Records land in `GARM_SINK_DEAD` and
stay there. There is no command to list them, re-drive them after a publisher
is fixed, or alert on the stream filling — and it is `DiscardNew`, so when it
fills the sink's own dead-letter writes start failing, which naks every batch
containing a bad record. That is the safe direction and a loud one, but it is a
cliff rather than a slope.

**`tail` redacts nothing.** It prints rows as they are on the stream, and
`error_detail` may carry unsanitised free text. `--no-detail` drops that
column; there is no allow-list of columns, no per-tenant view and no audit of
who tailed what. It is a debugging eye for someone already entitled to read
the stream, and nothing here checks that they are — NATS credentials do.

**Only the environment variables, not the whole AWS credential chain.** The S3
side reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_REGION`/`AWS_DEFAULT_REGION` and `AWS_ENDPOINT_URL`. It does not read
`~/.aws/credentials`, `AWS_PROFILE`, a web-identity token file, or the instance
metadata service, so a drain running under an EC2 or EKS role with no variables
exported is refused rather than picking the role up. minio-go ships the chain
(`credentials.NewChainCredentials`) and wiring it in is small; what it costs is
that "no credentials" stops being a decidable state at startup, which is the
property this release was about. It is a deliberate order of work, not an
oversight: variables first, because that is what the platform sets.

**No metrics.** Rows per flush, flush latency, dead letters by reason and
consumer lag are all things an operator will want, and all of them are
currently a log line at best.

**`provision` cannot fix anything.** It reports a policy mismatch and exits
non-zero, deliberately, but there is no `--force` and no guided path from "the
audit stream is wrong" to a corrected stream. Today that is `nats stream edit`
and a human.

## Candidates for the next release

**sink does not need DuckDB to write.** Writing Parquet is the drain's job and
reading it is the user's, so nothing here has to link a query engine: a pure-Go
writer (`parquet-go`) would drop the cgo dependency and the tens of megabytes
of static library, shrink the binary, speed the build, and let `garmstack`
embed sink rather than ask a developer to run it beside. The risk is schema
fidelity: DuckDB's `COPY` decides the Parquet types today, and every column in
`internal/row` — the timestamps, the nullable strings, the derived `date`
partition, the ZSTD settings and the row-group statistics — would have to come
out byte-compatible enough that a reader cannot tell which writer produced a
file. That is a test matrix against the existing writer, not a rewrite.

## Coverage

```
internal/cli       100%
internal/streams    94%
internal/drain      92%
internal/row        90%
internal/lake       87%
internal/tail       84%
cmd/sinkd           55%
```

`cmd/sinkd` is flag wiring around a NATS connection and a signal handler:
the flag surface, `ackWait`, `destinationOf` and the tail flag set are tested,
`runDrain` and `runTail` themselves are not, and what is worth testing in them
is tested where it lives. `internal/tail`'s remainder is the `--since <duration>` start
against a live broker (the config mapping is tested; the timing is not) and
the error paths of writing to a closed stdout. `internal/lake`'s remainder is the error paths of the two destinations that
need a failing disk or a failing store to reach — a short read while copying a
part, an `fsync` that refuses. `internal/row`'s remainder is
the defensive half of its error paths — a `json.Marshal` of a `map[string]string`
that fails, a `proto.Marshal` of a message that just unmarshalled.

Two of the load-bearing tests were verified by mutation: changing the batch
limit back to counting messages, and moving `Term()` ahead of the dead-letter
write. Both failed with the message they were written to produce.
