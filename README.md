# sink

**The record streams, drained into a lake.** NATS JetStream in; micro-batched
ZSTD Parquet out — onto an S3-compatible object store, or onto a directory on
this machine — hive-partitioned so DuckDB, and later Iceberg or DuckLake, reads
it without being told anything.

Two streams, two policies. `GARM_LEDGER` carries metering; `GARM_AUDIT` carries
the records something is obliged to keep.

## This repository does not import garmd, and garmd does not import it

The daemon publishes to a stream. This drains it. The only thing they share is
the wire format — `garm.ledger.v1.Event` — and the subject names, both in
[`garm-ai/garm`](https://github.com/garm-ai/garm).

That is deliberate and CI asserts it. This binary links DuckDB, which is cgo
and tens of megabytes of static library, and an S3 client. Neither has any
business in the dependency graph of something that sits in a request path, and
the coupling would run the other way too: a lake's release cadence would become
the daemon's problem. It is the same data-not-imports inversion the catalogue
uses.

## Duplicates are the design

Delivery is at-least-once, so **the lake holds duplicate rows**. A flush that
lands in S3 and then fails to ack is redelivered and written again.

This is not an apology. The alternative to at-least-once is at-most-once, which
for a ledger means losing rows on exactly the failures anyone would want a row
for. So the sink never acks before the rows are durable, and the reader
deduplicates:

```sql
SELECT * FROM read_parquet('s3://garm-lake/ledger/**/*.parquet', hive_partitioning=true)
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY time) = 1;
```

`event_id` is assigned when an event is created, never at publish time, so a
retried batch carries the same ids it carried the first time. Every row in the
lake has one; a record without one never becomes a row.

## What cannot be read is not discarded

A message that will not parse goes to `GARM_SINK_DEAD` with the original bytes
and headers naming the stream, sequence and reason — and is terminated only
after that write succeeds.

The blast radius is one event, not one message. A ledger message holds hundreds
of events; one bad event in it yields 499 rows and one dead letter.

## Commands

```
sinkd provision          create or verify the three streams
sinkd drain ledger       drain GARM_LEDGER  (messages are garm.ledger.v1.Batch)
sinkd drain audit        drain GARM_AUDIT   (messages are garm.ledger.v1.Event)
sinkd tail ledger        print GARM_LEDGER rows as JSON lines, without consuming
sinkd tail audit         print GARM_AUDIT rows as JSON lines, without consuming
```

`drain` needs a destination and has no default for one: pass `--lake-dir
<path>`, or `--s3-endpoint` and `--s3-bucket`. Naming both is an error.

The binary was `garm-sink` before v0.3.0; the module path is unchanged, so
`go install github.com/garm-ai/sink/cmd/sinkd@v0.3.0` is the new spelling and
tags up to v0.2.0 keep `cmd/garm-sink`.

`provision` never patches. A stream that exists with a different policy is
reported and the command exits non-zero — an audit stream running `DiscardOld`
is data loss that still acks, and a deploy that silently flipped it back would
erase both the change and the evidence of who made it.

Any number of drains can run against one stream: they share a durable consumer,
and object names carry `<hostname>-<pid>` plus the stream-sequence range, so no
two instances can write the same key.

## On an object store

The S3 side reads the **AWS-standard variables** — the ones the AWS CLI, every
SDK, DuckDB's `httpfs` and the rest of this platform already read:

| variable | what it does |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | the access key; required unless `--s3-anonymous` |
| `AWS_SECRET_ACCESS_KEY` | the secret key |
| `AWS_SESSION_TOKEN` | the session token, for temporary credentials; empty for a long-lived key |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | the region, `AWS_REGION` first, as the SDKs resolve it |
| `AWS_ENDPOINT_URL` | the default for `--s3-endpoint`; a URL or a bare `host:port` |
| `S3_BUCKET` | the default for `--s3-bucket`. **This one is ours**: AWS addresses a bucket in the URL and defines no variable for it |

```
export AWS_ACCESS_KEY_ID=garmdev AWS_SECRET_ACCESS_KEY=garmdevsecret AWS_DEFAULT_REGION=us-east-1
sinkd drain ledger --s3-endpoint 127.0.0.1:28333 --s3-bucket garm-lake
```

**`S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_ENDPOINT` are not read any more.**
They were what this binary read before v0.5.0, and being the one thing on a
machine that wanted its own spelling cost an afternoon: with the AWS variables
exported and correct, the drain built an anonymous client and the store
answered `Access Denied`, which names neither a credential nor a variable. A
run started with only the old names now stops at startup and says which one to
export instead.

A drain with no credentials at all is refused for the same reason. A store with
no IAM configured is a real thing, and it is now a flag — `--s3-anonymous` —
rather than what an unset variable decays into. The first log line says which
of the two is in use:

```
"destination":"s3://garm-lake at 127.0.0.1:28333 (region us-east-1, signed with AWS_ACCESS_KEY_ID)"
```

## A lake on disk

The lake does not need an object store. `--lake-dir` writes the same objects to
a directory, which is what makes it usable beside a platform running from one
binary with an embedded NATS server and no SeaweedFS.

```
sinkd drain ledger --lake-dir ./lake
```

Query it where it lies:

```sql
SELECT * FROM read_parquet('./lake/ledger/**/*.parquet', hive_partitioning=true)
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY time) = 1;
```

**It is the same format.** The same ZSTD Parquet, hive-partitioned by
`(date, app)`, under the same keys — `ledger/date=2026-09-29/app=agentd/part-….parquet`
is a path here and an object key there, built by one piece of code for both. So
a directory can be copied into an object store later and read unchanged: put
`s3://garm-lake/` where `./lake/` was and every query keeps working.

A part is written under `<name>.parquet.incomplete` in its final directory and
renamed onto its name. The rename is atomic, so a reader globbing `*.parquet`
never opens a half-written file — and a crash leaves the temporary file rather
than a corrupt part. Sweep them with:

```
find ./lake -name '*.parquet.incomplete' -delete
```

`--lake-dir` needs no credentials. `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` are read on the S3 path and nowhere else, and the
drain's first log line says which destination is in use.

What a directory does not give you is in [KNOWN-GAPS](KNOWN-GAPS.md): no
lifecycle, no replication, and no compaction — that last one is true of both
destinations.

## Watching a stream

```
sinkd tail ledger                                  the last 100 messages, then exit
sinkd tail ledger --since 20 --filter outcome=denied
sinkd tail ledger --since 10m --follow             from ten minutes ago, and keep going
sinkd tail audit --from-start --pretty --no-detail
```

`tail` is the debugging eye: one JSON object per event, keys named and
ordered exactly as the lake's columns, so a filter that works on a terminal
line is a `WHERE` clause that works on the Parquet. The mapping is one list
(`internal/row.Columns`) that the Parquet writer and `tail` both read, so the
two cannot drift.

It **never consumes**. The consumer it creates is ephemeral, has no durable
name, and is `AckNone` at the broker — there is no ack for it to get wrong —
and it is deleted on exit. A drain running against the same stream keeps its
durable consumer, its ack floor and its pending count exactly as they were;
the test for that runs against a real embedded broker with a durable that has
messages half-acked. It is safe to point at a live plane.

`--since` takes a message count back from the end (`20`) or a duration back
from now (`10m`); the default is the last 100 messages. On the ledger a
message is a `Batch`, so "20 messages" may be many more events. `--from-start`
begins at the oldest message still in the stream. Without `--follow` it stops
at the end of the stream as it was when it started.

`--filter column=value` is repeatable and every filter must match. It is
applied client-side after decoding — the stream's subjects only carry tenant
and app, and nothing is indexed on `run_id` or `outcome`. An unknown column
is an error that lists the real ones, not an empty result.

Rows are printed as they are on the stream — `tail` redacts nothing, because
the rows already carry only what garmd or agentd put in them. The exception
worth knowing about is `error_detail`, the one column that may carry
unsanitised free text (a resolver error routinely interpolates the value it
was protecting). `--no-detail` drops the column. A terminal is the least
governed place a row can land; use it when the output is going anywhere but
your own eyes.

A message that will not decode is reported on stderr with its sequence and
skipped; it is a drain's dead letter, not a tail's problem.

## Batch sizing

250,000 rows or five minutes, whichever comes first. Below roughly 100k rows a
Parquet file is mostly footer and column statistics, every query pays a `LIST`
over thousands of keys before it reads anything, and object stores bill per
request. The cost is that up to five minutes of records sit unacked in
JetStream — not lost, but the stream has to be sized to hold them.

## Working here

```
mise install     the toolchain
mise run test    go test ./... -race
mise run ci      what CI runs
```

Task names match every other garm-ai repository.

MIT licensed.
