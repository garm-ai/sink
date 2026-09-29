# garm-sink

**The record streams, drained into a lake.** NATS JetStream in; micro-batched
ZSTD Parquet on an S3-compatible object store out, hive-partitioned so DuckDB —
and later Iceberg or DuckLake — reads it without being told anything.

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
garm-sink provision          create or verify the three streams
garm-sink drain ledger       drain GARM_LEDGER  (messages are garm.ledger.v1.Batch)
garm-sink drain audit        drain GARM_AUDIT   (messages are garm.ledger.v1.Event)
garm-sink tail ledger        print GARM_LEDGER rows as JSON lines, without consuming
garm-sink tail audit         print GARM_AUDIT rows as JSON lines, without consuming
```

`provision` never patches. A stream that exists with a different policy is
reported and the command exits non-zero — an audit stream running `DiscardOld`
is data loss that still acks, and a deploy that silently flipped it back would
erase both the change and the evidence of who made it.

Any number of drains can run against one stream: they share a durable consumer,
and object names carry `<hostname>-<pid>` plus the stream-sequence range, so no
two instances can write the same key.

## Watching a stream

```
garm-sink tail ledger                                  the last 100 messages, then exit
garm-sink tail ledger --since 20 --filter outcome=denied
garm-sink tail ledger --since 10m --follow             from ten minutes ago, and keep going
garm-sink tail audit --from-start --pretty --no-detail
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
