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
```

`provision` never patches. A stream that exists with a different policy is
reported and the command exits non-zero — an audit stream running `DiscardOld`
is data loss that still acks, and a deploy that silently flipped it back would
erase both the change and the evidence of who made it.

Any number of drains can run against one stream: they share a durable consumer,
and object names carry `<hostname>-<pid>` plus the stream-sequence range, so no
two instances can write the same key.

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
