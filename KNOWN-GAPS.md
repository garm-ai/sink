# Known gaps

## Built

- `internal/row` — `garm.ledger.v1.Batch` and `Event` to Parquet rows, and the
  rejects that never become one. Field mapping goes through the contract's own
  `ledger.FromProto`, so a field added there is forgotten in one place rather
  than two.
- `internal/drain` — the consume loop. Ack after a successful flush, nak for
  redelivery, dead-letter then terminate, and a shutdown flush on a context
  derived with `WithoutCancel`.
- `internal/lake` — DuckDB writes hive-partitioned ZSTD Parquet, minio puts the
  tree. Instance-unique object names, so any number of drains share one work
  queue with no leader election.
- `internal/streams` — the two streams' configurations and `provision`, which
  creates what is missing and refuses what exists with the wrong policy.
- `internal/tail` — `garm-sink tail ledger|audit`: an ephemeral `AckNone`
  consumer, deleted on exit, printing one JSON object per event under the
  lake's column names (`row.Columns`, the one list the Parquet writer also
  builds from). `--since` (count or duration), `--from-start`, `--follow`,
  repeatable client-side `--filter column=value`, `--pretty`, `--no-detail`.
  Tested against the embedded broker, including that a half-acked durable on
  the same stream does not move.
- `cmd/garm-sink` — `provision`, `drain ledger`, `drain audit`, `tail ledger`,
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

**No end-to-end run from a NATS message to an object.** `ParquetSink.Flush` is
tested against real DuckDB and the fake store, and the drain loop is tested
against a real broker with a fake sink, but nothing runs all three at once.

**Producer/consumer agreement is untested.** That a real garmd publishes what
this decodes. The faithful version needs the dependency CI exists to refuse, so
it belongs in a cross-repository test where both sides exist — not in a fake
here, and not in a skip.

## Not built

**Compaction.** Five-minute files at low volume are still small files. A lake
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

**No metrics.** Rows per flush, flush latency, dead letters by reason and
consumer lag are all things an operator will want, and all of them are
currently a log line at best.

**`provision` cannot fix anything.** It reports a policy mismatch and exits
non-zero, deliberately, but there is no `--force` and no guided path from "the
audit stream is wrong" to a corrected stream. Today that is `nats stream edit`
and a human.

## Coverage

```
internal/cli       100%
internal/streams    94%
internal/drain      92%
internal/row        90%
internal/lake       85%
internal/tail       84%
cmd/garm-sink       48%
```

`cmd/garm-sink` is flag wiring around a NATS connection and a signal handler:
the flag surface, `ackWait` and the tail flag set are tested, `runDrain` and
`runTail` themselves are not, and what is worth testing in them is tested
where it lives. `internal/tail`'s remainder is the `--since <duration>` start
against a live broker (the config mapping is tested; the timing is not) and
the error paths of writing to a closed stdout. `internal/row`'s remainder is
the defensive half of its error paths — a `json.Marshal` of a `map[string]string`
that fails, a `proto.Marshal` of a message that just unmarshalled.

Two of the load-bearing tests were verified by mutation: changing the batch
limit back to counting messages, and moving `Term()` ahead of the dead-letter
write. Both failed with the message they were written to produce.
