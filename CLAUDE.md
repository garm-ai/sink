# garm-sink — the record streams, drained into a lake

JetStream → micro-batch → ZSTD Parquet → S3-compatible object store. One
binary, `garm-sink`, with three verbs: `provision`, `drain ledger`,
`drain audit`.

## The invariant

**This repository has no import relationship with `garm-ai/garmd`, in either
direction.** garmd publishes; this consumes. The contract is the wire format
(`garm.ledger.v1.Event`) and the subject names in `garm/contracts/wire` — never
a Go import. CI asserts it, and CI also asserts that the one allowed edge, to
`garm-ai/garm`, reaches its contracts and nothing else.

It matters because of what is linked here: DuckDB, which is cgo, and minio.
Neither belongs in a request path's dependency graph. Measured in the monorepo,
folding this into the sidecar took it from 22.5 MB to 66.8 MB.

## Layout

```
cmd/garm-sink/          the binary: provision, drain ledger, drain audit
internal/
  row/                  wire message -> Parquet rows, and what cannot become one
  drain/                the consume loop and the ack discipline; dead letters
  lake/                 DuckDB writes the Parquet, minio puts it
  streams/              the two streams' configurations, and provisioning
  cli/                  the cobra conventions shared with the other repositories
```

**Everything starts `internal/`.** Promoting a package later is easy where
demoting one is breaking.

## The four things that are easy to get wrong

**1. The batch limit counts rows, not messages.** One ledger message is a
`garm.ledger.v1.Batch` holding hundreds of events. A limit of 1000 messages is
a Parquet file of a million rows.

**2. Nothing unreadable is discarded.** A parse failure goes to `GARM_SINK_DEAD`
with its original bytes, and the source message is terminated only after that
write succeeds. The previous implementation called `Term()` and logged a line:
right for metering, and for audit it permanently discards a record something
was obliged to keep.

**3. The two streams have opposite discard policies.** The audit stream is
`DiscardNew`, so a full stream makes the publisher's write FAIL and a
fail_closed tool refuses to run. The ledger is `DiscardOld`, because a metering
publisher must never block a request. `provision` refuses to patch, ever.

**4. AckWait has to outlast a whole cycle.** BatchInterval plus FlushTimeout
plus slack. Shorter and JetStream redelivers mid-flush, two instances write the
same rows continuously, and the duplicate story stops being about failures.

## Testing

`mise run test` — `-race`, `-count=1`, a timeout. Always that, never bare
`go test`.

The NATS tests run a **real embedded server** (`Port: -1`, `NoLog`, `NoSigs`,
then `ReadyForConnections`; never 4222). Ack, nak, term and redelivery are the
broker's behaviour, and a fake that agreed with our beliefs about them would be
worth nothing.

**No test may skip when a dependency is absent.** A file whose tests all skip
reports `ok` while covering nothing. If something genuinely cannot be tested
here, it goes in KNOWN-GAPS with the reason — not behind a `t.Skip`.

## The design record is not in this repository

It lives in **[`garm-ai/spec`](https://github.com/garm-ai/spec)** (private).
**Do not create `docs/superpowers/` here.**
