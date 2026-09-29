# sink — the record streams, drained into a lake

JetStream → micro-batch → ZSTD Parquet → an S3-compatible object store, or a
directory on this machine. One binary, `sinkd`, with three verbs: `provision`,
`drain ledger`, `drain audit` — and a fourth that only reads: `tail ledger`,
`tail audit`.

## The invariant

**This repository has no import relationship with `garm-ai/garmd`, in either
direction.** garmd publishes; this consumes. The contract is the wire format
(`garm.ledger.v1.Event`) and the subject names in `github.com/garm-ai/contracts`
— never a Go import. CI asserts it, and CI also asserts that the contract module
is the ONLY garm-ai edge: `garm-ai/garm` is now the command line tool and nothing
here may link it.

The contract used to live inside `garm-ai/garm` as `contracts/…`; it was split
out and flattened to the new module's root at contracts v0.2.0, so
`garm/contracts/wire` is `contracts/wire` and `garm/contracts/garm/ledger/v1` is
`contracts/garm/ledger/v1`. Between the old pin here (garm v0.8.0) and the new
module the ledger changed in exactly one way: `execution_subject`, field 71,
added at garm v0.14.0. It became a Parquet column at v0.7.0 — see README,
"`execution_subject`, NULL and empty", for what `NULL` means either side of
that boundary.

It matters because of what is linked here: DuckDB, which is cgo, and minio.
Neither belongs in a request path's dependency graph. Measured in the monorepo,
folding this into the sidecar took it from 22.5 MB to 66.8 MB.

## Layout

```
cmd/sinkd/              the binary: provision, drain ledger|audit, tail ledger|audit
internal/
  row/                  wire message -> Parquet rows, and what cannot become one;
                        row.Columns is THE column list, shared by lake and tail;
                        contract_test.go fails when the descriptor outgrows it
  drain/                the consume loop and the ack discipline; dead letters
  tail/                 the debugging eye: ephemeral AckNone consumer -> JSON lines
  lake/                 DuckDB writes the Parquet; a Destination lands it —
                        minio on S3, or DirStore under --lake-dir
  streams/              the two streams' configurations, and provisioning
  cli/                  the cobra conventions shared with the other repositories
```

**Everything starts `internal/`.** Promoting a package later is easy where
demoting one is breaking.

## The eight things that are easy to get wrong

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

**5. `tail` never acks and never creates a durable.** It is pointed at live
planes. Its consumer is ephemeral, `AckNone`, and deleted on exit, and a test
with a half-acked durable on the same stream asserts the durable did not
move. A `tail` that acked would be a drain that loses rows.

**6. The two destinations produce the same keys, and that is load-bearing.**
`--lake-dir` exists so the lake works with no object store; its whole value is
that a directory can be copied into a bucket and read unchanged. Both
destinations therefore build their keys in `walkParts` and nowhere else. On
disk a part is written as `<name>.parquet.incomplete` and renamed onto its
name: the rename is atomic, and the suffix is a suffix rather than a hidden
staging directory because DuckDB's `**` matches dot-directories — hiding the
staging area would not have hidden it from a query.

**7. The S3 side reads the AWS-standard variable names, and only those.**
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_REGION`/`AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL` — the ones the AWS CLI,
the SDKs, DuckDB's `httpfs` and this platform's compose already set. Only the
bucket is ours (`S3_BUCKET`), because AWS addresses a bucket in the URL and has
no variable for one. `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_ENDPOINT` were
read until v0.5.0 and are now refused by name, because reading a private
spelling turns a correctly configured machine into `Access Denied` from the
store — which names neither a credential nor a variable. For the same reason
there is no silent fallback to unsigned requests: a store with no IAM is
`--s3-anonymous`, and the startup line says which of the two is in use.

**8. A field added to the contract does not become a column by itself, and a
test is the only thing that says so.** `row.Columns` is an explicit list rather
than a walk over the descriptor, deliberately: the lake's names, order and
types are ours, not the proto's. The cost was that nothing failed when the
contract grew a field with no entry in the list — `execution_subject` was on
the wire, set by garmd, for nine releases before anyone noticed.
`internal/row/contract_test.go` closes that class: it walks
`garm.ledger.v1.Event`, `Usage` and `Batch` and requires every field to be a
column or on a named ignore list **with a reason beside it**, in both
directions, so a column with no field behind it fails as well. If it fails on a
field you meant to leave out, write the reason into the ignore list — an entry
without one cannot be told from an oversight. `Event`'s ignore list is empty
today, and `error_detail` is the field that was considered for it and kept as a
column: destroying the detail is what the contract warns against, and what it
actually asks for is a governance grade this repository does not yet provide
(KNOWN-GAPS).

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
