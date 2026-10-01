package drain_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/garm-ai/contracts/garm/ledger/v1"
	"github.com/garm-ai/contracts/wire"
	"github.com/garm-ai/sink/internal/drain"
	"github.com/garm-ai/sink/internal/lake"
	"github.com/garm-ai/sink/internal/row"
	"github.com/garm-ai/sink/internal/streams"
)

// --- harness -----------------------------------------------------------

// embedded runs a real NATS server in this process, on a port the OS picks.
//
// A real broker and not a fake, because everything this package is about —
// ack, nak, term, redelivery after AckWait — is the broker's behaviour and not
// something a stand-in can be wrong about convincingly. Port -1 rather than
// 4222: a test that binds the default port fails when a developer has NATS
// running, and passes by talking to their data.
func embedded(t *testing.T) jetstream.JetStream {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
		// The real stream configurations declare multi-gigabyte ceilings, and
		// an embedded server sizes its account limit from the temp
		// filesystem. Without this the streams under test cannot be created
		// at all, which would fail as "insufficient storage" rather than as
		// anything to do with the code.
		JetStreamMaxStore: 64 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func ctx5(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// ledgerFixture provisions the real stream configurations and a consumer with
// a short AckWait, so a redelivery test does not take ten minutes.
func ledgerFixture(t *testing.T) (jetstream.JetStream, jetstream.Consumer) {
	t.Helper()
	js := embedded(t)
	ctx := ctx5(t)
	for _, cfg := range []jetstream.StreamConfig{
		streams.LedgerConfig(streams.Sizing{}), streams.DeadConfig(streams.Sizing{}),
	} {
		if _, err := streams.Provision(ctx, js, cfg); err != nil {
			t.Fatal(err)
		}
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, wire.LedgerStream, jetstream.ConsumerConfig{
		Durable:       "sink-ledger",
		FilterSubject: wire.LedgerSubject + ".>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return js, cons
}

// auditFixture is the other stream: single Events, not a Batch, because a
// fail_closed write-ahead must be durable before the tool runs and a batched
// write is by definition not yet written.
func auditFixture(t *testing.T) (jetstream.JetStream, jetstream.Consumer) {
	t.Helper()
	js := embedded(t)
	ctx := ctx5(t)
	for _, cfg := range []jetstream.StreamConfig{
		streams.AuditConfig(streams.Sizing{}), streams.DeadConfig(streams.Sizing{}),
	} {
		if _, err := streams.Provision(ctx, js, cfg); err != nil {
			t.Fatal(err)
		}
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, wire.AuditStream, jetstream.ConsumerConfig{
		Durable:       "sink-audit",
		FilterSubject: wire.AuditSubject + ".>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return js, cons
}

func ev(id string) *ledgerv1.Event {
	return &ledgerv1.Event{
		EventId: id,
		Time:    timestamppb.New(time.Now().UTC().Truncate(time.Second)),
		Tenant:  "acme",
		App:     "svc",
		Outcome: "ok",
	}
}

// publishBatch sends one message holding n events — the shape garmd publishes
// on the ledger stream.
func publishBatch(t *testing.T, js jetstream.JetStream, prefix string, n int) {
	t.Helper()
	b := &ledgerv1.Batch{}
	for i := 0; i < n; i++ {
		b.Events = append(b.Events, ev(fmt.Sprintf("%s-%d", prefix, i)))
	}
	publishRaw(t, js, marshal(t, b))
}

func publishRaw(t *testing.T, js jetstream.JetStream, data []byte) {
	t.Helper()
	if _, err := js.Publish(ctx5(t), wire.LedgerSubjectFor("acme", "svc"), data); err != nil {
		t.Fatal(err)
	}
}

func marshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type fakeSink struct {
	mu      sync.Mutex
	batches []drain.Batch
	failN   int
}

func (s *fakeSink) Flush(_ context.Context, b drain.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failN > 0 {
		s.failN--
		return errors.New("sink down")
	}
	s.batches = append(s.batches, b)
	return nil
}

func (s *fakeSink) snapshot() []drain.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]drain.Batch(nil), s.batches...)
}

func (s *fakeSink) rows() int {
	n := 0
	for _, b := range s.snapshot() {
		n += len(b.Rows)
	}
	return n
}

type failingDeadLetter struct{ err error }

func (d failingDeadLetter) Dead(context.Context, drain.Dead) error { return d.err }

func run(t *testing.T, d *drain.Drainer) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := d.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the drainer did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func drained(t *testing.T, cons jetstream.Consumer) bool {
	t.Helper()
	info, err := cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.NumAckPending == 0 && info.NumPending == 0 && info.NumRedelivered == 0
}

// --- tests -------------------------------------------------------------

// The batch limit counts ROWS. It counted messages when one message was one
// event; the ledger now carries a Batch per message, so a limit of 1000
// messages is a Parquet file of a million rows.
//
// FetchMax is 1 so each fetch brings exactly one four-event message: under row
// counting the flush comes after the second message (8 rows past a limit of
// 5), under message counting it would not come until the fifth.
func TestTheBatchLimitCountsRowsAndNotMessages(t *testing.T) {
	js, cons := ledgerFixture(t)
	for i := 0; i < 3; i++ {
		publishBatch(t, js, fmt.Sprintf("m%d", i), 4)
	}
	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 5, BatchInterval: 30 * time.Second, FetchMax: 1},
	})

	eventually(t, "a flush triggered by the row count", func() bool {
		return len(sink.snapshot()) > 0
	})
	first := sink.snapshot()[0]
	if len(first.Rows) != 8 {
		t.Fatalf("first flush has %d rows, want the 8 from two four-event messages; "+
			"a flush of 4 means the limit is not counting rows at all, and no flush "+
			"at all within the interval means it is still counting messages", len(first.Rows))
	}
	if first.FirstSeq == 0 || first.LastSeq < first.FirstSeq {
		t.Errorf("sequence range is %d..%d; Parquet names depend on it", first.FirstSeq, first.LastSeq)
	}
}

func TestAPartialBatchIsFlushedWhenTheIntervalExpires(t *testing.T) {
	js, cons := ledgerFixture(t)
	publishBatch(t, js, "m", 2)

	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 1_000_000, BatchInterval: 300 * time.Millisecond},
	})
	eventually(t, "the interval flush", func() bool { return sink.rows() == 2 })
	eventually(t, "everything acked", func() bool { return drained(t, cons) })
}

// A flush that fails must leave the messages for redelivery. Acking first and
// writing after is the shape that loses data on exactly the failures anyone
// would want the rows for.
func TestAFailedFlushIsRedeliveredRatherThanAcked(t *testing.T) {
	js, cons := ledgerFixture(t)
	publishBatch(t, js, "m", 3)

	sink := &fakeSink{failN: 1}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 3, BatchInterval: 300 * time.Millisecond},
	})
	eventually(t, "the redelivered rows to land", func() bool {
		ids := map[string]bool{}
		for _, b := range sink.snapshot() {
			for _, r := range b.Rows {
				ids[r.EventID] = true
			}
		}
		return len(ids) == 3
	})
}

// A record that cannot be parsed is written somewhere a person can find it,
// and only then terminated.
//
// The previous version called Term() on it and logged a line. That is
// defensible for metering and indefensible for audit, where it permanently
// discards a record something was obliged to keep — and the log line is not
// the record.
func TestAnUnreadableMessageIsDeadLetteredBeforeItIsTerminated(t *testing.T) {
	js, cons := ledgerFixture(t)
	garbage := []byte("\xff\xff not protobuf")
	publishRaw(t, js, garbage)
	publishBatch(t, js, "good", 1)

	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 100, BatchInterval: 200 * time.Millisecond},
	})
	eventually(t, "the good row to land", func() bool { return sink.rows() == 1 })
	eventually(t, "the poison message to be accounted for", func() bool { return drained(t, cons) })

	msg := firstDeadLetter(t, js)
	if got := string(msg.Data()); got != string(garbage) {
		t.Errorf("the dead letter holds %q, not the original bytes", got)
	}
	for hdr, want := range map[string]string{
		drain.HdrReason: string(row.ReasonUnmarshal),
		drain.HdrStream: wire.LedgerStream,
		drain.HdrIndex:  "-1",
	} {
		if got := msg.Headers().Get(hdr); got != want {
			t.Errorf("header %s = %q, want %q", hdr, got, want)
		}
	}
	if msg.Headers().Get(drain.HdrSequence) == "" {
		t.Error("the dead letter does not say which message it came from")
	}
}

// One bad event inside a batch of good ones costs one event.
func TestOneBadEventDoesNotDiscardTheGoodOnesBesideIt(t *testing.T) {
	js, cons := ledgerFixture(t)
	bad := ev("")
	publishRaw(t, js, marshal(t, &ledgerv1.Batch{Events: []*ledgerv1.Event{ev("a"), bad, ev("c")}}))

	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 100, BatchInterval: 200 * time.Millisecond},
	})
	eventually(t, "the two good rows", func() bool { return sink.rows() == 2 })
	eventually(t, "the message to be acked", func() bool { return drained(t, cons) })

	msg := firstDeadLetter(t, js)
	if got := msg.Headers().Get(drain.HdrIndex); got != "1" {
		t.Errorf("dead letter index = %q, want 1 — the position of the bad event", got)
	}
	var back ledgerv1.Event
	if err := proto.Unmarshal(msg.Data(), &back); err != nil {
		t.Fatalf("the dead-lettered event is not readable: %v", err)
	}
	if back.GetTenant() != "acme" {
		t.Errorf("the dead-lettered event lost its content: %+v", &back)
	}
}

// If the dead-letter write fails, nothing may be terminated and nothing may be
// acked. A Term here would be the discard the dead-letter stream exists to
// prevent, taken on the one path where we already know the record is unusual.
func TestNothingIsTerminatedWhenTheDeadLetterWriteFails(t *testing.T) {
	js, cons := ledgerFixture(t)
	publishRaw(t, js, []byte("\xff\xff not protobuf"))

	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: failingDeadLetter{err: errors.New("dead letter down")},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 100, BatchInterval: 200 * time.Millisecond},
	})
	// Give it several flush cycles to get it wrong in.
	time.Sleep(1500 * time.Millisecond)
	info, err := cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.NumRedelivered == 0 && info.NumPending == 0 && info.NumAckPending == 0 {
		t.Fatal("the message was consumed even though it was never recorded anywhere")
	}
	if n := deadLetterCount(t, js); n != 0 {
		t.Fatalf("the dead-letter stream holds %d messages after a failing writer", n)
	}
}

// Redelivery must not multiply dead letters. A flush that dead-letters and
// then fails to upload is retried, and without a stable message id one
// publisher bug becomes unbounded growth in the stream meant to reveal it.
func TestADeadLetterIsWrittenOnceHoweverOftenItIsRetried(t *testing.T) {
	js := embedded(t)
	if _, err := streams.Provision(ctx5(t), js, streams.DeadConfig(streams.Sizing{})); err != nil {
		t.Fatal(err)
	}
	d := &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"}
	rec := drain.Dead{
		Stream: wire.LedgerStream, Subject: "garm.v1.ledger.acme.svc", Sequence: 42,
		Reject: row.Reject{Reason: row.ReasonUnmarshal, Err: errors.New("nope"), Index: -1, Payload: []byte("x")},
	}
	for i := 0; i < 3; i++ {
		if err := d.Dead(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}
	if n := deadLetterCount(t, js); n != 1 {
		t.Fatalf("the dead-letter stream holds %d copies of one record, want 1", n)
	}
}

// Shutdown flushes. The rows in hand are already unacked work; dropping them
// on SIGTERM means every rolling restart loses up to one interval of records
// and redelivers them, for no reason.
func TestShutdownFlushesWhatIsBuffered(t *testing.T) {
	js, cons := ledgerFixture(t)
	publishBatch(t, js, "m", 3)

	sink := &fakeSink{}
	stop := run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 1_000_000, BatchInterval: time.Hour},
	})
	// Long interval and a huge row limit: nothing may flush until shutdown.
	time.Sleep(500 * time.Millisecond)
	if n := sink.rows(); n != 0 {
		t.Fatalf("%d rows flushed before shutdown; the test proves nothing", n)
	}
	stop()
	if n := sink.rows(); n != 3 {
		t.Fatalf("shutdown flushed %d rows, want 3", n)
	}
}

// An empty Batch is a well-formed message with nothing in it. It has to be
// acked: left unacked it is redelivered forever, and dead-lettered it fills
// the stream that is supposed to hold real problems.
func TestAnEmptyBatchIsAckedRatherThanRedeliveredForever(t *testing.T) {
	js, cons := ledgerFixture(t)
	publishRaw(t, js, marshal(t, &ledgerv1.Batch{}))

	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 100, BatchInterval: 200 * time.Millisecond},
	})
	eventually(t, "the empty message to be acked", func() bool { return drained(t, cons) })
	if n := sink.rows(); n != 0 {
		t.Fatalf("an empty batch produced %d rows", n)
	}
	if n := deadLetterCount(t, js); n != 0 {
		t.Fatalf("an empty batch produced %d dead letters", n)
	}
}

// The audit stream carries one Event per message and drains with the same loop
// and the same ack discipline. It is a separate stream and not a separate
// pipeline: what differs is the envelope and what happens when the stream is
// full, neither of which is the drain's business.
func TestTheAuditStreamDrainsSingleEvents(t *testing.T) {
	js, cons := auditFixture(t)
	for _, id := range []string{"intent-1", "outcome-1"} {
		if _, err := js.Publish(ctx5(t), wire.AuditSubjectFor("acme", "svc"), marshal(t, ev(id))); err != nil {
			t.Fatal(err)
		}
	}
	sink := &fakeSink{}
	run(t, &drain.Drainer{
		Consumer: cons, Sink: sink, Dead: &drain.JetStreamDeadLetter{JS: js, Origin: "audit"},
		Envelope: row.EnvelopeEvent,
		Cfg:      drain.Config{BatchMaxRows: 100, BatchInterval: 200 * time.Millisecond},
	})
	eventually(t, "both audit rows", func() bool { return sink.rows() == 2 })
	eventually(t, "both acked", func() bool { return drained(t, cons) })
	if n := deadLetterCount(t, js); n != 0 {
		t.Fatalf("%d dead letters from two well-formed audit events", n)
	}
}

// A nil DeadLetter would mean "discard what cannot be read", silently. The
// refusal is at startup because the alternative is discovering it on the first
// bad message, in production, from an absence.
func TestADrainerWithNoDeadLetterRefusesToStart(t *testing.T) {
	_, cons := ledgerFixture(t)
	err := (&drain.Drainer{Consumer: cons, Sink: &fakeSink{}}).Run(context.Background())
	if err == nil {
		t.Fatal("a drainer with no dead-letter target started")
	}
}

// --- dead-letter stream helpers ---------------------------------------

func deadLetterCount(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	s, err := js.Stream(ctx5(t), streams.DeadStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func firstDeadLetter(t *testing.T, js jetstream.JetStream) jetstream.Msg {
	t.Helper()
	var msg jetstream.Msg
	eventually(t, "a dead letter to arrive", func() bool { return deadLetterCount(t, js) > 0 })
	s, err := js.Stream(ctx5(t), streams.DeadStream)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := s.CreateOrUpdateConsumer(ctx5(t), jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for m := range msgs.Messages() {
		msg = m
	}
	if msg == nil {
		t.Fatal("no dead letter could be read back")
	}
	return msg
}

// The whole path, once: a message on a real broker, through the drain, into a
// lake on disk, read back with the engine a developer would point at it.
//
// This became possible to write when the local destination arrived: it needs
// no object store, so nothing has to be stood up for it. The S3 half of the
// path is still untested end to end — see KNOWN-GAPS — but everything up to
// the PUT is the same code, and the keys are built in one place for both.
func TestAMessageOnTheStreamBecomesQueryableParquetInALakeOnDisk(t *testing.T) {
	js, cons := ledgerFixture(t)
	root := t.TempDir()
	dest, err := lake.NewDirStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Ensure(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	publishBatch(t, js, "e", 3)

	run(t, &drain.Drainer{
		Consumer: cons,
		Sink:     &lake.ParquetSink{Dest: dest, KeyPrefix: "ledger", InstanceID: "box-77"},
		Dead:     &drain.JetStreamDeadLetter{JS: js, Origin: "ledger"},
		Envelope: row.EnvelopeBatch,
		Cfg:      drain.Config{BatchMaxRows: 3, BatchInterval: time.Second, FetchMax: 8},
	})
	eventually(t, "a part file under the lake directory", func() bool {
		return len(parquetFiles(t, root)) > 0
	})
	eventually(t, "the messages to be acked", func() bool { return drained(t, cons) })

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// The query from the README, against a local path.
	glob := filepath.Join(root, "ledger", "**", "*.parquet")
	var count int
	var tenant, app string
	if err := db.QueryRow(fmt.Sprintf(
		`SELECT count(*), any_value(tenant), any_value(app)
		 FROM read_parquet('%s', hive_partitioning=true)`, glob)).Scan(&count, &tenant, &app); err != nil {
		t.Fatalf("reading %s: %v", glob, err)
	}
	if count != 3 || tenant != "acme" || app != "svc" {
		t.Fatalf("the lake holds count=%d tenant=%q app=%q, want the three published events", count, tenant, app)
	}
	// And the hive path is on disk, not just in the file: the partition a row
	// lands in is what lets a reader prune before it opens anything.
	for _, p := range parquetFiles(t, root) {
		if !strings.Contains(filepath.ToSlash(p), "/ledger/date=") || !strings.Contains(filepath.ToSlash(p), "/app=svc/") {
			t.Errorf("%s is not on a hive path", p)
		}
	}
}

func parquetFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		out = append(out, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
