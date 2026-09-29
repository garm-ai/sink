package tail_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/garm-ai/contracts/garm/ledger/v1"
	"github.com/garm-ai/contracts/wire"
	"github.com/garm-ai/sink/internal/row"
	"github.com/garm-ai/sink/internal/streams"
	"github.com/garm-ai/sink/internal/tail"
)

// --- harness -----------------------------------------------------------

// embedded runs a real NATS server in this process, on a port the OS picks —
// the same harness the drain tests use, for the same reason: whether a
// consumer is durable and what it acks are the broker's facts, not ours.
func embedded(t *testing.T) jetstream.JetStream {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
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

func ledgerStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	js := embedded(t)
	if _, err := streams.Provision(ctx5(t), js, streams.LedgerConfig(streams.Sizing{})); err != nil {
		t.Fatal(err)
	}
	return js
}

func auditStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	js := embedded(t)
	if _, err := streams.Provision(ctx5(t), js, streams.AuditConfig(streams.Sizing{})); err != nil {
		t.Fatal(err)
	}
	return js
}

func ev(id, outcome string) *ledgerv1.Event {
	return &ledgerv1.Event{
		EventId:     id,
		Time:        timestamppb.New(time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)),
		Tenant:      "acme",
		App:         "svc",
		Outcome:     outcome,
		Tool:        "initiate_payment",
		ErrorDetail: "user with email ada@corp.com not found",
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

// publishBatch sends one ledger message holding the given events.
func publishBatch(t *testing.T, js jetstream.JetStream, events ...*ledgerv1.Event) {
	t.Helper()
	b := &ledgerv1.Batch{Events: events}
	if _, err := js.Publish(ctx5(t), wire.LedgerSubjectFor("acme", "svc"), marshal(t, b)); err != nil {
		t.Fatal(err)
	}
}

func ledgerOpts() tail.Options {
	return tail.Options{
		Stream: wire.LedgerStream, Subject: wire.LedgerSubject + ".>", Envelope: row.EnvelopeBatch,
		FetchWait: 200 * time.Millisecond,
	}
}

// run tails to completion and returns stdout and stderr.
func run(t *testing.T, js jetstream.JetStream, o tail.Options) (string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tail.Run(ctx, js, o, &out, &errOut); err != nil {
		t.Fatalf("tail: %v\nstderr:\n%s", err, errOut.String())
	}
	return out.String(), errOut.String()
}

func lines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func decodeLine(t *testing.T, line string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, line)
	}
	return m
}

// --- the row on the terminal ------------------------------------------

// Each event is one line, and the keys are the lake's column names in the
// lake's order — a line and a Parquet row are the same schema, so a query
// written against one can be written against the other.
func TestOneJSONLinePerEventUnderTheLakeColumnNames(t *testing.T) {
	js := ledgerStream(t)
	publishBatch(t, js, ev("ev-1", "ok"), ev("ev-2", "ok"), ev("ev-3", "denied"))
	publishBatch(t, js, ev("ev-4", "ok"))

	out, _ := run(t, js, ledgerOpts())
	got := lines(out)
	if len(got) != 4 {
		t.Fatalf("got %d lines, want 4:\n%s", len(got), out)
	}

	// Key ORDER, not just key set: decode by hand so the order survives.
	dec := json.NewDecoder(strings.NewReader(got[0]))
	if tok, _ := dec.Token(); tok != json.Delim('{') {
		t.Fatalf("line does not start an object: %s", got[0])
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, tok.(string))
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if len(keys) != len(row.Columns) {
		t.Fatalf("line has %d keys, row.Columns has %d", len(keys), len(row.Columns))
	}
	for i, c := range row.Columns {
		if keys[i] != c.Name {
			t.Errorf("key %d = %q, want %q (the lake's column order)", i, keys[i], c.Name)
		}
	}

	m := decodeLine(t, got[2])
	if m["event_id"] != "ev-3" || m["outcome"] != "denied" || m["tool"] != "initiate_payment" {
		t.Errorf("third line is not ev-3: %v", m)
	}
	if m["time"] != "2026-09-29T12:00:00Z" {
		t.Errorf("time = %v, want RFC 3339 UTC", m["time"])
	}
}

func TestTheAuditStreamIsOneEventPerMessage(t *testing.T) {
	js := auditStream(t)
	for _, id := range []string{"au-1", "au-2"} {
		if _, err := js.Publish(ctx5(t), wire.AuditSubjectFor("acme", "svc"), marshal(t, ev(id, "ok"))); err != nil {
			t.Fatal(err)
		}
	}
	out, _ := run(t, js, tail.Options{
		Stream: wire.AuditStream, Subject: wire.AuditSubject + ".>", Envelope: row.EnvelopeEvent,
		FetchWait: 200 * time.Millisecond,
	})
	got := lines(out)
	if len(got) != 2 || decodeLine(t, got[1])["event_id"] != "au-2" {
		t.Fatalf("audit tail:\n%s", out)
	}
}

// --- a reader, not a drain --------------------------------------------

// The properties that make this safe to run against a live plane, asserted
// on the configuration before it reaches a broker.
func TestTheConsumerIsEphemeralAndAcksNothing(t *testing.T) {
	state := jetstream.StreamState{FirstSeq: 1, LastSeq: 500}
	for name, start := range map[string]tail.Start{
		"last":  {Last: 20},
		"age":   {Age: time.Minute},
		"start": {FromStart: true},
		"zero":  {},
	} {
		o := ledgerOpts()
		o.Start = start
		cfg := tail.ConsumerConfig(o, state)
		if cfg.Durable != "" || cfg.Name != "" {
			t.Errorf("%s: the consumer has a name (%q/%q); it would outlive the tail", name, cfg.Durable, cfg.Name)
		}
		if cfg.AckPolicy != jetstream.AckNonePolicy {
			t.Errorf("%s: AckPolicy = %v, want AckNone: a tail must have no ack state to get wrong", name, cfg.AckPolicy)
		}
		if cfg.InactiveThreshold <= 0 {
			t.Errorf("%s: no InactiveThreshold; a tail killed with -9 would leave a consumer forever", name)
		}
	}
}

// And the same thing observed at the broker: a drain's durable consumer,
// with messages pending and an ack floor, is exactly as it was after a tail
// has read the whole stream.
func TestATailLeavesTheDrainsDurableConsumerUntouched(t *testing.T) {
	js := ledgerStream(t)
	for i := 0; i < 5; i++ {
		publishBatch(t, js, ev(fmt.Sprintf("ev-%d", i), "ok"))
	}
	ctx := ctx5(t)
	durable, err := js.CreateOrUpdateConsumer(ctx, wire.LedgerStream, jetstream.ConsumerConfig{
		Durable: "sink-ledger", FilterSubject: wire.LedgerSubject + ".>",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The drain has consumed two and acked one: an ack floor, a pending
	// ack and three still pending. Every one of those must survive.
	msgs, err := durable.Fetch(2, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	first := true
	for m := range msgs.Messages() {
		if first {
			if err := m.DoubleAck(ctx); err != nil {
				t.Fatal(err)
			}
			first = false
		}
	}
	before, err := durable.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.AckFloor.Stream != 1 || before.NumAckPending != 1 || before.NumPending != 3 {
		t.Fatalf("fixture: ack_floor=%d ack_pending=%d pending=%d", before.AckFloor.Stream, before.NumAckPending, before.NumPending)
	}

	o := ledgerOpts()
	o.Start.FromStart = true
	out, errOut := run(t, js, o)
	if n := len(lines(out)); n != 5 {
		t.Fatalf("tail printed %d lines, want all 5:\n%s", n, out)
	}
	if !strings.Contains(errOut, "durable=false") || !strings.Contains(errOut, "ack=none") {
		t.Errorf("the tail does not say what it is on stderr:\n%s", errOut)
	}

	after, err := durable.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Sequence numbers, not the SequenceInfo structs: those carry a
	// *time.Time and would compare by pointer.
	if after.AckFloor.Stream != before.AckFloor.Stream || after.Delivered.Stream != before.Delivered.Stream ||
		after.NumAckPending != before.NumAckPending || after.NumPending != before.NumPending ||
		after.NumRedelivered != before.NumRedelivered {
		t.Errorf("the durable consumer moved:\nbefore ack_floor=%d delivered=%d ack_pending=%d pending=%d\nafter  ack_floor=%d delivered=%d ack_pending=%d pending=%d",
			before.AckFloor.Stream, before.Delivered.Stream, before.NumAckPending, before.NumPending,
			after.AckFloor.Stream, after.Delivered.Stream, after.NumAckPending, after.NumPending)
	}

	// Nothing but the durable is left on the stream: no second durable, and
	// the ephemeral is gone rather than waiting for its inactivity timer.
	st, err := js.Stream(ctx, wire.LedgerStream)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for n := range st.ConsumerNames(ctx).Name() {
		names = append(names, n)
	}
	if len(names) != 1 || names[0] != "sink-ledger" {
		t.Errorf("consumers after the tail = %v, want only sink-ledger", names)
	}
}

// --- where to start and when to stop ---------------------------------

func TestSinceCountsMessagesBackFromTheEnd(t *testing.T) {
	js := ledgerStream(t)
	for i := 1; i <= 5; i++ {
		// Two events per message, so "messages" and "events" are visibly
		// different units.
		publishBatch(t, js, ev(fmt.Sprintf("m%d-a", i), "ok"), ev(fmt.Sprintf("m%d-b", i), "ok"))
	}
	o := ledgerOpts()
	o.Start = tail.Start{Last: 2}
	out, _ := run(t, js, o)
	got := lines(out)
	if len(got) != 4 {
		t.Fatalf("--since 2 printed %d events, want the 4 in the last 2 messages:\n%s", len(got), out)
	}
	if decodeLine(t, got[0])["event_id"] != "m4-a" || decodeLine(t, got[3])["event_id"] != "m5-b" {
		t.Errorf("wrong window:\n%s", out)
	}
}

func TestSinceLargerThanTheStreamMeansFromTheStart(t *testing.T) {
	js := ledgerStream(t)
	publishBatch(t, js, ev("only", "ok"))
	o := ledgerOpts()
	o.Start = tail.Start{Last: 1000}
	out, _ := run(t, js, o)
	if len(lines(out)) != 1 {
		t.Fatalf("got:\n%s", out)
	}
}

func TestAnEmptyStreamPrintsNothingAndReturns(t *testing.T) {
	js := ledgerStream(t)
	out, _ := run(t, js, ledgerOpts())
	if out != "" {
		t.Fatalf("printed something from an empty stream:\n%s", out)
	}
}

func TestFollowKeepsReadingUntilCancelled(t *testing.T) {
	js := ledgerStream(t)
	publishBatch(t, js, ev("before", "ok"))

	var out, errOut syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	o := ledgerOpts()
	o.Follow = true
	go func() { done <- tail.Run(ctx, js, o, &out, &errOut) }()

	waitFor(t, func() bool { return strings.Contains(out.String(), `"before"`) })
	publishBatch(t, js, ev("after", "ok"))
	waitFor(t, func() bool { return strings.Contains(out.String(), `"after"`) })

	select {
	case err := <-done:
		t.Fatalf("follow returned before being cancelled: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("follow returned an error on cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follow did not stop on cancellation")
	}
}

// --- filtering and shaping -------------------------------------------

func TestFilterIsAppliedToDecodedRows(t *testing.T) {
	js := ledgerStream(t)
	publishBatch(t, js, ev("ok-1", "ok"), ev("den-1", "denied"))
	publishBatch(t, js, ev("den-2", "denied"), ev("ok-2", "ok"))

	o := ledgerOpts()
	o.Filter = map[string]string{"outcome": "denied", "tool": "initiate_payment"}
	out, _ := run(t, js, o)
	got := lines(out)
	if len(got) != 2 {
		t.Fatalf("got %d lines, want the 2 denied:\n%s", len(got), out)
	}
	for _, l := range got {
		if m := decodeLine(t, l); m["outcome"] != "denied" {
			t.Errorf("filter let through %v", m["event_id"])
		}
	}

	o.Filter = map[string]string{"outcome": "denied", "tool": "other"}
	if out, _ := run(t, js, o); out != "" {
		t.Errorf("two filters are AND, not OR:\n%s", out)
	}
}

func TestAnUnknownFilterColumnIsRefusedWithTheColumnList(t *testing.T) {
	_, err := tail.ParseFilters([]string{"outcom=denied"})
	if err == nil || !strings.Contains(err.Error(), `"outcom"`) || !strings.Contains(err.Error(), "outcome") {
		t.Fatalf("err = %v; want the bad key quoted and the real columns listed", err)
	}
	if _, err := tail.ParseFilters([]string{"outcome"}); err == nil {
		t.Error("a filter with no = was accepted")
	}
	f, err := tail.ParseFilters([]string{"run_id=r-1", "tool=a=b"})
	if err != nil || f["run_id"] != "r-1" || f["tool"] != "a=b" {
		t.Errorf("filters = %v, %v", f, err)
	}
}

func TestParseSince(t *testing.T) {
	for in, want := range map[string]tail.Start{
		"":    {Last: tail.DefaultLast},
		"20":  {Last: 20},
		"10m": {Age: 10 * time.Minute},
		"1h":  {Age: time.Hour},
	} {
		got, err := tail.ParseSince(in)
		if err != nil || got != want {
			t.Errorf("ParseSince(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"0", "-5", "-1m", "twenty", "5x"} {
		if _, err := tail.ParseSince(bad); err == nil {
			t.Errorf("ParseSince(%q) accepted", bad)
		}
	}
}

func TestNoDetailDropsErrorDetailAndNothingElse(t *testing.T) {
	js := ledgerStream(t)
	publishBatch(t, js, ev("ev-1", "error"))
	o := ledgerOpts()
	o.NoDetail = true
	out, _ := run(t, js, o)
	if strings.Contains(out, "ada@corp.com") {
		t.Fatalf("--no-detail printed the detail:\n%s", out)
	}
	m := decodeLine(t, lines(out)[0])
	if _, present := m["error_detail"]; present {
		t.Error("error_detail is present as a key; --no-detail drops the column, it does not blank it")
	}
	if len(m) != len(row.Columns)-1 {
		t.Errorf("%d keys, want every column but one", len(m))
	}
}

func TestPrettyIndentsAndKeepsColumnOrder(t *testing.T) {
	line, err := tail.Encode(row.Row{EventID: "e", Time: time.Unix(0, 0).UTC()}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	s := string(line)
	if !strings.HasPrefix(s, "{\n  \"event_id\": \"e\",\n  \"time\": ") || !strings.HasSuffix(s, "\n}\n") {
		t.Fatalf("pretty output:\n%s", s)
	}
}

func TestAnUnreadableMessageIsReportedAndTheRestStillPrints(t *testing.T) {
	js := ledgerStream(t)
	publishBatch(t, js, ev("good-1", "ok"))
	if _, err := js.Publish(ctx5(t), wire.LedgerSubjectFor("acme", "svc"), []byte("\xff\xff not protobuf")); err != nil {
		t.Fatal(err)
	}
	publishBatch(t, js, ev("good-2", "ok"))

	out, errOut := run(t, js, ledgerOpts())
	if n := len(lines(out)); n != 2 {
		t.Fatalf("got %d lines, want the 2 good events:\n%s", n, out)
	}
	if !strings.Contains(errOut, "seq 2") || !strings.Contains(errOut, "unreadable") {
		t.Errorf("stderr does not name the bad message:\n%s", errOut)
	}
}

// --- helpers ----------------------------------------------------------

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
