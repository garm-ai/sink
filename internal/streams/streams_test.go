package streams_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/garm-ai/garm/contracts/wire"
	"github.com/garm-ai/sink/internal/streams"
)

// A real server on an OS-chosen port. Discard policy is the broker's
// behaviour, and a fake that agreed with our beliefs about it would be worth
// nothing — the whole reason this file exists is that the wrong policy is
// silent.
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

func all() []jetstream.StreamConfig {
	return []jetstream.StreamConfig{
		streams.LedgerConfig(streams.Sizing{}),
		streams.AuditConfig(streams.Sizing{}),
		streams.DeadConfig(streams.Sizing{}),
	}
}

func TestProvisionCreatesEachStreamOnceAndThenVerifiesIt(t *testing.T) {
	js := embedded(t)
	for _, cfg := range all() {
		st, err := streams.Provision(ctx5(t), js, cfg)
		if err != nil {
			t.Fatalf("%s: %v", cfg.Name, err)
		}
		if !st.Created {
			t.Errorf("%s: reported as existing on a fresh server", cfg.Name)
		}
	}
	// Running it again must be a verification, not a second creation and not
	// an error. `provision` is expected to run on every deploy.
	for _, cfg := range all() {
		st, err := streams.Provision(ctx5(t), js, cfg)
		if err != nil {
			t.Fatalf("%s on the second run: %v", cfg.Name, err)
		}
		if st.Created {
			t.Errorf("%s: created twice", cfg.Name)
		}
	}
}

// The audit stream refuses a write when it is full. That refusal is the whole
// reason it is a separate stream: a fail_closed tool that cannot record its
// intent must not run, and DiscardOld would let it run while the record of an
// older call is deleted to make room — with every publish returning success.
func TestAFullAuditStreamRefusesTheWriteInsteadOfDiscardingAnOlderRecord(t *testing.T) {
	js := embedded(t)
	cfg := streams.AuditConfig(streams.Sizing{MaxBytes: 16 << 10})
	if _, err := streams.Provision(ctx5(t), js, cfg); err != nil {
		t.Fatal(err)
	}
	subject := wire.AuditSubjectFor("acme", "svc")
	payload := make([]byte, 1024)

	var lastErr error
	published := 0
	for i := 0; i < 200 && lastErr == nil; i++ {
		_, lastErr = js.Publish(ctx5(t), subject, payload)
		if lastErr == nil {
			published++
		}
	}
	if lastErr == nil {
		t.Fatal("the audit stream accepted 200 writes into 16KiB; it is discarding records and acking them")
	}
	if published == 0 {
		t.Fatalf("nothing could be published at all: %v", lastErr)
	}
	// And what was already written is still there.
	s, err := js.Stream(ctx5(t), wire.AuditStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.FirstSeq != 1 {
		t.Errorf("the oldest audit record is sequence %d, not 1; records were discarded", info.State.FirstSeq)
	}
}

// The ledger makes the opposite trade, and must keep accepting writes. A
// sidecar that cannot publish a metering event still has a request to serve.
func TestAFullLedgerStreamKeepsAcceptingWritesAndDropsTheOldest(t *testing.T) {
	js := embedded(t)
	cfg := streams.LedgerConfig(streams.Sizing{MaxBytes: 16 << 10})
	if _, err := streams.Provision(ctx5(t), js, cfg); err != nil {
		t.Fatal(err)
	}
	subject := wire.LedgerSubjectFor("acme", "svc")
	payload := make([]byte, 1024)
	for i := 0; i < 200; i++ {
		if _, err := js.Publish(ctx5(t), subject, payload); err != nil {
			t.Fatalf("the ledger refused write %d: %v — a metering publisher must never block", i, err)
		}
	}
	s, err := js.Stream(ctx5(t), wire.LedgerStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.FirstSeq == 1 {
		t.Error("nothing was discarded; the stream did not actually fill and the test proves nothing")
	}
}

// Provision refuses and changes nothing. A deploy that silently repaired an
// audit stream someone had edited would erase both the edit and the evidence
// of it — and the edit people actually make under pressure is the one that
// flips a full audit stream to DiscardOld.
func TestProvisionRefusesAWrongPolicyAndPatchesNothing(t *testing.T) {
	js := embedded(t)
	wrong := streams.AuditConfig(streams.Sizing{})
	wrong.Discard = jetstream.DiscardOld
	wrong.DenyDelete = false
	if _, err := js.CreateStream(ctx5(t), wrong); err != nil {
		t.Fatal(err)
	}

	_, err := streams.Provision(ctx5(t), js, streams.AuditConfig(streams.Sizing{}))
	if !errors.Is(err, streams.ErrPolicyMismatch) {
		t.Fatalf("Provision returned %v, want ErrPolicyMismatch", err)
	}
	for _, want := range []string{"discard", "deny_delete"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q, so nobody knows what to fix:\n%v", want, err)
		}
	}
	s, err := js.Stream(ctx5(t), wire.AuditStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Discard != jetstream.DiscardOld {
		t.Fatal("Provision changed the running stream; it must only report")
	}
}

// A missing subject is the quiet one. Publishing to a subject no stream
// captures is not an error in NATS: the publisher succeeds, the record never
// lands, and nothing anywhere reports it.
func TestProvisionRefusesAStreamThatHasLostASubject(t *testing.T) {
	js := embedded(t)
	wrong := streams.LedgerConfig(streams.Sizing{})
	wrong.Subjects = []string{wire.LedgerSubject + ".acme.>"}
	if _, err := js.CreateStream(ctx5(t), wrong); err != nil {
		t.Fatal(err)
	}
	_, err := streams.Provision(ctx5(t), js, streams.LedgerConfig(streams.Sizing{}))
	if !errors.Is(err, streams.ErrPolicyMismatch) || !strings.Contains(err.Error(), "subjects") {
		t.Fatalf("a narrowed subject list was accepted: %v", err)
	}
}

// Zero and -1 are the same "no limit", and the server returns the second for
// the first. A diff that reported it would fire on every freshly created
// stream, and a check that always fires is a check nobody reads.
func TestAFreshlyCreatedStreamDiffsAgainstNothing(t *testing.T) {
	js := embedded(t)
	for _, cfg := range all() {
		if _, err := js.CreateStream(ctx5(t), cfg); err != nil {
			t.Fatal(err)
		}
		s, err := js.Stream(ctx5(t), cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Info(ctx5(t))
		if err != nil {
			t.Fatal(err)
		}
		if d := streams.Diff(cfg, info.Config); len(d) > 0 {
			t.Errorf("%s diffs against what just created it: %v", cfg.Name, d)
		}
	}
}

// The two origins are not the same severity and never will be: a ledger parse
// failure is a metering gap, an audit parse failure is a missing record.
func TestTheDeadLetterSubjectsSeparateTheTwoOrigins(t *testing.T) {
	l, a := streams.DeadSubjectFor("ledger"), streams.DeadSubjectFor("audit")
	if l == a {
		t.Fatal("both origins dead-letter to the same subject")
	}
	for _, s := range []string{l, a, streams.DeadSubjectFor("")} {
		if !strings.HasPrefix(s, streams.DeadSubject+".") {
			t.Errorf("%q is outside the dead-letter stream's subject space, so nothing captures it", s)
		}
		if strings.HasSuffix(s, ".") {
			t.Errorf("%q ends in an empty subject element, which NATS rejects", s)
		}
	}
}
