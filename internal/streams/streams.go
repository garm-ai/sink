// Package streams declares the JetStream streams this pipeline reads and
// writes, and provisions them.
//
// The stream configuration is code here rather than a runbook because the two
// streams differ in the one field that is silent when it is wrong. An audit
// stream configured DiscardOld deletes the oldest records to make room for
// new ones and every publish still succeeds: the publisher is told the record
// was kept, the auditor finds a gap years later, and nothing in between
// reported anything. The ledger wants exactly that behaviour, which is why
// one shared config cannot serve both.
//
// So Provision creates what is missing and REFUSES what exists with the wrong
// policy. It never patches. A stream whose discard policy someone changed by
// hand is a decision, possibly a deliberate one taken during an incident, and
// silently reverting it from a deploy is how the incident comes back.
package streams

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/garm-ai/garm/contracts/wire"
)

// The dead-letter stream. Its name and subject are this repository's to
// choose — the wire package names what garmd publishes, and nothing publishes
// here but the sink.
//
// It is a stream and not a log line because the records in it are the ones
// that were unreadable, which is to say the ones somebody will need. A record
// we could not parse is still evidence that a call happened.
const (
	DeadStream  = "GARM_SINK_DEAD"
	DeadSubject = "garm.v1.sink.dead"
)

// DeadSubjectFor keeps the two origins apart so a ledger parse failure and an
// audit parse failure can be alerted on differently. They are not the same
// severity and never will be.
func DeadSubjectFor(origin string) string {
	if origin == "" {
		origin = "unknown"
	}
	return DeadSubject + "." + origin
}

// Sizing is what an operator legitimately tunes. Everything else about a
// stream is policy, and policy is not a flag.
type Sizing struct {
	MaxBytes int64
	MaxAge   time.Duration
	Replicas int
}

func (s Sizing) or(maxBytes int64, maxAge time.Duration) Sizing {
	if s.MaxBytes == 0 {
		s.MaxBytes = maxBytes
	}
	if s.MaxAge == 0 {
		s.MaxAge = maxAge
	}
	if s.Replicas == 0 {
		s.Replicas = 1
	}
	return s
}

// LedgerConfig is the metering stream: high volume, and the lake is the
// durable copy.
//
// DiscardOld because a metering publisher must never block on a full stream —
// a sidecar that cannot publish a usage event must still serve the request.
// MaxAge is the drain's grace period, not a retention promise: a sink that has
// been down for longer than this has lost rows, and that is the trade the
// ledger makes in exchange for never failing a call.
func LedgerConfig(s Sizing) jetstream.StreamConfig {
	s = s.or(8<<30, 7*24*time.Hour)
	return jetstream.StreamConfig{
		Name:        wire.LedgerStream,
		Subjects:    []string{wire.LedgerSubject + ".>"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxBytes:    s.MaxBytes,
		MaxAge:      s.MaxAge,
		Replicas:    s.Replicas,
		Duplicates:  2 * time.Minute,
		Description: "garm ledger: metering records, drained to the lake",
	}
}

// AuditConfig is the audit stream: lower volume, and the record itself.
//
// DiscardNew is the point of the separate stream. When it fills, the publish
// FAILS — and a fail_closed tool that cannot record its intent refuses to run.
// A refused call is a visible, attributable failure; a discarded audit record
// is an invisible one. Sizing this stream is therefore an availability
// decision, taken deliberately, rather than a limit that quietly deletes
// evidence.
//
// DenyDelete and DenyPurge for the same reason: with them a stream admin
// cannot remove a record by accident, and removing one on purpose requires
// changing the stream's configuration first, which leaves a trace.
//
// MaxAge is zero — nothing ages out. The sink drains into the lake, the lake
// is where retention is enforced, and an age limit here would be a second,
// quieter retention policy that nobody reads.
func AuditConfig(s Sizing) jetstream.StreamConfig {
	s = s.or(8<<30, 0)
	return jetstream.StreamConfig{
		Name:        wire.AuditStream,
		Subjects:    []string{wire.AuditSubject + ".>"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardNew,
		MaxBytes:    s.MaxBytes,
		MaxAge:      s.MaxAge,
		Replicas:    s.Replicas,
		DenyDelete:  true,
		DenyPurge:   true,
		Duplicates:  2 * time.Minute,
		Description: "garm audit: the record itself; refuses writes rather than discarding",
	}
}

// DeadConfig holds what could not be parsed.
//
// DiscardNew, like the audit stream: this stream filling means something is
// publishing garbage at volume, and the useful thing to keep is the FIRST
// evidence of it, not the most recent. Dropping the oldest would roll the
// start of the incident out of the window while the incident is still running.
func DeadConfig(s Sizing) jetstream.StreamConfig {
	s = s.or(1<<30, 0)
	return jetstream.StreamConfig{
		Name:        DeadStream,
		Subjects:    []string{DeadSubject + ".>"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardNew,
		MaxBytes:    s.MaxBytes,
		MaxAge:      s.MaxAge,
		Replicas:    s.Replicas,
		DenyDelete:  true,
		DenyPurge:   true,
		Duplicates:  2 * time.Minute,
		Description: "garm sink dead letters: records that could not be parsed",
	}
}

// ErrPolicyMismatch is what a caller gets when a stream exists with a
// configuration this package would not have created.
var ErrPolicyMismatch = errors.New("stream exists with a different policy")

// Status is what Provision did.
type Status struct {
	Name    string
	Created bool
}

// Provision creates want if it does not exist, and otherwise verifies it.
//
// It does not patch, and it does not fall back to CreateOrUpdateStream. An
// update would turn `provision` into a command that silently repairs a stream
// an operator changed on purpose, and — worse — would let the fix for "the
// audit stream is full" be a deploy that flips it to DiscardOld and starts
// dropping records with every publish still returning success.
func Provision(ctx context.Context, js jetstream.JetStream, want jetstream.StreamConfig) (Status, error) {
	existing, err := js.Stream(ctx, want.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if _, err := js.CreateStream(ctx, want); err != nil {
			return Status{Name: want.Name}, fmt.Errorf("create stream %s: %w", want.Name, err)
		}
		return Status{Name: want.Name, Created: true}, nil
	}
	if err != nil {
		return Status{Name: want.Name}, fmt.Errorf("look up stream %s: %w", want.Name, err)
	}
	info, err := existing.Info(ctx)
	if err != nil {
		return Status{Name: want.Name}, fmt.Errorf("info for stream %s: %w", want.Name, err)
	}
	if diffs := Diff(want, info.Config); len(diffs) > 0 {
		return Status{Name: want.Name}, fmt.Errorf(
			"%w: %s differs in %d field(s):\n  %s\n\n"+
				"Nothing was changed. Decide which is right: if the running stream is "+
				"wrong, fix it explicitly (`nats stream edit %s`) or recreate it; if this "+
				"binary is wrong, it needs a change here.",
			ErrPolicyMismatch, want.Name, len(diffs), strings.Join(diffs, "\n  "), want.Name)
	}
	return Status{Name: want.Name}, nil
}

// Diff lists the policy fields on which a live stream disagrees with what this
// package would have created.
//
// The list is deliberately the whole managed policy rather than only the
// dangerous fields. "Only discard policy matters" is true right up until the
// day a subject is missing from the stream, every publish to it succeeds
// because publishing to a subject nothing captures is not an error, and no
// record of that tenant ever reaches the lake.
func Diff(want, got jetstream.StreamConfig) []string {
	var out []string
	cmp := func(field string, w, g any) {
		if fmt.Sprint(w) != fmt.Sprint(g) {
			out = append(out, fmt.Sprintf("%s: want %v, running %v", field, w, g))
		}
	}
	cmp("subjects", want.Subjects, got.Subjects)
	cmp("storage", want.Storage, got.Storage)
	cmp("retention", want.Retention, got.Retention)
	cmp("discard", want.Discard, got.Discard)
	cmp("max_age", want.MaxAge, got.MaxAge)
	cmp("max_bytes", limit(want.MaxBytes), limit(got.MaxBytes))
	cmp("max_msgs", limit(want.MaxMsgs), limit(got.MaxMsgs))
	cmp("replicas", want.Replicas, got.Replicas)
	cmp("deny_delete", want.DenyDelete, got.DenyDelete)
	cmp("deny_purge", want.DenyPurge, got.DenyPurge)
	return out
}

// limit folds the two spellings of "no limit" together. A config field left
// at zero comes back from the server as -1, so comparing them raw reports
// every freshly created stream as a policy mismatch — a check that fires on
// everything is a check nobody reads.
func limit(v int64) int64 {
	if v <= 0 {
		return -1
	}
	return v
}
