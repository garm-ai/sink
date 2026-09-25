package drain

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/garm-ai/sink/internal/streams"
)

// Header names on a dead-lettered record. They carry everything needed to
// find the original message in its own stream, so the body can stay the
// original bytes and nothing has to be reconstructed from a log.
const (
	HdrReason   = "Garm-Dead-Reason"
	HdrError    = "Garm-Dead-Error"
	HdrStream   = "Garm-Dead-Stream"
	HdrSubject  = "Garm-Dead-Subject"
	HdrSequence = "Garm-Dead-Sequence"
	HdrIndex    = "Garm-Dead-Event-Index"
)

// JetStreamDeadLetter publishes unreadable records to the dead-letter stream.
//
// It publishes synchronously and waits for the ack. That is the slow way, and
// it is the only way the caller's promise holds: the message it came from is
// terminated once this returns nil, so a fire-and-forget publish would turn
// "recorded elsewhere" into "probably recorded elsewhere".
type JetStreamDeadLetter struct {
	JS     jetstream.JetStream
	Origin string // "ledger" or "audit"
}

func (d *JetStreamDeadLetter) Dead(ctx context.Context, rec Dead) error {
	h := nats.Header{}
	h.Set(HdrReason, string(rec.Reject.Reason))
	if rec.Reject.Err != nil {
		h.Set(HdrError, rec.Reject.Err.Error())
	}
	h.Set(HdrStream, rec.Stream)
	h.Set(HdrSubject, rec.Subject)
	h.Set(HdrSequence, strconv.FormatUint(rec.Sequence, 10))
	h.Set(HdrIndex, strconv.Itoa(rec.Reject.Index))

	// The message id makes a redelivery idempotent. A flush that
	// dead-letters and then fails to upload is naked and retried, and
	// without this the dead-letter stream would accumulate a copy of the
	// same bad record for every attempt — turning one publisher bug into
	// unbounded growth in the stream that is supposed to make it visible.
	msg := &nats.Msg{
		Subject: streams.DeadSubjectFor(d.Origin),
		Header:  h,
		Data:    rec.Reject.Payload,
	}
	_, err := d.JS.PublishMsg(ctx, msg, jetstream.WithMsgID(
		fmt.Sprintf("%s-%d-%d", rec.Stream, rec.Sequence, rec.Reject.Index)))
	return err
}
