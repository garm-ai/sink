// Package drain is the consume loop: JetStream messages in, micro-batches out,
// acks only after the batch is durable somewhere else.
//
// The ack discipline is the whole contract with the publisher. A message is
// acked when its rows are in the lake, naked when they are not, and never
// acked because it was hard to read.
package drain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/garm-ai/sink/internal/row"
)

// Batch is one flush unit: rows plus the stream-sequence range they cover.
// The range feeds instance-unique Parquet file names, so an object can be
// traced back to the messages it came from.
//
// Not to be confused with garm.ledger.v1.Batch, which is how ONE message is
// framed. One of these holds the rows of many of those.
type Batch struct {
	Rows     []row.Row
	FirstSeq uint64
	LastSeq  uint64
}

// Sink lands a batch durably. Flush must be all-or-nothing from the caller's
// perspective: on error the whole batch is redelivered later, so sinks must
// tolerate duplicates. Consumers of the lake dedupe on event_id.
type Sink interface {
	Flush(ctx context.Context, b Batch) error
}

// Dead is one record that will never become a row, on its way out of the
// pipeline and into somewhere a person can find it.
type Dead struct {
	Stream   string // the stream it arrived on
	Subject  string
	Sequence uint64
	Reject   row.Reject
}

// DeadLetter is where unreadable records go.
//
// There is no nil default and no "log it and move on" implementation on
// purpose. The previous version of this code called msg.Term() on anything it
// could not parse: correct for metering, where an event is worth less than the
// cost of keeping it, and wrong for audit, where it permanently discards a
// record that something was legally obliged to keep and leaves a log line
// where the record was.
type DeadLetter interface {
	Dead(ctx context.Context, d Dead) error
}

type Config struct {
	// BatchMaxRows flushes at this many ROWS, not messages.
	//
	// It counted messages when one message was one event. It no longer is:
	// the ledger stream carries garm.ledger.v1.Batch, so one message holds
	// hundreds, and a limit of 1000 messages would produce Parquet files of
	// a million rows.
	BatchMaxRows int

	// BatchInterval flushes after this long with a non-empty batch.
	BatchInterval time.Duration

	// FetchMax is how many MESSAGES one Fetch asks for. It bounds how far
	// past BatchMaxRows a batch can overshoot — by up to FetchMax messages'
	// worth of rows — and it bounds how much is in flight and unacked.
	FetchMax int

	// FlushTimeout caps one Sink.Flush. It has to outlast a multi hundred
	// megabyte upload to a store having a bad minute, because a flush that
	// times out costs the whole batch a redelivery.
	FlushTimeout time.Duration
}

// The defaults exist to produce Parquet files worth querying.
//
// The old ones — 1000 rows every 30s, hive-partitioned by (date, app) —
// produce up to 2,880 files per day per app, each a few hundred kilobytes.
// Parquet's per-file overhead is the footer, the schema and one set of column
// statistics per row group; below roughly 100k rows that overhead dominates
// the data, every query pays a LIST over thousands of keys before it reads
// anything, and object stores charge per request. Over months the planning
// cost grows faster than the scan cost.
//
// 250k rows or five minutes gives files in the tens of megabytes at any real
// volume, and at low volume the interval still bounds how long a record waits
// before it is queryable. Five minutes is also the freshness anyone actually
// asks a lake for; a sub-minute answer is a job for the stream, not for this.
//
// The cost of the interval is honest: up to five minutes of rows sit unacked
// in JetStream. They are not lost — that is what unacked means — but the
// stream must be sized to hold them, which is what `sinkd provision`
// accounts for.
const (
	DefaultBatchMaxRows  = 250_000
	DefaultBatchInterval = 5 * time.Minute
	DefaultFetchMax      = 512
	DefaultFlushTimeout  = 5 * time.Minute
)

func (c *Config) defaults() {
	if c.BatchMaxRows <= 0 {
		c.BatchMaxRows = DefaultBatchMaxRows
	}
	if c.BatchInterval <= 0 {
		c.BatchInterval = DefaultBatchInterval
	}
	if c.FetchMax <= 0 {
		c.FetchMax = DefaultFetchMax
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = DefaultFlushTimeout
	}
}

// Drainer consumes one stream into one sink.
type Drainer struct {
	Consumer jetstream.Consumer
	Sink     Sink
	Dead     DeadLetter
	Envelope row.Envelope
	Cfg      Config
	Log      *slog.Logger
}

// pending is one message and what came out of it. A message can yield rows,
// rejects, or both — a batch of five hundred events with one bad event in it
// yields 499 rows and one reject, and loses nothing.
type pending struct {
	rows []row.Row
	bad  []row.Reject
	msg  jetstream.Msg
	seq  uint64
}

// Run consumes until ctx is cancelled, flushing on whichever comes first:
// BatchMaxRows rows, BatchInterval elapsed with a non-empty batch, or
// shutdown.
func (d *Drainer) Run(ctx context.Context) error {
	d.Cfg.defaults()
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Consumer == nil || d.Sink == nil {
		return errors.New("drain: Consumer and Sink are required")
	}
	// A nil DeadLetter would mean "discard what we cannot read", silently.
	// That is the failure this package was rewritten to remove, so it is a
	// refusal at startup rather than a surprise on the first bad message.
	if d.Dead == nil {
		return errors.New("drain: a DeadLetter is required; unreadable records are never discarded")
	}

	var (
		batch    []pending
		rowCount int
	)
	deadline := time.Now().Add(d.Cfg.BatchInterval)

	flush := func() {
		if len(batch) == 0 {
			deadline = time.Now().Add(d.Cfg.BatchInterval)
			return
		}
		// Shutdown must still flush, so the context is derived from a
		// background one: the ctx that just went away is the reason we are
		// flushing.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.Cfg.FlushTimeout)
		defer cancel()

		b, done := rowsOf(batch, rowCount), batch
		batch, rowCount = nil, 0
		deadline = time.Now().Add(d.Cfg.BatchInterval)

		// Dead letters go first. They are small and local to NATS, where the
		// Parquet flush is a multi megabyte round trip to an object store; a
		// dead-letter write that fails after a successful upload would cost
		// the whole batch a redelivery for the sake of one bad record.
		if err := d.deadLetter(fctx, done); err != nil {
			d.Log.Error("dead-letter write failed; nak for redelivery",
				"rows", len(b.Rows), "err", err)
			for _, p := range done {
				_ = p.msg.Nak()
			}
			return
		}
		// A message that produced nothing but rejects is now recorded
		// elsewhere, so it can be terminated. Only now: Term before the
		// dead-letter write succeeded is the discard this package exists to
		// avoid.
		live := done[:0:0]
		for _, p := range done {
			if len(p.rows) == 0 && len(p.bad) > 0 {
				_ = p.msg.Term()
				continue
			}
			live = append(live, p)
		}
		if len(b.Rows) > 0 {
			if err := d.Sink.Flush(fctx, b); err != nil {
				d.Log.Error("flush failed; nak for redelivery", "rows", len(b.Rows), "err", err)
				for _, p := range live {
					_ = p.msg.Nak()
				}
				return
			}
			d.Log.Info("flushed batch", "rows", len(b.Rows),
				"messages", len(done), "first_seq", b.FirstSeq, "last_seq", b.LastSeq)
		}
		// Everything not terminated is acked, including a message that
		// carried an empty Batch: it is readable, it is accounted for, and
		// leaving it unacked would redeliver it forever.
		for _, p := range live {
			_ = p.msg.Ack()
		}
	}

	for {
		if ctx.Err() != nil {
			flush()
			return nil
		}
		wait := time.Until(deadline)
		if len(batch) == 0 {
			wait = d.Cfg.BatchInterval // empty batch: no deadline pressure
		}
		if wait <= 0 {
			flush()
			continue
		}
		if wait > 2*time.Second {
			wait = 2 * time.Second // stay responsive to ctx cancellation
		}
		msgs, err := d.Consumer.Fetch(d.Cfg.FetchMax, jetstream.FetchMaxWait(wait))
		if err != nil {
			d.Log.Warn("fetch error", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for m := range msgs.Messages() {
			rows, bad := row.Decode(d.Envelope, m.Data())
			p := pending{rows: rows, bad: bad, msg: m}
			if md, err := m.Metadata(); err == nil {
				p.seq = md.Sequence.Stream
			}
			for _, r := range bad {
				d.Log.Error("unreadable record; dead-lettering",
					"subject", m.Subject(), "seq", p.seq, "reject", r.String())
			}
			batch = append(batch, p)
			rowCount += len(rows)
		}
		// A fetch that fails partway through delivers some messages and then
		// an error. Unchecked it is indistinguishable from an idle stream,
		// which is how a broker problem becomes "the lake stopped filling"
		// with nothing in the log.
		if err := msgs.Error(); err != nil {
			d.Log.Warn("fetch ended in error", "err", err)
		}
		if rowCount >= d.Cfg.BatchMaxRows || (len(batch) > 0 && time.Now().After(deadline)) {
			flush()
		}
	}
}

func rowsOf(batch []pending, rowCount int) Batch {
	b := Batch{Rows: make([]row.Row, 0, rowCount)}
	for _, p := range batch {
		b.Rows = append(b.Rows, p.rows...)
		if p.seq == 0 {
			continue
		}
		if b.FirstSeq == 0 || p.seq < b.FirstSeq {
			b.FirstSeq = p.seq
		}
		if p.seq > b.LastSeq {
			b.LastSeq = p.seq
		}
	}
	return b
}

func (d *Drainer) deadLetter(ctx context.Context, batch []pending) error {
	for _, p := range batch {
		for _, r := range p.bad {
			dead := Dead{Subject: p.msg.Subject(), Sequence: p.seq, Reject: r}
			if md, err := p.msg.Metadata(); err == nil {
				dead.Stream = md.Stream
			}
			if err := d.Dead.Dead(ctx, dead); err != nil {
				return fmt.Errorf("dead-letter %s seq %d: %w", dead.Subject, dead.Sequence, err)
			}
		}
	}
	return nil
}
