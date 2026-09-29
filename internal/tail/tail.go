// Package tail is the debugging eye on a stream: decoded rows, one JSON line
// per event, straight to a terminal.
//
// It exists because the first real-model runs against the compose plane
// produced a refusal whose reason was on the ledger and nowhere else, and the
// only way to read the ledger was to drain it into Parquet and query the
// lake. Half a day to read one row. This reads it in a second.
//
// It is a reader and only a reader. The consumer it creates is ephemeral,
// has no durable name, and acknowledges nothing — AckNone at the broker, so
// there is no ack to forget. A drain's durable consumer and its ack floor are
// exactly as they were when tail exits, and the test that says so runs
// against a real broker.
//
// What it prints is the row, under the names the lake uses: row.Columns is
// the one list the Parquet writer and this share, so a column is never
// spelled one way in DuckDB and another on a terminal.
package tail

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/garm-ai/sink/internal/row"
)

// Options is one tail.
type Options struct {
	Stream   string
	Subject  string
	Envelope row.Envelope

	// Start is where reading begins. See ParseSince.
	Start Start

	// Follow keeps reading after the stream's current end instead of
	// stopping there.
	Follow bool

	// Filter is column name to required value, matched on every row after
	// it is decoded. Client-side on purpose: the stream is not indexed on
	// any of these, and a subject filter would only reach tenant and app.
	Filter map[string]string

	// Pretty indents each object. One event is then many lines, which is
	// what you want when you are reading one and not what you want when you
	// are piping into jq.
	Pretty bool

	// NoDetail omits error_detail. That column is the one that may carry
	// unsanitised free text — a resolver error routinely interpolates the
	// value it was protecting — and a terminal is the least governed place
	// a row can land.
	NoDetail bool

	// FetchWait bounds one wait for a message. Zero means two seconds,
	// which keeps the loop responsive to cancellation.
	FetchWait time.Duration
}

// Start says where in the stream to begin.
//
// Exactly one of the three applies: FromStart, else Age when it is non-zero,
// else Last. The zero value is the last hundred messages, which for the
// ledger — one message is a Batch of many events — is usually more than
// enough to see what just happened and not so much that it scrolls away.
type Start struct {
	FromStart bool
	Last      int
	Age       time.Duration
}

// DefaultLast is how many messages back a tail starts when nothing says
// otherwise.
const DefaultLast = 100

// ParseSince reads the --since flag: a bare integer is a count of messages
// back from the end, anything else is a duration back from now.
//
// One flag for both because they answer the same question — "how far back?"
// — and a person at a terminal thinks in whichever unit the moment calls
// for: "the last twenty" after a test run, "the last ten minutes" after a
// page.
func ParseSince(s string) (Start, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Start{Last: DefaultLast}, nil
	}
	if n, err := strconv.Atoi(s); err == nil {
		if n <= 0 {
			return Start{}, fmt.Errorf("--since %d: a message count must be positive", n)
		}
		return Start{Last: n}, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return Start{}, fmt.Errorf("--since %q: neither a message count nor a duration", s)
	}
	if d <= 0 {
		return Start{}, fmt.Errorf("--since %s: a duration must be positive", d)
	}
	return Start{Age: d}, nil
}

// ParseFilters reads repeated --filter key=value flags and refuses a key the
// row does not have. A misspelled column is not "no matches", which would
// read as "nothing happened"; it is an error with the column list in it.
func ParseFilters(kv []string) (map[string]string, error) {
	if len(kv) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(kv))
	for _, f := range kv {
		k, v, ok := strings.Cut(f, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("--filter %q: want key=value", f)
		}
		if _, known := row.ColumnNamed(k); !known {
			return nil, fmt.Errorf("--filter %q: no column %q; columns are %s", f, k, strings.Join(columnNames(), ", "))
		}
		out[k] = v
	}
	return out, nil
}

func columnNames() []string {
	names := make([]string, len(row.Columns))
	for i, c := range row.Columns {
		names[i] = c.Name
	}
	return names
}

// ConsumerConfig is the consumer a tail creates, given the stream as it is
// now. Pure, so the properties that make this a reader and not a drain —
// no durable, no name, AckNone — are asserted in a test rather than trusted.
func ConsumerConfig(o Options, state jetstream.StreamState) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{
		FilterSubject: o.Subject,
		AckPolicy:     jetstream.AckNonePolicy,
		// The broker deletes it this long after the last fetch. Run deletes
		// it on exit too; this is for the exit that never runs.
		InactiveThreshold: 30 * time.Second,
		Description:       "garm-sink tail: ephemeral, acks nothing",
	}
	switch {
	case o.Start.FromStart:
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case o.Start.Age > 0:
		t := time.Now().Add(-o.Start.Age)
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		cfg.OptStartTime = &t
	default:
		last := o.Start.Last
		if last <= 0 {
			last = DefaultLast
		}
		// Sequences are inclusive, so the last N messages start at
		// LastSeq-N+1, clamped to the first message still in the stream.
		// An empty stream has FirstSeq 0 and no valid start sequence at
		// all; DeliverAll is the same thing said in a way the broker
		// accepts.
		if state.LastSeq == 0 {
			cfg.DeliverPolicy = jetstream.DeliverAllPolicy
			break
		}
		start := uint64(1)
		if uint64(last) < state.LastSeq {
			start = state.LastSeq - uint64(last) + 1
		}
		if start < state.FirstSeq {
			start = state.FirstSeq
		}
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = start
	}
	return cfg
}

// Run tails one stream into out until it reaches the end (or, with Follow,
// until ctx is cancelled). Messages that will not decode are reported on
// errOut, one line each, and skipped; they are somebody's dead letters, not
// this command's problem.
func Run(ctx context.Context, js jetstream.JetStream, o Options, out, errOut io.Writer) error {
	if o.Stream == "" || o.Subject == "" {
		return errors.New("tail: Stream and Subject are required")
	}
	for k := range o.Filter {
		if _, ok := row.ColumnNamed(k); !ok {
			return fmt.Errorf("tail: no column %q", k)
		}
	}
	wait := o.FetchWait
	if wait <= 0 {
		wait = 2 * time.Second
	}

	st, err := js.Stream(ctx, o.Stream)
	if err != nil {
		return fmt.Errorf("stream %s: %w", o.Stream, err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		return fmt.Errorf("stream %s: %w", o.Stream, err)
	}

	cons, err := st.CreateConsumer(ctx, ConsumerConfig(o, info.State))
	if err != nil {
		return fmt.Errorf("consumer on %s: %w", o.Stream, err)
	}
	created := cons.CachedInfo()
	// Deleted on the way out with a context that survives cancellation:
	// the ctx that just ended is the reason we are leaving.
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = st.DeleteConsumer(dctx, created.Name)
	}()
	ack := "none"
	if created.Config.AckPolicy != jetstream.AckNonePolicy {
		ack = created.Config.AckPolicy.String()
	}
	fmt.Fprintf(errOut, "garm-sink tail: stream=%s consumer=%s durable=%t ack=%s pending=%d follow=%t\n",
		o.Stream, created.Name, created.Config.Durable != "", ack, created.NumPending, o.Follow)

	if !o.Follow && created.NumPending == 0 {
		return nil
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(wait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) ||
				errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("reading %s: %w", o.Stream, err)
		}
		var seq, pending uint64
		if md, err := msg.Metadata(); err == nil {
			seq, pending = md.Sequence.Stream, md.NumPending
		}
		rows, bad := row.Decode(o.Envelope, msg.Data())
		for _, r := range bad {
			fmt.Fprintf(errOut, "garm-sink tail: %s seq %d: unreadable: %s\n", msg.Subject(), seq, r)
		}
		for _, r := range rows {
			if !matches(r, o.Filter) {
				continue
			}
			line, err := Encode(r, o.Pretty, o.NoDetail)
			if err != nil {
				return err
			}
			if _, err := out.Write(line); err != nil {
				return err
			}
		}
		if !o.Follow && pending == 0 {
			return nil
		}
	}
}

// matches applies every filter. A filter value is compared against the
// column's text form: strings as they are, numbers and booleans as Go prints
// them, times as RFC 3339.
func matches(r row.Row, filter map[string]string) bool {
	for k, want := range filter {
		c, ok := row.ColumnNamed(k)
		if !ok || text(c.Get(r)) != want {
			return false
		}
	}
	return true
}

func text(v any) string {
	if t, ok := v.(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprint(v)
}

// Encode is one row as one JSON object, keys in row.Columns order, with a
// trailing newline. Hand-assembled rather than marshalled from a map so the
// key order is the lake's column order and not alphabetical: a person
// reading the line finds event_id and time first, every time.
func Encode(r row.Row, pretty, noDetail bool) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	first := true
	for _, c := range row.Columns {
		if noDetail && c.Name == "error_detail" {
			continue
		}
		v, err := json.Marshal(c.Get(r))
		if err != nil {
			return nil, fmt.Errorf("encoding %s: %w", c.Name, err)
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteByte('"')
		b.WriteString(c.Name)
		b.WriteString(`":`)
		b.Write(v)
	}
	b.WriteByte('}')
	if pretty {
		var ind bytes.Buffer
		if err := json.Indent(&ind, b.Bytes(), "", "  "); err != nil {
			return nil, err
		}
		b = ind
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

// FilterKeys is the sorted filter set, for a log line.
func FilterKeys(f map[string]string) []string {
	out := make([]string, 0, len(f))
	for k, v := range f {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
