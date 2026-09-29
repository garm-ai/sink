// Package row turns a wire message into Parquet rows.
//
// The wire type is garm.ledger.v1.Event, from github.com/garm-ai/contracts.
// That module is the ONLY thing this repository and garmd share: garmd
// publishes, this drains, and neither imports the other. A Go import between
// them would put DuckDB and an S3 client in a request path's dependency graph,
// and would make the lake's release cadence the daemon's problem.
//
// The two streams are framed differently and that is a property of the
// streams, not something to sniff per message. The ledger carries
// garm.ledger.v1.Batch because a publisher that sends one JetStream message
// per call pays one in-flight ack per call; the audit stream carries a bare
// Event because a fail_closed write-ahead must be durable before the tool
// runs, and a batched write is by definition not yet written.
package row

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	ledgerv1 "github.com/garm-ai/contracts/garm/ledger/v1"
	"github.com/garm-ai/contracts/ledger"
)

// Envelope is how one stream's messages are framed.
type Envelope int

const (
	// EnvelopeBatch is the ledger stream: one message is a Batch of Events.
	EnvelopeBatch Envelope = iota
	// EnvelopeEvent is the audit stream: one message is one Event.
	EnvelopeEvent
)

func (e Envelope) String() string {
	if e == EnvelopeEvent {
		return "event"
	}
	return "batch"
}

// Row is the flattened, columnar form of an Event — one Parquet row.
//
// Repeated and map fields are flattened to JSON text rather than to Parquet
// LIST and MAP types. DuckDB reads both, but a string column survives a
// schema change in the contract without rewriting history, and every engine
// that will ever read this lake can filter on it with json_extract.
type Row struct {
	// EventID is the dedupe key, and the reason it is a first-class column.
	//
	// Delivery is at-least-once, so the lake WILL hold duplicate rows: a
	// flush that lands in S3 and then fails to ack is redelivered and
	// written again. Readers deduplicate with
	// `QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY time) = 1`,
	// or any equivalent. See README, "Duplicates are the design".
	EventID string
	Time    time.Time

	Tenant        string
	App           string
	Feature       string
	RunID         string
	CorrelationID string
	CausationID   string
	BudgetID      string
	TagsJSON      string

	PromptName      string
	PromptHash      string
	Alias           string
	ResolvedModel   string
	ModelOverridden bool

	InputTokens     int64
	OutputTokens    int64
	CachedTokens    int64
	ReasoningTokens int64
	CostUSD         float64
	CostSource      string
	LatencyMS       int64

	ProviderRequestID string
	FallbackUsed      bool
	Outcome           string
	ErrorKind         string

	PolicyMode           string
	PolicyViolationsJSON string

	Tool                      string
	PrincipalSubject          string
	PrincipalActor            string
	PrincipalKind             string
	ChainDepth                int32
	ClearanceEffective        string
	CompartmentsEffectiveJSON string
	RedactionPlan             string
	RedactionCount            int32
	DisclosedCount            int32

	// ErrorDetail is the one column here that may carry unsanitized free
	// text: a resolver error routinely interpolates the value it was
	// protecting. It inherits the retention and access grade of the most
	// sensitive field in the registry that produced it, and a lake that
	// treats this column like the others has undone the redaction.
	ErrorDetail string
}

// Reason is why one message, or one event inside one, could not become a row.
type Reason string

const (
	ReasonUnmarshal   Reason = "unmarshal"
	ReasonNoEventID   Reason = "missing_event_id"
	ReasonNoTimestamp Reason = "missing_time"
)

// Reject is an event that will never become a row however many times it is
// redelivered. It goes to the dead-letter stream, never to Term alone.
//
// Payload holds bytes, not a description: for a whole-message failure the
// message verbatim, and for one bad event inside a batch that event
// re-marshalled — proto.Marshal round-trips unknown fields, so nothing the
// publisher sent is lost on the way to the dead-letter stream. A record we
// could not read is still a record, and an audit trail with a hole in it and
// a log line where the hole is has failed at its only job.
type Reject struct {
	Reason Reason
	Err    error
	// Index is the event's position within a Batch, or -1 when the whole
	// message failed to parse.
	Index   int
	Payload []byte
}

func (r Reject) String() string {
	if r.Index < 0 {
		return fmt.Sprintf("%s: %v", r.Reason, r.Err)
	}
	return fmt.Sprintf("event %d: %s: %v", r.Index, r.Reason, r.Err)
}

// Decode turns one message into rows and rejects.
//
// It returns BOTH, and that is the point: one malformed event inside a batch
// of five hundred loses one event, not five hundred. The caller lands the
// rows and dead-letters the rejects, and acks only when both succeeded.
func Decode(env Envelope, data []byte) ([]Row, []Reject) {
	if env == EnvelopeEvent {
		var ev ledgerv1.Event
		if err := proto.Unmarshal(data, &ev); err != nil {
			return nil, []Reject{whole(data, err)}
		}
		r, err := FromProto(&ev)
		if err != nil {
			return nil, []Reject{{Reason: reasonOf(err), Err: err, Index: -1, Payload: data}}
		}
		return []Row{r}, nil
	}

	var b ledgerv1.Batch
	if err := proto.Unmarshal(data, &b); err != nil {
		return nil, []Reject{whole(data, err)}
	}
	events := b.GetEvents()
	rows := make([]Row, 0, len(events))
	var bad []Reject
	for i, ev := range events {
		r, err := FromProto(ev)
		if err != nil {
			bad = append(bad, Reject{
				Reason: reasonOf(err), Err: err, Index: i, Payload: remarshal(ev, data),
			})
			continue
		}
		rows = append(rows, r)
	}
	return rows, bad
}

// whole reports a message that could not be parsed at all.
//
// protojson is not attempted as a fallback. A stream whose encoding is
// "whichever of two the publisher felt like" has no encoding, and the failure
// mode of guessing is a message that parses as the wrong thing rather than an
// error anyone sees. The hint exists because a JSON payload here means a
// publisher is on the wrong contract, and that is worth naming exactly once
// in the dead-letter record rather than discovering from a hex dump.
func whole(data []byte, err error) Reject {
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		err = fmt.Errorf("%w (the payload looks like JSON; this stream carries binary garm.ledger.v1 protobuf)", err)
	}
	return Reject{Reason: ReasonUnmarshal, Err: err, Index: -1, Payload: data}
}

func remarshal(ev *ledgerv1.Event, fallback []byte) []byte {
	b, err := proto.Marshal(ev)
	if err != nil {
		return fallback
	}
	return b
}

var (
	// ErrNoEventID and ErrNoTimestamp are the two ways a well-formed Event is
	// still unusable.
	ErrNoEventID = errors.New("event_id is empty")

	// ErrNoTimestamp: the row would have to be partitioned under SOME date,
	// and the two candidates are both wrong. The epoch buries it in a
	// partition nobody queries; the ingest clock fabricates a timestamp that
	// is indistinguishable from a real one forever after — a row that claims
	// to have happened when the sink happened to read it. Refusing puts the
	// event somewhere it can be found and fixed instead.
	ErrNoTimestamp = errors.New("time is unset")
)

func reasonOf(err error) Reason {
	switch {
	case errors.Is(err, ErrNoEventID):
		return ReasonNoEventID
	case errors.Is(err, ErrNoTimestamp):
		return ReasonNoTimestamp
	default:
		return ReasonUnmarshal
	}
}

// FromProto flattens one Event.
//
// The field-by-field mapping goes through ledger.FromProto, the contract's own
// reader half, rather than reading the generated getters here. A field added
// to the contract and forgotten in one of two independent mappings is a column
// that is silently always empty; sharing the mapping with the producer's side
// of the same module leaves one place to forget it.
func FromProto(p *ledgerv1.Event) (Row, error) {
	ev := ledger.FromProto(p)
	if ev.ID == "" {
		return Row{}, ErrNoEventID
	}
	if ev.Time.IsZero() {
		return Row{}, ErrNoTimestamp
	}
	return Row{
		EventID: ev.ID,
		Time:    ev.Time.UTC(),

		Tenant:        ev.Tenant,
		App:           ev.App,
		Feature:       ev.Feature,
		RunID:         ev.RunID,
		CorrelationID: ev.CorrelationID,
		CausationID:   ev.CausationID,
		BudgetID:      ev.BudgetID,
		TagsJSON:      tagsJSON(ev.Tags),

		PromptName:      ev.PromptName,
		PromptHash:      ev.PromptHash,
		Alias:           ev.Alias,
		ResolvedModel:   ev.ResolvedModel,
		ModelOverridden: ev.ModelOverridden,

		InputTokens:     ev.Usage.InputTokens,
		OutputTokens:    ev.Usage.OutputTokens,
		CachedTokens:    ev.Usage.CachedTokens,
		ReasoningTokens: ev.Usage.ReasoningTokens,
		CostUSD:         ev.CostUSD,
		CostSource:      ev.CostSource,
		LatencyMS:       ev.LatencyMS,

		ProviderRequestID: ev.ProviderRequestID,
		FallbackUsed:      ev.FallbackUsed,
		Outcome:           string(ev.Outcome),
		ErrorKind:         ev.ErrorKind,

		PolicyMode:           ev.PolicyMode,
		PolicyViolationsJSON: listJSON(ev.PolicyViolations),

		Tool:                      ev.Tool,
		PrincipalSubject:          ev.PrincipalSubject,
		PrincipalActor:            ev.PrincipalActor,
		PrincipalKind:             ev.PrincipalKind,
		ChainDepth:                int32(ev.ChainDepth),
		ClearanceEffective:        ev.ClearanceEffective,
		CompartmentsEffectiveJSON: listJSON(ev.CompartmentsEffective),
		RedactionPlan:             ev.RedactionPlan,
		RedactionCount:            int32(ev.RedactionCount),
		DisclosedCount:            int32(ev.DisclosedCount),

		ErrorDetail: ev.ErrorDetail,
	}, nil
}

// tagsJSON and listJSON never return "null". A column that is sometimes
// empty, sometimes "null" and sometimes a value needs three cases in every
// query written against it for the rest of the lake's life.
//
// Map keys come out sorted, so two flushes of the same event produce byte
// identical rows and the duplicates a reader deduplicates really are
// identical.
func tagsJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func listJSON(v []string) string {
	if len(v) == 0 {
		return "[]"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// Column is one column of the lake's schema: its name, and how to read it
// from a Row.
type Column struct {
	Name string
	Get  func(Row) any
}

// Columns is the schema, in order — the ONE list of what a row is called
// when it leaves this process.
//
// The Parquet writer builds its DDL from it and `sinkd tail` prints keys
// from it, so a column added here is added to both, and a name can never be
// spelled one way in the lake and another way on a terminal. The partition
// column `date` is not here: the writer derives it from `time`, and a
// derived column belongs to the layout, not to the row.
var Columns = []Column{
	{"event_id", func(r Row) any { return r.EventID }},
	{"time", func(r Row) any { return r.Time }},
	{"tenant", func(r Row) any { return r.Tenant }},
	{"app", func(r Row) any { return r.App }},
	{"feature", func(r Row) any { return r.Feature }},
	{"run_id", func(r Row) any { return r.RunID }},
	{"correlation_id", func(r Row) any { return r.CorrelationID }},
	{"causation_id", func(r Row) any { return r.CausationID }},
	{"budget_id", func(r Row) any { return r.BudgetID }},
	{"tags_json", func(r Row) any { return r.TagsJSON }},
	{"prompt_name", func(r Row) any { return r.PromptName }},
	{"prompt_hash", func(r Row) any { return r.PromptHash }},
	{"alias", func(r Row) any { return r.Alias }},
	{"resolved_model", func(r Row) any { return r.ResolvedModel }},
	{"model_overridden", func(r Row) any { return r.ModelOverridden }},
	{"input_tokens", func(r Row) any { return r.InputTokens }},
	{"output_tokens", func(r Row) any { return r.OutputTokens }},
	{"cached_tokens", func(r Row) any { return r.CachedTokens }},
	{"reasoning_tokens", func(r Row) any { return r.ReasoningTokens }},
	{"cost_usd", func(r Row) any { return r.CostUSD }},
	{"cost_source", func(r Row) any { return r.CostSource }},
	{"latency_ms", func(r Row) any { return r.LatencyMS }},
	{"provider_request_id", func(r Row) any { return r.ProviderRequestID }},
	{"fallback_used", func(r Row) any { return r.FallbackUsed }},
	{"outcome", func(r Row) any { return r.Outcome }},
	{"error_kind", func(r Row) any { return r.ErrorKind }},
	{"policy_mode", func(r Row) any { return r.PolicyMode }},
	{"policy_violations", func(r Row) any { return r.PolicyViolationsJSON }},
	{"tool", func(r Row) any { return r.Tool }},
	{"principal_subject", func(r Row) any { return r.PrincipalSubject }},
	{"principal_actor", func(r Row) any { return r.PrincipalActor }},
	{"principal_kind", func(r Row) any { return r.PrincipalKind }},
	{"chain_depth", func(r Row) any { return r.ChainDepth }},
	{"clearance_effective", func(r Row) any { return r.ClearanceEffective }},
	{"compartments_effective", func(r Row) any { return r.CompartmentsEffectiveJSON }},
	{"redaction_plan", func(r Row) any { return r.RedactionPlan }},
	{"redaction_count", func(r Row) any { return r.RedactionCount }},
	{"disclosed_count", func(r Row) any { return r.DisclosedCount }},
	{"error_detail", func(r Row) any { return r.ErrorDetail }},
}

// ColumnNamed finds a column by its lake name.
func ColumnNamed(name string) (Column, bool) {
	for _, c := range Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}
