package row_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/garm-ai/contracts/garm/ledger/v1"
	"github.com/garm-ai/sink/internal/row"
)

func event(id string, when time.Time) *ledgerv1.Event {
	return &ledgerv1.Event{
		EventId:               id,
		Time:                  timestamppb.New(when),
		Tenant:                "acme",
		App:                   "svc",
		Feature:               "greeting",
		Tags:                  map[string]string{"k": "v"},
		PromptName:            "greeter",
		Alias:                 "fast",
		Usage:                 &ledgerv1.Usage{InputTokens: 80, OutputTokens: 4},
		Outcome:               "ok",
		PolicyViolations:      []string{"dlp:ssn"},
		Tool:                  "get_profile",
		PrincipalSubject:      "user:ada",
		PrincipalKind:         "PRINCIPAL_KIND_AGENT",
		ChainDepth:            2,
		ClearanceEffective:    "CLEARANCE_INTERNAL",
		CompartmentsEffective: []string{"financial", "pii-contact"},
		RedactionPlan:         "sha256:planhash",
		RedactionCount:        3,
		ErrorDetail:           "user with email ada@corp.com not found",
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

func TestAnEventBecomesEveryColumnItCarries(t *testing.T) {
	when := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	rows, bad := row.Decode(row.EnvelopeEvent, marshal(t, event("ev-1", when)))
	if len(bad) != 0 {
		t.Fatalf("rejects: %v", bad)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	r := rows[0]
	for _, c := range []struct {
		field     string
		got, want any
	}{
		{"EventID", r.EventID, "ev-1"},
		{"Time", r.Time, when},
		{"Tenant", r.Tenant, "acme"},
		{"App", r.App, "svc"},
		{"Feature", r.Feature, "greeting"},
		{"TagsJSON", r.TagsJSON, `{"k":"v"}`},
		{"InputTokens", r.InputTokens, int64(80)},
		{"Outcome", r.Outcome, "ok"},
		{"PolicyViolationsJSON", r.PolicyViolationsJSON, `["dlp:ssn"]`},
		{"Tool", r.Tool, "get_profile"},
		{"PrincipalSubject", r.PrincipalSubject, "user:ada"},
		{"PrincipalKind", r.PrincipalKind, "PRINCIPAL_KIND_AGENT"},
		{"ChainDepth", r.ChainDepth, int32(2)},
		{"ClearanceEffective", r.ClearanceEffective, "CLEARANCE_INTERNAL"},
		{"CompartmentsEffectiveJSON", r.CompartmentsEffectiveJSON, `["financial","pii-contact"]`},
		{"RedactionPlan", r.RedactionPlan, "sha256:planhash"},
		{"RedactionCount", r.RedactionCount, int32(3)},
		{"ErrorDetail", r.ErrorDetail, "user with email ada@corp.com not found"},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.field, c.got, c.want)
		}
	}
}

// PrincipalKind is the field the monorepo's Row predated. A column that is
// always empty because nothing maps into it looks identical, in the lake, to
// a field nobody ever sets.
func TestPrincipalKindReachesTheRow(t *testing.T) {
	ev := event("ev-kind", time.Now().UTC().Truncate(time.Second))
	ev.PrincipalKind = "PRINCIPAL_KIND_SERVICE"
	rows, _ := row.Decode(row.EnvelopeEvent, marshal(t, ev))
	if len(rows) != 1 || rows[0].PrincipalKind != "PRINCIPAL_KIND_SERVICE" {
		t.Fatalf("principal_kind did not reach the row: %+v", rows)
	}
}

// Empty repeated and map fields must be "[]" and "{}" and never "null": a
// column with three spellings of nothing needs three cases in every query
// written against it for the life of the lake.
func TestEmptyCollectionsAreEmptyJSONAndNeverNull(t *testing.T) {
	ev := &ledgerv1.Event{EventId: "e", Time: timestamppb.New(time.Now())}
	rows, bad := row.Decode(row.EnvelopeEvent, marshal(t, ev))
	if len(bad) != 0 {
		t.Fatalf("rejects: %v", bad)
	}
	if rows[0].TagsJSON != "{}" || rows[0].PolicyViolationsJSON != "[]" || rows[0].CompartmentsEffectiveJSON != "[]" {
		t.Fatalf("empty collections: tags=%q violations=%q compartments=%q",
			rows[0].TagsJSON, rows[0].PolicyViolationsJSON, rows[0].CompartmentsEffectiveJSON)
	}
}

func TestABatchBecomesOneRowPerEvent(t *testing.T) {
	when := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	b := &ledgerv1.Batch{Events: []*ledgerv1.Event{
		event("ev-1", when), event("ev-2", when), event("ev-3", when),
	}}
	rows, bad := row.Decode(row.EnvelopeBatch, marshal(t, b))
	if len(bad) != 0 {
		t.Fatalf("rejects: %v", bad)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
}

// The blast radius of one bad event is one event. When a message held one
// event this was the same statement; now that a ledger message holds hundreds,
// rejecting the message would discard hundreds of good records for one bad one.
func TestOneBadEventInABatchLosesOnlyThatEvent(t *testing.T) {
	when := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	good1, good2 := event("ev-1", when), event("ev-2", when)
	noID := event("", when)
	b := &ledgerv1.Batch{Events: []*ledgerv1.Event{good1, noID, good2}}

	rows, bad := row.Decode(row.EnvelopeBatch, marshal(t, b))
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want the two good ones", len(rows))
	}
	if len(bad) != 1 {
		t.Fatalf("got %d rejects, want 1: %v", len(bad), bad)
	}
	if bad[0].Reason != row.ReasonNoEventID {
		t.Errorf("reason = %q, want %q", bad[0].Reason, row.ReasonNoEventID)
	}
	if bad[0].Index != 1 {
		t.Errorf("index = %d, want 1 (its position in the batch)", bad[0].Index)
	}
	// The dead letter carries the event's bytes, not a description of them.
	var back ledgerv1.Event
	if err := proto.Unmarshal(bad[0].Payload, &back); err != nil {
		t.Fatalf("the rejected payload is not a readable Event: %v", err)
	}
	if back.GetTenant() != "acme" {
		t.Errorf("the rejected payload lost its content: %+v", &back)
	}
}

// An event with no timestamp has nowhere honest to go. The epoch buries it in
// a partition nobody reads and the ingest clock invents a time that is
// indistinguishable from a real one forever after.
func TestAnEventWithNoTimestampIsRejectedRatherThanBackdated(t *testing.T) {
	ev := event("ev-no-time", time.Time{})
	ev.Time = nil
	rows, bad := row.Decode(row.EnvelopeEvent, marshal(t, ev))
	if len(rows) != 0 {
		t.Fatalf("a timeless event became a row: %+v", rows)
	}
	if len(bad) != 1 || bad[0].Reason != row.ReasonNoTimestamp {
		t.Fatalf("rejects = %v, want one missing_time", bad)
	}
	if !errors.Is(bad[0].Err, row.ErrNoTimestamp) {
		t.Errorf("error does not wrap ErrNoTimestamp: %v", bad[0].Err)
	}
}

func TestAnUnparseableMessageIsOneRejectCarryingTheOriginalBytes(t *testing.T) {
	payload := []byte("\xff\xff not protobuf")
	rows, bad := row.Decode(row.EnvelopeBatch, payload)
	if len(rows) != 0 {
		t.Fatalf("rows from garbage: %+v", rows)
	}
	if len(bad) != 1 || bad[0].Reason != row.ReasonUnmarshal {
		t.Fatalf("rejects = %v, want one unmarshal", bad)
	}
	if bad[0].Index != -1 {
		t.Errorf("index = %d, want -1 for a whole-message failure", bad[0].Index)
	}
	if string(bad[0].Payload) != string(payload) {
		t.Errorf("the dead letter does not carry the original bytes")
	}
}

// protojson was the monorepo's encoding. A publisher still on it produces a
// stream of dead letters, and the reason has to name the actual problem —
// "cannot parse invalid wire-format data" sends whoever reads it looking for
// corruption.
func TestAJSONPayloadSaysSoRatherThanReadingAsCorruption(t *testing.T) {
	_, bad := row.Decode(row.EnvelopeEvent, []byte(`{"eventId":"ev-1"}`))
	if len(bad) != 1 {
		t.Fatalf("rejects = %v", bad)
	}
	if !strings.Contains(bad[0].Err.Error(), "JSON") {
		t.Errorf("the error does not name the encoding mismatch: %v", bad[0].Err)
	}
}

// An empty Batch is well-formed. It must produce nothing and complain about
// nothing — anything else and a publisher flushing an empty tick fills the
// dead-letter stream.
func TestAnEmptyBatchIsNeitherRowsNorRejects(t *testing.T) {
	rows, bad := row.Decode(row.EnvelopeBatch, marshal(t, &ledgerv1.Batch{}))
	if len(rows) != 0 || len(bad) != 0 {
		t.Fatalf("rows=%v rejects=%v; want neither", rows, bad)
	}
}

// The envelope is a property of the stream, not of the message. Decoding an
// audit Event as if it were a ledger Batch must not quietly produce a row, or
// the two streams could be swapped in configuration and nothing would say so.
func TestTheWrongEnvelopeFailsRatherThanInventingARow(t *testing.T) {
	when := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	rows, bad := row.Decode(row.EnvelopeBatch, marshal(t, event("ev-1", when)))
	if len(rows) != 0 {
		t.Fatalf("an Event decoded as a Batch produced rows: %+v", rows)
	}
	if len(bad) == 0 {
		t.Fatal("an Event decoded as a Batch produced neither rows nor rejects")
	}
}

// Columns is the schema the lake writes and tail prints. Every field of Row
// must be in it exactly once, and every column must read its own field: a
// field added to Row and not here is a value that never leaves the process,
// and two columns reading the same field is a shift-by-one nobody sees.
func TestEveryRowFieldIsExactlyOneColumn(t *testing.T) {
	var r row.Row
	v := reflect.ValueOf(&r).Elem()
	if v.NumField() != len(row.Columns) {
		t.Fatalf("Row has %d fields and Columns has %d entries", v.NumField(), len(row.Columns))
	}
	// Give every field a value unlike every other field's.
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(fmt.Sprintf("field-%d", i))
		case reflect.Int64, reflect.Int32:
			f.SetInt(int64(1000 + i))
		case reflect.Float64:
			f.SetFloat(float64(i) + 0.5)
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Struct: // time.Time
			f.Set(reflect.ValueOf(time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)))
		default:
			t.Fatalf("field %s has kind %s, which this test does not know how to fill", v.Type().Field(i).Name, f.Kind())
		}
	}
	seen := map[string]string{}
	for _, c := range row.Columns {
		got := fmt.Sprint(c.Get(r))
		if got == "" || got == "0" || got == "false" {
			t.Errorf("column %q reads a zero value from a fully populated row", c.Name)
		}
		if other, dup := seen[got]; dup && got != "true" {
			t.Errorf("columns %q and %q read the same field", other, c.Name)
		}
		seen[got] = c.Name
		if _, ok := row.ColumnNamed(c.Name); !ok {
			t.Errorf("ColumnNamed(%q) does not find its own column", c.Name)
		}
	}
	if _, ok := row.ColumnNamed("no_such_column"); ok {
		t.Error("ColumnNamed found a column that does not exist")
	}
}
