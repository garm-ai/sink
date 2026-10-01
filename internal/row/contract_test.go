package row_test

import (
	"sort"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	ledgerv1 "github.com/garm-ai/contracts/garm/ledger/v1"
	"github.com/garm-ai/sink/internal/row"
)

// This file is the check that `row.Columns` keeps up with the contract.
//
// `row.Columns` is an explicit list, not a walk over the descriptor, and that
// is deliberate: the lake's column names, order and types are this
// repository's decisions, not the proto's. The cost of the decision is that
// nothing fails when the contract grows a field the list has no entry for —
// which is how `execution_subject` sat on the wire, set by garmd, for nine
// releases and never reached a Parquet file. Nothing was broken and no test
// went red; the lake was simply missing an answer it should have had.
//
// So the descriptor is walked HERE instead, in a test, where the failure is a
// build failure and not a discovery someone makes months later against a
// partition. Every field of garm.ledger.v1.Event must be either a column or on
// an ignore list WITH A REASON — the reason is the point of the list, because
// "deliberately not in the lake" and "nobody noticed" look identical without
// one. The check runs in both directions: a contract field with no column
// fails, and a column with no contract field behind it fails too.
//
// Adding a column to a lake that already has files in it is still a decision
// rather than a consequence. What this removes is the option of not making it.

// eventColumns names the Event fields whose lake columns are not simply the
// field's own name. Everything absent from this map must be a column spelled
// exactly as the proto spells the field.
var eventColumns = map[string][]string{
	// tags is a map, flattened to JSON text in one column. The suffix is in
	// the column name so a reader knows to reach for json_extract.
	"tags": {"tags_json"},
	// usage is a nested message, flattened to its four counters rather than
	// written as a Parquet STRUCT: every engine can sum a BIGINT column.
	// Usage's own descriptor is walked by TestEveryUsageFieldIsAColumn, so a
	// fifth counter fails there rather than passing unnoticed here.
	"usage": {"input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens"},
}

// eventIgnored is the ignore list: an Event field that is deliberately NOT a
// lake column, and why. It is empty, and that is a statement rather than an
// oversight — every field of garm.ledger.v1.Event reaches the lake today.
//
// error_detail is the field that was considered for it and is not on it. The
// proto says the field "may carry unsanitized free text" and that "a lake that
// treats this column like the others has undone the redaction", so dropping it
// here looks like the cautious reading. It is the wrong one: the same comment
// says the detail lands on the ledger precisely so it is NOT destroyed,
// because destroying it is the pressure that makes someone log the error
// inside a handler and leak exactly what the scrubbing prevented. A sink that
// silently dropped it would move that pressure, not remove it. What the
// comment asks for is a governance grade — separate retention, separate
// access, or encryption at rest — and this repository provides none of it:
// error_detail is a VARCHAR beside the other thirty-nine. That is a real gap,
// it is in KNOWN-GAPS under its own heading, and it is not closed by an entry
// in this map. `sinkd tail --no-detail` is the only place the warning is
// honoured at all.
var eventIgnored = map[string]string{}

// batchIgnored is the same list for garm.ledger.v1.Batch. The Batch is the
// ledger stream's envelope, not a row: row.Decode iterates `events` and each
// one becomes a Row of its own. A field added to Batch — a publisher id, a
// flush timestamp, a schema version — would be read by no one and land
// nowhere, which is exactly the class of silence this file exists to break.
var batchIgnored = map[string]string{
	"events": "the envelope itself: Decode iterates it, and each Event becomes a Row",
}

// usageColumns: Usage's counters are columns under their own names.
var usageColumns = map[string][]string{}

var usageIgnored = map[string]string{}

// expectedColumns walks a message descriptor and returns every lake column its
// fields are expected to reach, failing the test for any field that is neither
// projected nor on the ignore list with a reason.
func expectedColumns(
	t *testing.T,
	d protoreflect.MessageDescriptor,
	projected map[string][]string,
	ignored map[string]string,
) []string {
	t.Helper()
	var want []string
	fields := d.Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())

		if reason, skip := ignored[name]; skip {
			if reason == "" {
				t.Errorf("%s.%s is on the ignore list with no reason beside it; "+
					"an entry without a reason cannot be told from an oversight", d.FullName(), name)
			}
			if _, isColumn := row.ColumnNamed(name); isColumn {
				t.Errorf("%s.%s is on the ignore list AND is a column %q; it is one or the other",
					d.FullName(), name, name)
			}
			continue
		}

		cols, ok := projected[name]
		if !ok {
			cols = []string{name}
		}
		for _, col := range cols {
			if _, found := row.ColumnNamed(col); !found {
				t.Errorf("%s.%s reaches no lake column: row.Columns has no %q. "+
					"Add the column, or add the field to the ignore list in this file WITH THE REASON.",
					d.FullName(), name, col)
				continue
			}
			want = append(want, col)
		}
	}
	return want
}

// The contract is the schema. A field on garm.ledger.v1.Event is a column, or
// it is on the ignore list with a reason, and there is no third state.
func TestEveryEventFieldIsAColumnOrDeliberatelyNot(t *testing.T) {
	d := (&ledgerv1.Event{}).ProtoReflect().Descriptor()
	want := expectedColumns(t, d, eventColumns, eventIgnored)

	// And the other way: a column with no contract field behind it is a
	// column nothing can ever fill. That is how a renamed field would show
	// up — the new name has no column, and the old column has no field.
	got := make([]string, 0, len(row.Columns))
	for _, c := range row.Columns {
		got = append(got, c.Name)
	}
	sort.Strings(want)
	sort.Strings(got)
	inWant := map[string]bool{}
	for _, c := range want {
		inWant[c] = true
	}
	for _, c := range got {
		if !inWant[c] {
			t.Errorf("column %q is in row.Columns and no field of %s projects into it",
				c, d.FullName())
		}
	}
	if len(want) != len(got) {
		t.Errorf("%s projects %d columns and row.Columns has %d:\n  contract: %v\n  lake:     %v",
			d.FullName(), len(want), len(got), want, got)
	}
}

// execution_subject is the instance the general check was written for. It is
// pinned by name as well, so that deleting it from row.Columns fails with the
// field's name in the message rather than only as an arithmetic mismatch.
func TestExecutionSubjectIsAColumn(t *testing.T) {
	if _, ok := row.ColumnNamed("execution_subject"); !ok {
		t.Fatal("execution_subject is on the wire, set by garmd's toolplane, and is not a lake column: " +
			"'which machine identity acted, on whose behalf' is then a question the stream answers and the lake cannot")
	}
	d := (&ledgerv1.Event{}).ProtoReflect().Descriptor()
	if f := d.Fields().ByName("execution_subject"); f == nil {
		t.Fatal("garm.ledger.v1.Event has no execution_subject field")
	} else if f.Number() != 71 {
		t.Errorf("execution_subject is field %d, want 71 — a renumbering is a wire break", f.Number())
	}
}

// Usage is flattened into four columns rather than written as a STRUCT, so its
// descriptor needs its own walk: a fifth counter added to the contract would
// satisfy the Event walk, which only checks the four names it names.
func TestEveryUsageFieldIsAColumn(t *testing.T) {
	d := (&ledgerv1.Usage{}).ProtoReflect().Descriptor()
	want := expectedColumns(t, d, usageColumns, usageIgnored)
	if len(want) != d.Fields().Len() {
		t.Errorf("%s has %d fields and %d reach columns", d.FullName(), d.Fields().Len(), len(want))
	}
	// The Event walk claims exactly these four. If Usage grows a counter,
	// this is the assertion that says so.
	if len(want) != len(eventColumns["usage"]) {
		t.Errorf("%s has %d fields but eventColumns[\"usage\"] names %d: %v",
			d.FullName(), len(want), len(eventColumns["usage"]), eventColumns["usage"])
	}
}

// Batch is projected as an envelope and nothing else. Its one field is the
// repeated Event; anything else added to it would be silently discarded by
// row.Decode, which reads `events` and nothing more.
func TestEveryBatchFieldIsTheEnvelopeOrDeliberatelyNot(t *testing.T) {
	d := (&ledgerv1.Batch{}).ProtoReflect().Descriptor()
	want := expectedColumns(t, d, nil, batchIgnored)
	if len(want) != 0 {
		t.Errorf("%s projects columns of its own: %v; a Batch is an envelope, not a row", d.FullName(), want)
	}
}
