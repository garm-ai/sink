package lake_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/garm-ai/sink/internal/lake"
	"github.com/garm-ai/sink/internal/row"
)

func openDuck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestTheHiveTreeIsWrittenAndReadsBackWithItsPartitionColumns(t *testing.T) {
	day1 := time.Date(2026, 9, 21, 8, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)
	rows := []row.Row{
		{EventID: "e1", Time: day1, Tenant: "acme", App: "svc-a", InputTokens: 10, Outcome: "ok", TagsJSON: "{}", Feature: "greeting"},
		{EventID: "e2", Time: day2, Tenant: "acme", App: "svc-a", InputTokens: 20, Outcome: "ok", TagsJSON: "{}"},
		{EventID: "e3", Time: day2, Tenant: "acme", App: "svc-b", InputTokens: 30, Outcome: "error", TagsJSON: "{}"},
	}
	dir := t.TempDir()
	if err := lake.WriteParquet(dir, rows); err != nil {
		t.Fatal(err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "date=2026-09-22", "app=svc-b", "*.parquet"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("expected partition dirs, glob err=%v matches=%v", err, matches)
	}

	db := openDuck(t)
	glob := filepath.Join(dir, "**", "*.parquet")
	var count, sumIn int
	if err := db.QueryRow(fmt.Sprintf(
		`SELECT count(*), sum(input_tokens) FROM read_parquet('%s', hive_partitioning=true)`, glob,
	)).Scan(&count, &sumIn); err != nil {
		t.Fatal(err)
	}
	if count != 3 || sumIn != 60 {
		t.Fatalf("count=%d sum=%d", count, sumIn)
	}
	var pruned int
	if err := db.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM read_parquet('%s', hive_partitioning=true) WHERE app='svc-a' AND date='2026-09-22'`, glob,
	)).Scan(&pruned); err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Fatalf("partition filter matched %d rows, want 1", pruned)
	}
}

// Every column holds the value of the field that belongs in it.
//
// This is the regression test for a positional `INSERT INTO ev VALUES (?,?,…)`
// against a separately written CREATE TABLE. Inserting one column in the
// middle of the DDL shifted every value after it by one — tenants in the app
// column, a redaction plan in the clearance column — with no error from
// anywhere, because every one of them is a VARCHAR. The sentinels are what
// make a shift of one visible: adjacent columns must not hold interchangeable
// values.
func TestEveryValueLandsInItsOwnColumn(t *testing.T) {
	when := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	r := row.Row{
		EventID:                   "v_event_id",
		Time:                      when,
		Tenant:                    "v_tenant",
		App:                       "v_app",
		Feature:                   "v_feature",
		RunID:                     "v_run_id",
		CorrelationID:             "v_correlation_id",
		CausationID:               "v_causation_id",
		BudgetID:                  "v_budget_id",
		TagsJSON:                  "v_tags_json",
		PromptName:                "v_prompt_name",
		PromptHash:                "v_prompt_hash",
		Alias:                     "v_alias",
		ResolvedModel:             "v_resolved_model",
		ModelOverridden:           true,
		InputTokens:               101,
		OutputTokens:              102,
		CachedTokens:              103,
		ReasoningTokens:           104,
		CostUSD:                   1.5,
		CostSource:                "v_cost_source",
		LatencyMS:                 105,
		ProviderRequestID:         "v_provider_request_id",
		FallbackUsed:              true,
		Outcome:                   "v_outcome",
		ErrorKind:                 "v_error_kind",
		PolicyMode:                "v_policy_mode",
		PolicyViolationsJSON:      "v_policy_violations",
		Tool:                      "v_tool",
		PrincipalSubject:          "v_principal_subject",
		PrincipalActor:            "v_principal_actor",
		PrincipalKind:             "v_principal_kind",
		ChainDepth:                7,
		ClearanceEffective:        "v_clearance_effective",
		CompartmentsEffectiveJSON: "v_compartments_effective",
		RedactionPlan:             "v_redaction_plan",
		RedactionCount:            8,
		DisclosedCount:            9,
		ErrorDetail:               "v_error_detail",
		ExecutionSubject:          "v_execution_subject",
	}
	want := map[string]string{
		"event_id": "v_event_id", "time": "2026-09-22 10:00:00 +0000 UTC",
		"tenant": "v_tenant", "app": "v_app", "feature": "v_feature",
		"run_id": "v_run_id", "correlation_id": "v_correlation_id",
		"causation_id": "v_causation_id", "budget_id": "v_budget_id",
		"tags_json": "v_tags_json", "prompt_name": "v_prompt_name",
		"prompt_hash": "v_prompt_hash", "alias": "v_alias",
		"resolved_model": "v_resolved_model", "model_overridden": "true",
		"input_tokens": "101", "output_tokens": "102", "cached_tokens": "103",
		"reasoning_tokens": "104", "cost_usd": "1.5", "cost_source": "v_cost_source",
		"latency_ms": "105", "provider_request_id": "v_provider_request_id",
		"fallback_used": "true", "outcome": "v_outcome", "error_kind": "v_error_kind",
		"policy_mode": "v_policy_mode", "policy_violations": "v_policy_violations",
		"tool": "v_tool", "principal_subject": "v_principal_subject",
		"principal_actor": "v_principal_actor", "principal_kind": "v_principal_kind",
		"chain_depth": "7", "clearance_effective": "v_clearance_effective",
		"compartments_effective": "v_compartments_effective",
		"redaction_plan":         "v_redaction_plan", "redaction_count": "8",
		"disclosed_count": "9", "error_detail": "v_error_detail",
		"execution_subject": "v_execution_subject",
		"date":              "2026-09-22 00:00:00 +0000 UTC",
	}

	dir := t.TempDir()
	if err := lake.WriteParquet(dir, []row.Row{r}); err != nil {
		t.Fatal(err)
	}

	cols := lake.Columns()
	// Every column is named here, so adding one to the writer and not to the
	// test fails rather than going unchecked.
	if len(cols) != len(want) {
		t.Fatalf("the writer has %d columns and this test names %d", len(cols), len(want))
	}
	db := openDuck(t)
	glob := filepath.Join(dir, "**", "*.parquet")
	q := "SELECT " + strings.Join(cols, ", ") +
		fmt.Sprintf(" FROM read_parquet('%s', hive_partitioning=true)", glob)
	res, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if !res.Next() {
		t.Fatalf("no row came back: %v", res.Err())
	}
	got := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range got {
		ptrs[i] = &got[i]
	}
	if err := res.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		w, named := want[c]
		if !named {
			t.Errorf("column %q is in the writer and not in this test", c)
			continue
		}
		if g := fmt.Sprint(got[i]); g != w {
			t.Errorf("column %q holds %q, want %q — a value landed in the wrong column", c, g, w)
		}
	}
}

// event_id is the dedupe key the whole at-least-once design rests on. A lake
// that does not carry it cannot be deduplicated at all.
func TestEventIDIsAColumn(t *testing.T) {
	for _, c := range lake.Columns() {
		if c == "event_id" {
			return
		}
	}
	t.Fatal("event_id is not a Parquet column; the lake cannot be deduplicated")
}

func TestWritingNoRowsWritesNothingRatherThanAnEmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := lake.WriteParquet(dir, nil); err != nil {
		t.Fatal(err)
	}
	found, err := filepath.Glob(filepath.Join(dir, "**", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Fatalf("an empty flush left files behind: %v", found)
	}
}

// The lake's schema is row.Columns plus the derived partition column, in
// that order. `sinkd tail` prints row.Columns, so this is the test that
// a line on a terminal and a row in the lake are one schema.
func TestTheParquetSchemaIsTheRowsColumnsPlusDate(t *testing.T) {
	got := lake.Columns()
	if len(got) != len(row.Columns)+1 {
		t.Fatalf("lake has %d columns, row has %d; want row's plus date", len(got), len(row.Columns))
	}
	for i, c := range row.Columns {
		if got[i] != c.Name {
			t.Errorf("column %d: lake %q, row %q", i, got[i], c.Name)
		}
	}
	if got[len(got)-1] != "date" {
		t.Errorf("last column is %q, want the partition column date", got[len(got)-1])
	}
}
