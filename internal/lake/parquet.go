// Package lake is the storage half: rows in, ZSTD Parquet on an
// S3-compatible store out.
//
// DuckDB writes the Parquet because it already knows how to produce a hive
// tree with the right compression and statistics, and because the same engine
// is what anyone will point at the result. It is cgo, which is exactly why it
// is here and not in anything with a request path.
package lake

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/garm-ai/sink/internal/row"
)

// columns is the table, in order, paired with the accessor for each.
//
// One list, used to build the DDL, the column list of the INSERT, and the
// argument slice. The previous version of this code had a positional
// `INSERT INTO ev VALUES (?,?,… ×39)` against a separately written CREATE
// TABLE: inserting one column in the middle of the DDL shifted every value
// after it into the wrong column, with no error anywhere — tenants would have
// appeared in the app column and the lake would have looked fine.
var columns = []struct {
	name string
	typ  string
	get  func(row.Row) any
}{
	{"event_id", "VARCHAR", func(r row.Row) any { return r.EventID }},
	{"time", "TIMESTAMP", func(r row.Row) any { return r.Time }},
	{"tenant", "VARCHAR", func(r row.Row) any { return r.Tenant }},
	{"app", "VARCHAR", func(r row.Row) any { return r.App }},
	{"feature", "VARCHAR", func(r row.Row) any { return r.Feature }},
	{"run_id", "VARCHAR", func(r row.Row) any { return r.RunID }},
	{"correlation_id", "VARCHAR", func(r row.Row) any { return r.CorrelationID }},
	{"causation_id", "VARCHAR", func(r row.Row) any { return r.CausationID }},
	{"budget_id", "VARCHAR", func(r row.Row) any { return r.BudgetID }},
	{"tags_json", "VARCHAR", func(r row.Row) any { return r.TagsJSON }},
	{"prompt_name", "VARCHAR", func(r row.Row) any { return r.PromptName }},
	{"prompt_hash", "VARCHAR", func(r row.Row) any { return r.PromptHash }},
	{"alias", "VARCHAR", func(r row.Row) any { return r.Alias }},
	{"resolved_model", "VARCHAR", func(r row.Row) any { return r.ResolvedModel }},
	{"model_overridden", "BOOLEAN", func(r row.Row) any { return r.ModelOverridden }},
	{"input_tokens", "BIGINT", func(r row.Row) any { return r.InputTokens }},
	{"output_tokens", "BIGINT", func(r row.Row) any { return r.OutputTokens }},
	{"cached_tokens", "BIGINT", func(r row.Row) any { return r.CachedTokens }},
	{"reasoning_tokens", "BIGINT", func(r row.Row) any { return r.ReasoningTokens }},
	{"cost_usd", "DOUBLE", func(r row.Row) any { return r.CostUSD }},
	{"cost_source", "VARCHAR", func(r row.Row) any { return r.CostSource }},
	{"latency_ms", "BIGINT", func(r row.Row) any { return r.LatencyMS }},
	{"provider_request_id", "VARCHAR", func(r row.Row) any { return r.ProviderRequestID }},
	{"fallback_used", "BOOLEAN", func(r row.Row) any { return r.FallbackUsed }},
	{"outcome", "VARCHAR", func(r row.Row) any { return r.Outcome }},
	{"error_kind", "VARCHAR", func(r row.Row) any { return r.ErrorKind }},
	{"policy_mode", "VARCHAR", func(r row.Row) any { return r.PolicyMode }},
	{"policy_violations", "VARCHAR", func(r row.Row) any { return r.PolicyViolationsJSON }},
	{"tool", "VARCHAR", func(r row.Row) any { return r.Tool }},
	{"principal_subject", "VARCHAR", func(r row.Row) any { return r.PrincipalSubject }},
	{"principal_actor", "VARCHAR", func(r row.Row) any { return r.PrincipalActor }},
	{"principal_kind", "VARCHAR", func(r row.Row) any { return r.PrincipalKind }},
	{"chain_depth", "INTEGER", func(r row.Row) any { return r.ChainDepth }},
	{"clearance_effective", "VARCHAR", func(r row.Row) any { return r.ClearanceEffective }},
	{"compartments_effective", "VARCHAR", func(r row.Row) any { return r.CompartmentsEffectiveJSON }},
	{"redaction_plan", "VARCHAR", func(r row.Row) any { return r.RedactionPlan }},
	{"redaction_count", "INTEGER", func(r row.Row) any { return r.RedactionCount }},
	{"disclosed_count", "INTEGER", func(r row.Row) any { return r.DisclosedCount }},
	{"error_detail", "VARCHAR", func(r row.Row) any { return r.ErrorDetail }},
	// date is the partition key, derived rather than carried, so that the
	// directory a row lands in can never disagree with the timestamp in it.
	{"date", "DATE", func(r row.Row) any { return r.Time.UTC().Truncate(24 * time.Hour) }},
}

// Columns names the Parquet schema, in order. Exported for the tests that
// assert the schema rather than restating it.
func Columns() []string {
	out := make([]string, len(columns))
	for i, c := range columns {
		out[i] = c.name
	}
	return out
}

// WriteParquet writes rows as ZSTD Parquet into dir, hive-partitioned by
// (date, app) — the layout DuckDB, and later Iceberg or DuckLake, reads
// natively. The partition columns live in the directory names.
func WriteParquet(dir string, rows []row.Row) error {
	if len(rows) == 0 {
		return nil
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	names := make([]string, len(columns))
	decls := make([]string, len(columns))
	holes := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.name
		decls[i] = c.name + " " + c.typ
		holes[i] = "?"
	}

	if _, err := db.Exec("CREATE TABLE ev (" + strings.Join(decls, ", ") + ")"); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	stmt, err := db.Prepare("INSERT INTO ev (" + strings.Join(names, ", ") +
		") VALUES (" + strings.Join(holes, ",") + ")")
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	args := make([]any, len(columns))
	for _, r := range rows {
		for i, c := range columns {
			args[i] = c.get(r)
		}
		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("insert row %s: %w", r.EventID, err)
		}
	}

	copySQL := fmt.Sprintf(
		`COPY (SELECT * FROM ev) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (date, app), OVERWRITE_OR_IGNORE)`,
		strings.ReplaceAll(dir, "'", "''"),
	)
	if _, err := db.Exec(copySQL); err != nil {
		return fmt.Errorf("copy to parquet: %w", err)
	}
	return nil
}
