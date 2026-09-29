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

// columns is the table, in order: the row's own column list from
// internal/row, each paired with its DuckDB type, plus the derived partition
// column.
//
// One list, used to build the DDL, the column list of the INSERT, and the
// argument slice. The previous version of this code had a positional
// `INSERT INTO ev VALUES (?,?,… ×39)` against a separately written CREATE
// TABLE: inserting one column in the middle of the DDL shifted every value
// after it into the wrong column, with no error anywhere — tenants would have
// appeared in the app column and the lake would have looked fine.
//
// The names and accessors are row.Columns rather than a second list here, so
// that what `sinkd tail` prints and what the lake stores are one schema
// by construction. Only the types are this package's to know.
type column struct {
	name string
	typ  string
	get  func(row.Row) any
}

var types = map[string]string{
	"event_id": "VARCHAR", "time": "TIMESTAMP", "tenant": "VARCHAR", "app": "VARCHAR",
	"feature": "VARCHAR", "run_id": "VARCHAR", "correlation_id": "VARCHAR",
	"causation_id": "VARCHAR", "budget_id": "VARCHAR", "tags_json": "VARCHAR",
	"prompt_name": "VARCHAR", "prompt_hash": "VARCHAR", "alias": "VARCHAR",
	"resolved_model": "VARCHAR", "model_overridden": "BOOLEAN",
	"input_tokens": "BIGINT", "output_tokens": "BIGINT", "cached_tokens": "BIGINT",
	"reasoning_tokens": "BIGINT", "cost_usd": "DOUBLE", "cost_source": "VARCHAR",
	"latency_ms": "BIGINT", "provider_request_id": "VARCHAR", "fallback_used": "BOOLEAN",
	"outcome": "VARCHAR", "error_kind": "VARCHAR", "policy_mode": "VARCHAR",
	"policy_violations": "VARCHAR", "tool": "VARCHAR", "principal_subject": "VARCHAR",
	"principal_actor": "VARCHAR", "principal_kind": "VARCHAR", "chain_depth": "INTEGER",
	"clearance_effective": "VARCHAR", "compartments_effective": "VARCHAR",
	"redaction_plan": "VARCHAR", "redaction_count": "INTEGER", "disclosed_count": "INTEGER",
	"error_detail": "VARCHAR", "execution_subject": "VARCHAR",
}

var columns = parquetColumns()

// parquetColumns pairs every row column with its type and appends the
// partition column. A row column with no type here is a programming error
// caught by any test run — and by the first `sinkd` invocation — rather
// than a column that quietly never reaches the lake.
func parquetColumns() []column {
	out := make([]column, 0, len(row.Columns)+1)
	for _, c := range row.Columns {
		typ, ok := types[c.Name]
		if !ok {
			panic(fmt.Sprintf("lake: row column %q has no Parquet type", c.Name))
		}
		out = append(out, column{c.Name, typ, c.Get})
	}
	// date is the partition key, derived rather than carried, so that the
	// directory a row lands in can never disagree with the timestamp in it.
	out = append(out, column{"date", "DATE", func(r row.Row) any {
		return r.Time.UTC().Truncate(24 * time.Hour)
	}})
	return out
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
