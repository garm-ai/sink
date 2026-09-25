// Command garm-sink drains the garm record streams into a queryable lake.
//
// NATS JetStream in, micro-batched ZSTD Parquet on an S3-compatible store out,
// hive-partitioned so that DuckDB — and later Iceberg or DuckLake — reads it
// without being told anything.
//
// # Why this is its own binary and its own repository
//
// It links DuckDB, which is cgo and tens of megabytes of static library, and
// an S3 client. Neither belongs anywhere near a request path. Measured in the
// monorepo: folding this into the sidecar took it from 22.5 MB to 66.8 MB, and
// the sidecar runs beside every application replica while this runs in ones
// and twos.
//
// It also has no Go dependency on garmd, in either direction, and CI asserts
// it. garmd publishes to a stream; this consumes it. The contract between them
// is the wire format in github.com/garm-ai/garm and the subject names in its
// wire package — never an import. An import edge would make the daemon's build
// carry a Parquet writer, and would make this repository's release cadence the
// daemon's problem.
package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"

	"github.com/garm-ai/sink/internal/cli"
)

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "garm-sink:", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "garm-sink",
		Short: "Drain the garm record streams into a Parquet lake",
		Long: "garm-sink consumes the garm ledger and audit streams from NATS\n" +
			"JetStream and lands them as hive-partitioned ZSTD Parquet on an\n" +
			"S3-compatible store.\n\n" +
			"Delivery is at-least-once, so the lake holds duplicate rows by\n" +
			"design. Deduplicate on event_id when you read it.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newVersionCmd(), newProvisionCmd(), newDrainCmd())
	// The root refuses too. This binary runs unattended, and a parent that
	// prints help and exits 0 is a green deploy that moved no data.
	return cli.RequireSubcommand(root)
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version of this binary",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), version())
			return nil
		},
	}
}

func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return "dev"
	}
	return info.Main.Version
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
