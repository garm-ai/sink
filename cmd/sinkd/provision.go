package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	"github.com/garm-ai/sink/internal/streams"
)

// newProvisionCmd creates or verifies the three streams.
//
// It is a separate command and not something `drain` does on startup. A drain
// that provisioned would need permission to create streams in every
// deployment, and the first thing it would do on meeting a stream configured
// wrongly is decide, alone and at 3am, whether to change it.
func newProvisionCmd() *cobra.Command {
	var (
		natsURL                 string
		ledgerBytes, auditBytes int64
		deadBytes               int64
		ledgerAge               time.Duration
		replicas                int
		dryRun                  bool
	)
	cmd := &cobra.Command{
		Use:   "provision",
		Short: "Create or verify the ledger, audit and dead-letter streams",
		Long: "Creates each stream if it is absent and verifies it if it is present.\n\n" +
			"It never patches a stream that exists with a different policy: it\n" +
			"reports the difference and exits non-zero. An audit stream running\n" +
			"DiscardOld is data loss that still acks, and a deploy that silently\n" +
			"flipped it back would hide whoever changed it and why.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			want := []jetstream.StreamConfig{
				streams.LedgerConfig(streams.Sizing{MaxBytes: ledgerBytes, MaxAge: ledgerAge, Replicas: replicas}),
				streams.AuditConfig(streams.Sizing{MaxBytes: auditBytes, Replicas: replicas}),
				streams.DeadConfig(streams.Sizing{MaxBytes: deadBytes, Replicas: replicas}),
			}
			if dryRun {
				for _, w := range want {
					fmt.Fprintf(cmd.OutOrStdout(), "%s: subjects=%v discard=%v retention=%v max_bytes=%d max_age=%s replicas=%d deny_delete=%t\n",
						w.Name, w.Subjects, w.Discard, w.Retention, w.MaxBytes, w.MaxAge, w.Replicas, w.DenyDelete)
				}
				return nil
			}
			nc, err := nats.Connect(natsURL)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint:errcheck // nothing useful to do with a drain error on exit
			js, err := jetstream.New(nc)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			// Every stream is attempted even after one is refused, so that a
			// single run reports every difference. Stopping at the first turns
			// fixing three into three round trips.
			var refused []error
			for _, w := range want {
				st, err := streams.Provision(ctx, js, w)
				switch {
				case errors.Is(err, streams.ErrPolicyMismatch):
					refused = append(refused, err)
					fmt.Fprintf(cmd.OutOrStdout(), "%s: REFUSED\n", w.Name)
				case err != nil:
					return err
				case st.Created:
					fmt.Fprintf(cmd.OutOrStdout(), "%s: created\n", w.Name)
				default:
					fmt.Fprintf(cmd.OutOrStdout(), "%s: ok\n", w.Name)
				}
			}
			return errors.Join(refused...)
		},
	}
	f := cmd.Flags()
	f.StringVar(&natsURL, "nats-url", envOr("NATS_URL", nats.DefaultURL), "NATS server URL")
	f.Int64Var(&ledgerBytes, "ledger-max-bytes", 0, "ledger stream size limit in bytes (0 = 8GiB)")
	f.Int64Var(&auditBytes, "audit-max-bytes", 0, "audit stream size limit in bytes (0 = 8GiB); when it fills, publishes FAIL")
	f.Int64Var(&deadBytes, "dead-max-bytes", 0, "dead-letter stream size limit in bytes (0 = 1GiB)")
	f.DurationVar(&ledgerAge, "ledger-max-age", 0, "how long an undrained ledger record survives (0 = 7 days)")
	f.IntVar(&replicas, "replicas", 0, "stream replicas (0 = 1)")
	f.BoolVar(&dryRun, "dry-run", false, "print the configuration that would be applied and connect to nothing")
	return cmd
}
