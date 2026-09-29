package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	"github.com/garm-ai/garm/contracts/wire"
	"github.com/garm-ai/sink/internal/cli"
	"github.com/garm-ai/sink/internal/row"
	"github.com/garm-ai/sink/internal/tail"
)

// newTailCmd mirrors newDrainCmd: one subcommand per stream, because the two
// are framed differently and that is the stream's property, not a flag's.
func newTailCmd() *cobra.Command {
	t := &cobra.Command{
		Use:   "tail",
		Short: "Print a garm stream's rows as JSON lines, without consuming it",
		Long: "tail reads a stream through an ephemeral consumer that acknowledges\n" +
			"nothing and prints one JSON object per event, under the same column\n" +
			"names the lake uses. It is a debugging eye, not a drain: a running\n" +
			"drain's durable consumer and ack state are untouched.\n\n" +
			"error_detail may carry unsanitised free text; --no-detail drops it.",
	}
	t.AddCommand(
		newTailStreamCmd("ledger", wire.LedgerStream, wire.LedgerSubject+".>", row.EnvelopeBatch,
			"Tail GARM_LEDGER (messages are garm.ledger.v1.Batch)"),
		newTailStreamCmd("audit", wire.AuditStream, wire.AuditSubject+".>", row.EnvelopeEvent,
			"Tail GARM_AUDIT (messages are garm.ledger.v1.Event)"),
	)
	return cli.RequireSubcommand(t)
}

func newTailStreamCmd(origin, stream, subject string, envelope row.Envelope, short string) *cobra.Command {
	var (
		natsURL   string
		since     string
		fromStart bool
		filters   []string
		o         = tail.Options{Stream: stream, Subject: subject, Envelope: envelope}
	)
	cmd := &cobra.Command{
		Use:   origin,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start, err := tail.ParseSince(since)
			if err != nil {
				return err
			}
			start.FromStart = fromStart
			o.Start = start
			if o.Filter, err = tail.ParseFilters(filters); err != nil {
				return err
			}
			return runTail(cmd.Context(), natsURL, o, cmd)
		},
	}
	f := cmd.Flags()
	f.StringVar(&natsURL, "nats-url", envOr("NATS_URL", nats.DefaultURL), "NATS server URL")
	f.StringVar(&since, "since", fmt.Sprint(tail.DefaultLast),
		"where to start: a message count back from the end (20) or a duration back from now (10m)")
	f.BoolVar(&fromStart, "from-start", false, "start at the first message still in the stream")
	f.BoolVarP(&o.Follow, "follow", "f", false, "keep printing as messages arrive")
	f.StringArrayVar(&filters, "filter", nil, "only rows where column=value (repeatable; all must match)")
	f.BoolVar(&o.Pretty, "pretty", false, "indent each object")
	f.BoolVar(&o.NoDetail, "no-detail", false, "omit error_detail, the one column that may carry unsanitised text")
	return cmd
}

func runTail(ctx context.Context, natsURL string, o tail.Options, cmd *cobra.Command) error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return tail.Run(ctx, js, o, cmd.OutOrStdout(), cmd.ErrOrStderr())
}
