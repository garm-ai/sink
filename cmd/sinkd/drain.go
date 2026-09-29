package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	"github.com/garm-ai/contracts/wire"
	"github.com/garm-ai/sink/internal/cli"
	"github.com/garm-ai/sink/internal/drain"
	"github.com/garm-ai/sink/internal/lake"
	"github.com/garm-ai/sink/internal/row"
)

// instanceID is <hostname>-<pid> with dots replaced, so the value is safe in
// an object key. It takes its inputs rather than reading them so the
// degenerate hostnames are testable: Parquet file names depend on this value
// to stay unique across the instances sharing one work queue, which is what
// lets you run any number of sinks without leader election.
//
// An empty hostname becomes "unknown" rather than yielding a bare "-1234",
// which reads as a bug in every log line and object key it appears in.
func instanceID(host string, pid int) string {
	if host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", strings.ReplaceAll(host, ".", "_"), pid)
}

func defaultInstanceID() string {
	host, _ := os.Hostname()
	return instanceID(host, os.Getpid())
}

// newDrainCmd is a parent with no Run of its own. The two streams are
// different enough — different envelope, different failure policy, different
// durable — that one command with a --stream flag would be a switch statement
// wearing a flag's clothes.
func newDrainCmd() *cobra.Command {
	d := &cobra.Command{
		Use:   "drain",
		Short: "Consume a garm stream into the lake",
	}
	d.AddCommand(
		newDrainStreamCmd("ledger", wire.LedgerStream, wire.LedgerSubject+".>", row.EnvelopeBatch,
			"Batch the ledger stream into hive-partitioned Parquet"),
		newDrainStreamCmd("audit", wire.AuditStream, wire.AuditSubject+".>", row.EnvelopeEvent,
			"Batch the audit stream into hive-partitioned Parquet"),
	)
	return cli.RequireSubcommand(d)
}

type drainOpts struct {
	origin      string
	stream      string
	subject     string
	envelope    row.Envelope
	natsURL     string
	durable     string
	lakeDir     string
	s3Endpoint  string
	s3Bucket    string
	s3SSL       bool
	s3Anonymous bool
	keyPrefix   string
	instance    string
	cfg         drain.Config
}

// retiredEnv names the variables this command used to read and what replaced
// them. They are gone rather than deprecated: the whole cost of the old names
// was that a correctly configured machine looked like a broken store, and a
// second spelling kept alive for one release keeps that ambiguity alive with
// it. The repository is pre-1.0 and its consumers pin a tag, so the break
// costs a line in a compose file rather than a migration.
//
// What is NOT gone is the diagnosis. A run started with only the old names set
// is refused at startup by a message that names the variable to export, which
// is the one thing the old failure never did.
var retiredEnv = []struct{ old, replacement string }{
	{"S3_ACCESS_KEY", lake.EnvAccessKey},
	{"S3_SECRET_KEY", lake.EnvSecretKey},
	{"S3_ENDPOINT", lake.EnvEndpoint},
}

// retiredEnvError refuses a machine configured the old way, naming the
// variable that replaced each one. It is given the names that could have
// changed the outcome of the call it guards, because a variable that would
// have been ignored anyway is not worth refusing over: an explicit
// --s3-endpoint settles the endpoint whatever S3_ENDPOINT says.
//
// It fires only when the old name is set and the new one is not, because that
// is exactly the case where the old value was the operator's whole intent. A
// machine with both set is already configured correctly for every other tool
// on it, and a drain has no business refusing to start over a leftover it now
// ignores.
func retiredEnvError(names ...string) error {
	var said []string
	for _, r := range retiredEnv {
		if !slices.Contains(names, r.old) {
			continue
		}
		if os.Getenv(r.old) != "" && os.Getenv(r.replacement) == "" {
			said = append(said, fmt.Sprintf("%s is no longer read: export %s instead", r.old, r.replacement))
		}
	}
	if said == nil {
		return nil
	}
	return errors.New(strings.Join(said, "; "))
}

// s3ConfigFromEnv reads the AWS-standard variables.
//
// AWS_REGION wins over AWS_DEFAULT_REGION because that is the precedence every
// SDK and the CLI use, and a region that resolved differently here than in the
// `aws s3 ls` someone ran to check the bucket would be its own afternoon.
func s3ConfigFromEnv(o drainOpts) lake.S3Config {
	return lake.S3Config{
		Endpoint:  o.s3Endpoint,
		Bucket:    o.s3Bucket,
		AccessKey: os.Getenv(lake.EnvAccessKey),
		SecretKey: os.Getenv(lake.EnvSecretKey),
		// Empty for a long-lived key, and required by the store for a
		// temporary one.
		SessionToken: os.Getenv(lake.EnvSessionToken),
		Region:       envOr(lake.EnvRegion, os.Getenv(lake.EnvDefaultRegion)),
		UseSSL:       o.s3SSL,
		Anonymous:    o.s3Anonymous,
	}
}

// destinationOf turns the destination flags into one destination, or into the
// error that says which flags were wrong.
//
// There is no default destination on purpose. It used to be 127.0.0.1:9000 and
// a bucket named garm-lake, which meant that `sinkd drain ledger` with a
// forgotten flag started, connected to nothing, and reported a connection
// error five minutes into the first flush. Now there are two destinations and
// no way to guess which one was meant, so the command refuses at startup and
// names both.
//
// The credentials are read here and only on the S3 branch: a lake on disk has
// nothing to authenticate to, and reading AWS_ACCESS_KEY_ID for it would make
// an unset variable look relevant to a failure that has nothing to do with it.
// The refusal over the retired names follows the same rule — but it also
// covers the no-destination case, because a machine that set only S3_ENDPOINT
// used to get a destination out of it and would otherwise now be told it named
// no destination at all, which is true and useless.
func destinationOf(o drainOpts) (lake.Destination, error) {
	dir, s3 := o.lakeDir != "", o.s3Endpoint != "" || o.s3Bucket != ""
	switch {
	case dir && s3:
		return nil, errors.New("--lake-dir and --s3-endpoint/--s3-bucket name two different destinations; pass one of them")
	case !dir && !s3:
		// Every retired name is worth naming here: with no flags at all, any
		// of them was the operator's whole configuration.
		if err := retiredEnvError("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY"); err != nil {
			return nil, err
		}
		return nil, errors.New("no destination: pass --lake-dir <path> for a lake on this machine, " +
			"or --s3-endpoint and --s3-bucket for an object store")
	case dir:
		return lake.NewDirStore(o.lakeDir)
	case o.s3Endpoint == "":
		return nil, errors.New("--s3-bucket without --s3-endpoint: there is no store to put the bucket on")
	case o.s3Bucket == "":
		return nil, errors.New("--s3-endpoint without --s3-bucket: there is no bucket to put the objects in")
	}
	if err := retiredEnvError("S3_ACCESS_KEY", "S3_SECRET_KEY"); err != nil {
		return nil, err
	}
	return lake.NewUploader(s3ConfigFromEnv(o))
}

func newDrainStreamCmd(origin, stream, subject string, envelope row.Envelope, short string) *cobra.Command {
	o := drainOpts{origin: origin, stream: stream, subject: subject, envelope: envelope}
	cmd := &cobra.Command{
		Use:   origin,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if o.instance == "" {
				o.instance = defaultInstanceID()
			}
			return runDrain(cmd.Context(), o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.natsURL, "nats-url", envOr("NATS_URL", nats.DefaultURL), "NATS server URL")
	// The durable consumer name is a thing in NATS, not in this binary.
	// Renaming it orphans the existing consumer and replays the stream.
	f.StringVar(&o.durable, "durable", "sink-"+origin, "durable consumer name")
	// The two destinations are mutually exclusive and neither is a default.
	// --lake-dir is the whole lake on this machine: the same Parquet under the
	// same keys, as paths, for a developer with no object store.
	f.StringVar(&o.lakeDir, "lake-dir", envOr("LAKE_DIR", ""), "write the lake to this directory instead of S3")
	// The S3 side reads the AWS-standard variables, so a machine already set
	// up for the AWS CLI, an SDK or DuckDB's httpfs is already set up for
	// this. The exception is the bucket: AWS addresses one in the URL and
	// defines no variable for it, so S3_BUCKET is ours and the help says so.
	f.StringVar(&o.s3Endpoint, "s3-endpoint", envOr(lake.EnvEndpoint, ""),
		"S3 endpoint, host:port or URL (env "+lake.EnvEndpoint+")")
	f.StringVar(&o.s3Bucket, "s3-bucket", envOr("S3_BUCKET", ""),
		"S3 bucket (env S3_BUCKET, which is ours: AWS has no standard variable for a bucket)")
	f.BoolVar(&o.s3SSL, "s3-ssl", false, "use TLS for S3; an https:// endpoint says it too")
	f.BoolVar(&o.s3Anonymous, "s3-anonymous", false,
		"send unsigned requests, for a store with no IAM, instead of "+lake.EnvAccessKey+"/"+lake.EnvSecretKey)
	f.StringVar(&o.keyPrefix, "key-prefix", origin, "object key prefix")
	f.StringVar(&o.instance, "instance-id", "", "unique instance id (default hostname-pid)")
	f.IntVar(&o.cfg.BatchMaxRows, "batch-max-rows", drain.DefaultBatchMaxRows,
		"flush after this many ROWS; one ledger message holds many")
	f.DurationVar(&o.cfg.BatchInterval, "batch-interval", drain.DefaultBatchInterval,
		"flush after this long with a non-empty batch")
	f.IntVar(&o.cfg.FetchMax, "fetch-max", drain.DefaultFetchMax, "messages per JetStream fetch")
	f.DurationVar(&o.cfg.FlushTimeout, "flush-timeout", drain.DefaultFlushTimeout, "cap on one flush")
	return cmd
}

func runDrain(ctx context.Context, o drainOpts) error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("origin", o.origin)

	// The destination is settled before anything is connected to. A flag
	// mistake should cost a line on stderr, not a durable consumer and a
	// five-minute batch that fails at the first flush.
	dest, err := destinationOf(o)
	if err != nil {
		return err
	}

	nc, err := nats.Connect(o.natsURL, nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	defer nc.Drain() //nolint:errcheck // nothing useful to do with a drain error on exit
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cons, err := js.CreateOrUpdateConsumer(ctx, o.stream, jetstream.ConsumerConfig{
		Durable:       o.durable,
		FilterSubject: o.subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait(o.cfg),
		// In messages, where the batch limit is in rows. It bounds how much
		// this instance holds unacked, which is what lets several of them
		// share one work queue without one hoarding the stream.
		MaxAckPending: 4 * o.cfg.FetchMax,
	})
	if err != nil {
		return fmt.Errorf("consumer on stream %s: %w", o.stream, err)
	}

	if err := dest.Ensure(ctx); err != nil {
		return err
	}

	logger.Info("sinkd drain started",
		"nats", o.natsURL, "stream", o.stream, "durable", o.durable,
		"destination", dest.Describe(), "key_prefix", o.keyPrefix, "instance", o.instance,
		"batch_max_rows", o.cfg.BatchMaxRows, "batch_interval", o.cfg.BatchInterval.String(),
		"ack_wait", ackWait(o.cfg).String())

	d := &drain.Drainer{
		Consumer: cons,
		Sink:     &lake.ParquetSink{Dest: dest, KeyPrefix: o.keyPrefix, InstanceID: o.instance},
		Dead:     &drain.JetStreamDeadLetter{JS: js, Origin: o.origin},
		Envelope: o.envelope,
		Cfg:      o.cfg,
		Log:      logger,
	}
	if err := d.Run(ctx); err != nil {
		return err
	}
	logger.Info("sinkd drain stopped")
	return nil
}

// ackWait must outlast a whole cycle: a batch can sit for BatchInterval and
// then spend FlushTimeout being written.
//
// Left at some fixed two minutes it would expire mid-flush under the default
// five-minute interval, JetStream would redeliver every message in the batch
// to another instance, and both would write the same rows — the duplicates the
// lake tolerates, produced continuously rather than on failure, at twice the
// object count and twice the cost. The extra minute covers fetch latency and
// the batch that starts filling just before the interval expires.
func ackWait(cfg drain.Config) time.Duration {
	c := cfg
	if c.BatchInterval <= 0 {
		c.BatchInterval = drain.DefaultBatchInterval
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = drain.DefaultFlushTimeout
	}
	return c.BatchInterval + c.FlushTimeout + time.Minute
}
