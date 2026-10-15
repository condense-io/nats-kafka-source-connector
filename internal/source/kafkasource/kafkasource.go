// Package kafkasource implements source.Source for Kafka, using franz-go's
// consumer group.
//
// Offsets are never auto-committed: the pipeline commits them explicitly,
// and only once every record up to that offset has been acked by
// JetStream. Rebalances are blocked from the moment a batch is polled until
// the pipeline calls AllowNext (after that commit), so a partition can never
// be handed to another member while this one still has unpublished or
// uncommitted records from it. The combination makes this source
// at-least-once: a crash between publish and commit re-delivers those
// records to whichever member owns the partition next.
package kafkasource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config"
	"condense.zeliot.in/nats-kafka-source-connector/internal/source"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Source struct {
	client         *kgo.Client
	maxPollRecords int
	maxPollWait    time.Duration
}

func New(cfg config.KafkaSourceConfig) (*Source, error) {
	client, err := newKafkaClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafkasource: %w", err)
	}

	return &Source{
		client:         client,
		maxPollRecords: cfg.KafkaMaxPollRecords,
		maxPollWait:    cfg.KafkaMaxPollWait,
	}, nil
}

// defaultBrokerMaxReadBytes is franz-go's own default for the largest
// response it will read from a broker.
const defaultBrokerMaxReadBytes = 100 << 20

func newKafkaClient(cfg config.KafkaSourceConfig) (*kgo.Client, error) {
	resetOffset := kgo.NewOffset().AtStart()
	if cfg.KafkaConsumeFromOffset == "latest" {
		resetOffset = kgo.NewOffset().AtEnd()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfg.KafkaBootstrapServers, ",")...),
		kgo.ClientID(cfg.KafkaConsumerClientId),
		kgo.ConsumerGroup(cfg.KafkaConsumerGroup),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.ConsumeResetOffset(resetOffset),

		// Offsets are committed by the pipeline once a batch is acked by
		// JetStream, never in the background — autocommit would commit
		// polled-but-unpublished records and lose them on a crash.
		kgo.DisableAutoCommit(),
		// Hold rebalances off between Poll and AllowNext, so the commit that
		// ends each batch is always for partitions this member still owns.
		// See config.ValidateCombined for why that cycle has to fit inside
		// the rebalance timeout.
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(cfg.KafkaSessionTimeout),
		kgo.RebalanceTimeout(cfg.KafkaRebalanceTimeout),

		// Fetch sizing. The client keeps up to one fetch in flight per
		// broker, so what it buffers is bounded by roughly
		// brokers × FetchMaxBytes.
		kgo.FetchMaxBytes(cfg.KafkaFetchMaxBytes),
		kgo.FetchMaxPartitionBytes(cfg.KafkaFetchMaxPartitionBytes),
		kgo.FetchMinBytes(cfg.KafkaFetchMinBytes),
		kgo.FetchMaxWait(cfg.KafkaFetchMaxWait),
		// franz-go recommends this be about double FetchMaxBytes; only ever
		// raise it above its default, never lower it.
		kgo.BrokerMaxReadBytes(int32(min(math.MaxInt32, max(defaultBrokerMaxReadBytes, 2*int64(cfg.KafkaFetchMaxBytes))))),

		// Logging only. None of these commit: by the time a revoke can run
		// (rebalances are blocked until AllowNext), everything publishable
		// has already been committed by the pipeline, and anything left
		// uncommitted — e.g. after a publish that exhausted its retries —
		// must stay uncommitted so the next owner re-reads it.
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			log.Printf("kafkasource: partitions assigned: %v", assigned)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			log.Printf("kafkasource: partitions revoked: %v", revoked)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			log.Printf("kafkasource: partitions lost: %v", lost)
		}),
	}

	if cfg.KafkaSecurityProtocol == "SASL_PLAINTEXT" {
		mechanism, err := scramMechanism(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	if cfg.KafkaTlsEnabled {
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	return kgo.NewClient(opts...)
}

// buildTLSConfig assembles a *tls.Config from the KAFKA_TLS_* settings.
// Leaving KAFKA_TLS_CA unset falls back to the system's default trust store
// (correct for managed Kafka with publicly-signed broker certificates); a
// client cert/key pair is only added for KAFKA_TLS_MODE=MUTUAL_TLS, for
// which LoadKafkaSourceConfig has already required both.
func buildTLSConfig(cfg config.KafkaSourceConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.KafkaTlsInsecureSkipVerify}

	if cfg.KafkaTlsCaString != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(unescapePEM(cfg.KafkaTlsCaString)) {
			return nil, fmt.Errorf("kafkasource: KAFKA_TLS_CA does not contain a valid PEM certificate")
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.KafkaTlsMode == "MUTUAL_TLS" {
		cert, err := tls.X509KeyPair(unescapePEM(cfg.KafkaTlsCertString), unescapePEM(cfg.KafkaTlsKeyString))
		if err != nil {
			return nil, fmt.Errorf("kafkasource: parse KAFKA_TLS_CERT/KAFKA_TLS_KEY: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// unescapePEM turns escaped "\\n" sequences back into newlines, since env
// vars commonly can't carry literal ones.
func unescapePEM(pem string) []byte {
	return []byte(strings.ReplaceAll(pem, "\\n", "\n"))
}

func scramMechanism(cfg config.KafkaSourceConfig) (sasl.Mechanism, error) {
	auth := scram.Auth{User: cfg.KafkaUsername, Pass: cfg.KafkaPassword}
	switch cfg.KafkaSCRAMAuthMechanism {
	case "SHA256":
		return auth.AsSha256Mechanism(), nil
	case "SHA512":
		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafkasource: unsupported KAFKA_SCRAM_AUTH_MECHANISM %q (want SHA256 or SHA512)", cfg.KafkaSCRAMAuthMechanism)
	}
}

func (s *Source) Name() string { return "kafka" }

func (s *Source) Ready(ctx context.Context) error {
	return s.client.Ping(ctx)
}

// Close leaves the consumer group and closes the client. With autocommit
// disabled, franz-go commits nothing on the way out, so whatever wasn't
// passed to Commit is re-read by the partition's next owner.
func (s *Source) Close(_ context.Context) error {
	s.client.Close()
	return nil
}

func (s *Source) AllowNext() { s.client.AllowRebalance() }

// Poll blocks until at least one record arrives, then keeps polling for up
// to maxPollWait to fill the batch up to maxPollRecords, and returns what it
// has grouped by partition. It returns (nil, ctx.Err()) if ctx ends before
// any record arrives, and the records collected so far if ctx ends while
// filling.
func (s *Source) Poll(ctx context.Context) ([][]*source.Record, error) {
	fetches := s.client.PollRecords(ctx, s.maxPollRecords)
	if err := checkFetches(fetches); err != nil {
		return nil, err
	}
	records := fetches.Records()
	if len(records) == 0 {
		return nil, ctx.Err()
	}

	if len(records) < s.maxPollRecords {
		fillCtx, cancel := context.WithTimeout(ctx, s.maxPollWait)
		for len(records) < s.maxPollRecords && fillCtx.Err() == nil {
			fetches := s.client.PollRecords(fillCtx, s.maxPollRecords-len(records))
			if err := checkFetches(fetches); err != nil {
				cancel()
				return nil, err
			}
			records = append(records, fetches.Records()...)
		}
		cancel()
	}

	return groupByPartition(records), nil
}

// checkFetches logs per-partition fetch errors, which franz-go retries on
// its own, and returns only the one that means the client can't be used
// any more. The context errors PollRecords injects when its ctx ends are
// expected and ignored — the caller checks its own ctx.
func checkFetches(fetches kgo.Fetches) error {
	if fetches.IsClientClosed() {
		return fmt.Errorf("kafkasource: %w", kgo.ErrClientClosed)
	}
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		log.Printf("kafkasource: fetch error on %s[%d]: %v", topic, partition, err)
	})
	return nil
}

// groupByPartition splits records into one slice per partition, keeping
// each partition's records in the offset order franz-go returned them in,
// with partitions sorted by number for deterministic logging.
func groupByPartition(records []*kgo.Record) [][]*source.Record {
	byPartition := make(map[int32][]*source.Record)
	for _, r := range records {
		byPartition[r.Partition] = append(byPartition[r.Partition], toSourceRecord(r))
	}

	partitions := make([]int32, 0, len(byPartition))
	for p := range byPartition {
		partitions = append(partitions, p)
	}
	slices.Sort(partitions)

	batch := make([][]*source.Record, 0, len(partitions))
	for _, p := range partitions {
		batch = append(batch, byPartition[p])
	}
	return batch
}

func toSourceRecord(r *kgo.Record) *source.Record {
	var headers []source.Header
	if len(r.Headers) > 0 {
		headers = make([]source.Header, len(r.Headers))
		for i, h := range r.Headers {
			headers[i] = source.Header{Key: h.Key, Value: h.Value}
		}
	}
	return &source.Record{
		Raw: r.Value,
		Meta: source.Metadata{
			Topic:       r.Topic,
			Partition:   r.Partition,
			Offset:      r.Offset,
			LeaderEpoch: r.LeaderEpoch,
			Key:         r.Key,
			Headers:     headers,
			Timestamp:   r.Timestamp,
		},
	}
}

// Commit commits, per partition, the offset just past the given record.
// Records should be passed in order per partition — committing a lower
// offset after a higher one rewinds the group.
func (s *Source) Commit(ctx context.Context, lastProcessed []*source.Record) error {
	if len(lastProcessed) == 0 {
		return nil
	}
	commit := make([]*kgo.Record, len(lastProcessed))
	for i, rec := range lastProcessed {
		commit[i] = &kgo.Record{
			Topic:       rec.Meta.Topic,
			Partition:   rec.Meta.Partition,
			Offset:      rec.Meta.Offset,
			LeaderEpoch: rec.Meta.LeaderEpoch,
		}
	}
	if err := s.client.CommitRecords(ctx, commit...); err != nil {
		return fmt.Errorf("kafkasource: commit: %w", err)
	}
	return nil
}
