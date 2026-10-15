package natsconn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config"
	"condense.zeliot.in/nats-kafka-source-connector/internal/source"
	"condense.zeliot.in/nats-kafka-source-connector/internal/subject"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher publishes records to JetStream with PublishMsgAsync: a whole
// batch is put in flight (bounded by NATS_MAX_UNACK_MSGS unacked
// messages) and every record's PubAck is then awaited individually.
//
// Ordering trade-off: because publishes overlap, a record that fails and is
// retried lands in the stream after records published behind it, so
// per-subject order is only guaranteed while nothing fails mid-flight.
// Retries are safe from duplication: every record carries a Nats-Msg-Id
// (when dedup is enabled), so a record the stream already stored — e.g. its
// ack was lost — is dropped on resend, within the stream's duplicate window.
type Publisher struct {
	js  jetstream.JetStream
	cfg config.NatsSinkConfig
}

func NewPublisher(js jetstream.JetStream, cfg config.NatsSinkConfig) *Publisher {
	return &Publisher{js: js, cfg: cfg}
}

// PublishBatch publishes recs and waits until each one is acked, retrying
// only the records that failed (up to NATS_PUBLISH_MAX_RETRIES more rounds,
// with backoff). stored[i] reports whether recs[i] is in the stream
// afterwards; duplicates counts records the stream already had, which are
// treated as stored. A non-nil error means at least one record was not
// stored: it exhausted its retries, or failed in a way retrying can't fix
// (see isPermanent). Records that did succeed stay stored regardless.
func (p *Publisher) PublishBatch(ctx context.Context, recs []*source.Record) (stored []bool, duplicates int, err error) {
	stored = make([]bool, len(recs))
	pending := make([]int, len(recs))
	for i := range pending {
		pending[i] = i
	}

	backoff := p.cfg.PublishRetryBackoff
	var permanent []error
	for attempt := 0; ; attempt++ {
		failed, errs, dups := p.publishRound(ctx, recs, pending, stored)
		duplicates += dups

		var retryable []int
		var lastErr error
		for n, i := range failed {
			if isPermanent(errs[n]) {
				permanent = append(permanent, fmt.Errorf("topic %s partition %d offset %d: natsconn: publish to %q failed with a non-retryable error: %w",
					recs[i].Meta.Topic, recs[i].Meta.Partition, recs[i].Meta.Offset, p.Subject(recs[i]), errs[n]))
				continue
			}
			retryable = append(retryable, i)
			lastErr = errs[n]
		}
		if len(retryable) == 0 {
			return stored, duplicates, errors.Join(permanent...)
		}

		if attempt >= p.cfg.PublishMaxRetries || ctx.Err() != nil {
			rec := recs[retryable[0]]
			return stored, duplicates, errors.Join(append(permanent, fmt.Errorf(
				"topic %s partition %d offset %d: natsconn: %d record(s) not stored after %d attempt(s) (first failed record shown, last error: %w)",
				rec.Meta.Topic, rec.Meta.Partition, rec.Meta.Offset, len(retryable), attempt+1, lastErr))...)
		}

		log.Printf("natsconn: %d of %d record(s) failed to publish, attempt %d/%d, retrying in %s: %v",
			len(retryable), len(recs), attempt+1, p.cfg.PublishMaxRetries+1, backoff, lastErr)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return stored, duplicates, fmt.Errorf("natsconn: publish abandoned: %w (last error: %v)", ctx.Err(), lastErr)
		}
		backoff = min(2*backoff, p.cfg.PublishRetryMaxBackoff)
		pending = retryable
	}
}

type inflight struct {
	idx int
	pubAckFuture jetstream.PubAckFuture
}

// publishRound sends recs[i] for every i in pending, in order, then waits
// for each ack. It sets stored for the ones acked and returns the indexes
// that failed (in order) with their errors.
func (p *Publisher) publishRound(ctx context.Context, recs []*source.Record, pending []int, stored []bool) (failed []int, errs []error, duplicates int) {
	pubAckFutures := make([]inflight, 0, len(pending))
	for n, i := range pending {
		rec := recs[i]
		msg := &nats.Msg{Subject: p.Subject(rec), Data: rec.Raw, Header: natsHeaders(rec.Meta)}
		opts := []jetstream.PublishOpt{
			jetstream.WithExpectStream(p.cfg.StreamName),
			jetstream.WithStallWait(p.cfg.PublishTimeout),
		}
		if p.cfg.DedupEnabled {
			opts = append(opts, jetstream.WithMsgID(msgID(rec.Meta)))
		}
		fut, err := p.js.PublishMsgAsync(msg, opts...)
		if err != nil {
			// Couldn't even submit (connection down, or stalled behind too
			// many unacked messages). Don't hammer the rest — fail them
			// all and let the retry backoff run.
			for _, j := range pending[n:] {
				failed = append(failed, j)
				errs = append(errs, err)
			}
			break
		}
		pubAckFutures = append(pubAckFutures, inflight{i, fut})
	}

	for n, f := range pubAckFutures {
		select {
		case ack := <-f.pubAckFuture.Ok():
			stored[f.idx] = true
			if ack.Duplicate {
				duplicates++
			}
		case err := <-f.pubAckFuture.Err():
			failed = append(failed, f.idx)
			errs = append(errs, err)
		case <-ctx.Done():
			for _, g := range pubAckFutures[n:] {
				failed = append(failed, g.idx)
				errs = append(errs, ctx.Err())
			}
			return failed, errs, duplicates
		}
	}
	return failed, errs, duplicates
}

// Subject returns the subject rec is published to. Records with the same
// subject must be published in order; records with different subjects
// need no ordering between them.
func (p *Publisher) Subject(rec *source.Record) string {
	return subject.For(p.cfg.SubjectPrefix, rec.Meta.Key, rec.Meta.Partition)
}

// msgID identifies a record uniquely and stably across re-deliveries, for
// the stream's duplicate detection.
func msgID(meta source.Metadata) string {
	return fmt.Sprintf("%s-%d-%d", meta.Topic, meta.Partition, meta.Offset)
}

// isPermanent reports whether retrying a failed publish could never
// succeed: the message itself is unacceptable (too large, bad subject or
// headers), or JetStream rejected it with a client-error (4xx) API error —
// e.g. the subject is bound to a different stream than STREAM_NAME, or the
// message exceeds the stream's max message size. 5xx API errors (e.g. no
// quorum during a leader election) and timeouts are transient.
func isPermanent(err error) bool {
	if errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, nats.ErrBadSubject) || errors.Is(err, nats.ErrBadHeaderMsg) {
		return true
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code >= 400 && apiErr.Code < 500
	}
	return false
}
