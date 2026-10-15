// Package pipeline binds a source.Source to the JetStream publisher: it
// polls a batch, publishes it, commits what was published, and repeats. It
// is the only package that knows about both sides — the source stays
// broker-agnostic and the publisher never sees a Kafka client.
//
// Publishing: the whole batch is sent to JetStream asynchronously and every
// record's ack is awaited individually (see natsconn.Publisher). Nothing new
// is polled from the source until the batch has been acked (or has failed).
//
// Ordering: Kafka order within a partition is preserved on the stream only
// while nothing fails mid-flight. A record that has to be retried is stored
// after records published behind it. This is an accepted trade-off.
//
// Failure handling: records that fail are retried; if some still can't be
// published, per partition the contiguous run of stored records from the
// start of the batch is committed and Run returns an error so the process
// exits; on restart the partition resumes from the committed offset, and any
// records that were stored but not committed are dropped by the stream's
// duplicate detection (Nats-Msg-Id).
package pipeline

import (
	"context"
	"fmt"
	"log"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/natsconn"
	"condense.zeliot.in/nats-kafka-source-connector/internal/source"
)

// Publisher is what the pipeline needs from natsconn.Publisher.
type Publisher interface {
	PublishBatch(ctx context.Context, recs []*source.Record) (stored []bool, duplicates int, err error)
}

var _ Publisher = (*natsconn.Publisher)(nil)

type Orchestrator struct {
	src           source.Source
	pub           Publisher
	shutdownGrace time.Duration
}

func New(src source.Source, pub Publisher, shutdownGrace time.Duration) *Orchestrator {
	return &Orchestrator{src: src, pub: pub, shutdownGrace: shutdownGrace}
}

// Run polls, publishes and commits batches until ctx is cancelled or a
// record can't be published. A batch that's already publishing when ctx is
// cancelled is allowed to finish and commit, bounded by shutdownGrace; a
// batch polled after cancellation is left uncommitted for the partition's
// next owner.
func (o *Orchestrator) Run(ctx context.Context) error {
	// Publishing and committing run on their own context so that shutdown
	// doesn't abort a batch midway: it's only cancelled shutdownGrace after
	// ctx is.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	stop := context.AfterFunc(ctx, func() {
		time.AfterFunc(o.shutdownGrace, cancelWork)
	})
	defer stop()

	for {
		batch, err := o.src.Poll(ctx)
		if ctx.Err() != nil {
			o.src.AllowNext()
			return nil
		}
		if err != nil {
			o.src.AllowNext()
			return fmt.Errorf("pipeline: poll %s: %w", o.src.Name(), err)
		}
		if len(batch) == 0 {
			o.src.AllowNext()
			continue
		}

		err = o.processBatch(workCtx, batch)
		o.src.AllowNext()
		if err != nil {
			return err
		}
	}
}

// processBatch publishes the batch, then commits for each partition the
// last record of its contiguous stored prefix — including after a failure,
// so a restart doesn't re-publish more than it has to.
func (o *Orchestrator) processBatch(ctx context.Context, batch [][]*source.Record) error {
	var all []*source.Record
	for _, records := range batch {
		all = append(all, records...)
	}
	stored, duplicates, pubErr := o.pub.PublishBatch(ctx, all)

	var lastPublished []*source.Record
	n := 0
	for _, records := range batch {
		var last *source.Record
		for i, rec := range records {
			if !stored[n+i] {
				break
			}
			last = rec
		}
		n += len(records)
		if last != nil {
			lastPublished = append(lastPublished, last)
		}
	}

	if duplicates > 0 {
		log.Printf("pipeline: %d record(s) in this batch were already in the stream (re-delivered after a restart or rebalance) and were dropped as duplicates", duplicates)
	}

	// A failed commit isn't fatal on its own: offsets are committed as a
	// high-water mark, so the next successful commit covers this batch
	// too. Until then a restart would re-publish these records, which the
	// stream's duplicate detection absorbs.
	if err := o.src.Commit(ctx, lastPublished); err != nil {
		log.Printf("pipeline: %v", err)
	}

	if pubErr != nil {
		return fmt.Errorf("pipeline: giving up, exiting so the connector restarts from the last committed offsets: %w", pubErr)
	}
	return nil
}
