// Package source defines the boundary between whatever upstream system
// records are read from and the NATS-specific publish pipeline. Nothing in
// this package imports franz-go or nats.go: a Source never sees a NATS
// message and the pipeline never sees a Kafka client, which is what keeps
// this interface swappable across brokers.
package source

import (
	"context"
	"time"
)

// Header is one record header. Headers are kept as an ordered slice rather
// than a map because Kafka allows the same key to appear more than once.
type Header struct {
	Key   string
	Value []byte
}

// Metadata carries the transport information the pipeline needs to route,
// annotate and deduplicate a record.
type Metadata struct {
	Topic     string
	Partition int32
	Offset    int64

	// LeaderEpoch is carried through only so that Commit can hand it back
	// to the broker (it's part of Kafka's offset-commit fencing); nothing
	// else uses it.
	LeaderEpoch int32

	Key       []byte
	Headers   []Header
	Timestamp time.Time
}

// Record is one message read from a Source. This connector never decodes
// the payload — it's a byte-for-byte pass-through, so Raw is all the
// pipeline needs.
type Record struct {
	Raw  []byte
	Meta Metadata
}

// Source reads batches of records from an upstream system.
type Source interface {
	// Poll blocks until at least one record is available (or ctx is done)
	// and returns a batch grouped by partition: each inner slice holds the
	// records of one partition, in offset order. Ordering is only
	// meaningful within a partition, so the caller may process partitions
	// in parallel but must process each inner slice sequentially.
	//
	// Every successful Poll must be followed by AllowNext once the batch
	// has been dealt with — rebalances are held off until then.
	Poll(ctx context.Context) ([][]*Record, error)

	// Commit durably records that everything up to and including each
	// given record has been processed, per partition. Passing only the last
	// successfully processed record of each partition is sufficient.
	Commit(ctx context.Context, lastProcessed []*Record) error

	// AllowNext releases the current batch, letting any pending rebalance
	// proceed before the next Poll.
	AllowNext()

	// Name identifies the source for logs/metrics.
	Name() string

	// Ready reports whether the source can currently be read from.
	// Consulted by the readiness probe.
	Ready(ctx context.Context) error

	// Close releases any resources held by the source. It must not commit
	// anything that wasn't explicitly passed to Commit.
	Close(ctx context.Context) error
}
