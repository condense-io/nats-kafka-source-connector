package natsconn

import (
	"log"
	"strconv"
	"strings"
	"sync"

	"condense.zeliot.in/nats-kafka-source-connector/internal/source"

	"github.com/nats-io/nats.go"
)

// Headers the connector adds to every message, unless the Kafka record
// already carries a header of the same name (compared case-insensitively),
// in which case the record's own value is kept.
const (
	HeaderKafkaTopic     = "Kafka-Topic"
	HeaderKafkaPartition = "Kafka-Partition"
	HeaderKafkaOffset    = "Kafka-Offset"
	// HeaderKafkaTimestamp is the record timestamp in Unix milliseconds,
	// Kafka's own unit for it.
	HeaderKafkaTimestamp = "Kafka-Timestamp"
	// HeaderKafkaKey carries the record key, so that it isn't lost when the
	// key can't be used as a subject token and the record is routed by
	// partition instead. Only added for records that have a key.
	HeaderKafkaKey = "Kafka-Key"
)

// renamedHeaderPrefix is prepended to any Kafka header whose name starts
// with "Nats-". JetStream acts on headers in that namespace (Nats-Msg-Id,
// Nats-Expected-*, Nats-TTL, Nats-Rollup, ...), so passing one through
// verbatim would let upstream data override this connector's dedup ID or
// trigger publish expectations it never asked for — which is exactly what
// happens to records that went NATS → Kafka through the sink connector and
// are now coming back.
const renamedHeaderPrefix = "Kafka-Orig-"

// natsHeaders maps a Kafka record's headers onto a NATS header set,
// preserving repeated keys and their order, then adds the Kafka-* metadata
// headers that the record doesn't already carry.
func natsHeaders(meta source.Metadata) nats.Header {
	h := make(nats.Header, len(meta.Headers)+5)
	for _, kh := range meta.Headers {
		key := kh.Key
		if len(key) >= len("Nats-") && strings.EqualFold(key[:len("Nats-")], "Nats-") {
			key = renamedHeaderPrefix + key
		}
		if !isValidHeaderKey(key) {
			logDroppedHeaderOnce(kh.Key)
			continue
		}
		h.Add(key, string(kh.Value))
	}

	addIfMissing(h, HeaderKafkaTopic, meta.Topic)
	addIfMissing(h, HeaderKafkaPartition, strconv.FormatInt(int64(meta.Partition), 10))
	addIfMissing(h, HeaderKafkaOffset, strconv.FormatInt(meta.Offset, 10))
	addIfMissing(h, HeaderKafkaTimestamp, strconv.FormatInt(meta.Timestamp.UnixMilli(), 10))
	if len(meta.Key) > 0 {
		addIfMissing(h, HeaderKafkaKey, string(meta.Key))
	}
	return h
}

func addIfMissing(h nats.Header, key, value string) {
	for existing := range h {
		if strings.EqualFold(existing, key) {
			return
		}
	}
	h.Set(key, value)
}

// isValidHeaderKey mirrors nats.go's own header-key check: printable ASCII
// (33–126) minus a handful of separators. nats.go rejects the whole publish
// with ErrBadHeaderMsg if any key fails it, so an invalid key has to be
// dropped here rather than allowed to fail the record forever.
func isValidHeaderKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if c < 33 || c > 126 || strings.IndexByte(`"()/,:;<=>?@[\]{}`, c) >= 0 {
			return false
		}
	}
	return true
}

var droppedHeaderKeys sync.Map

// logDroppedHeaderOnce logs each distinct invalid header key once per
// process, rather than once per record carrying it.
func logDroppedHeaderOnce(key string) {
	if _, seen := droppedHeaderKeys.LoadOrStore(key, struct{}{}); !seen {
		log.Printf("natsconn: dropping Kafka header %q — not a valid NATS header name (further occurrences are not logged)", key)
	}
}
