package natsconn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config"

	"github.com/nats-io/nats.go/jetstream"
)

// permanentError marks an error from withRetry's fn as non-retryable —
// retrying it again could never succeed (e.g. the server has definitively
// told us a resource doesn't exist), so withRetry returns the wrapped error
// immediately instead of burning through its remaining attempts.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// withRetry retries fn up to maxAttempts times with exponential backoff
// (2s, 4s, 8s, capped at 15s between attempts), for absorbing transient
// NATS unavailability at startup — e.g. many replicas restarting together
// and all hitting the JetStream management API's default 5s per-call
// timeout around the same moment. It returns immediately, without
// retrying, if fn returns a *permanentError, and stops early if ctx is
// cancelled.
func withRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		var perm *permanentError
		if errors.As(err, &perm) {
			return perm.err
		}
		if attempt == maxAttempts {
			break
		}

		const maxBackoff = 15 * time.Second
		backoff := time.Duration(1<<uint(attempt)) * time.Second // 2s, 4s, 8s, 16s...
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		log.Printf("natsconn: attempt %d/%d failed: %v — retrying in %s", attempt, maxAttempts, err, backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// VerifyStream looks up the target stream and confirms it captures every
// subject this connector can publish to — <SubjectPrefix>.<token> for any
// single token. The stream itself is provisioned out of band by a separate
// management service/UI — this connector never creates or updates it. A
// missing stream or an uncovered prefix is treated as a startup-fatal
// misconfiguration: publishing into it would either fail every time (no
// stream bound to the subject) or, worse, succeed into some other stream.
func VerifyStream(ctx context.Context, js jetstream.JetStream, natsSinkConfig config.NatsSinkConfig) error {
	var stream jetstream.Stream
	err := withRetry(ctx, 5, func() error {
		s, err := js.Stream(ctx, natsSinkConfig.StreamName)
		if err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) {
				return &permanentError{fmt.Errorf("natsconn: stream %q does not exist on the NATS server — it must be created by the stream owner before this connector can start", natsSinkConfig.StreamName)}
			}
			return fmt.Errorf("natsconn: look up stream %q: %w", natsSinkConfig.StreamName, err)
		}
		stream = s
		return nil
	})
	if err != nil {
		return err
	}

	want := natsSinkConfig.SubjectPrefix + ".*"
	streamSubjects := stream.CachedInfo().Config.Subjects
	if !coveredByAny(want, streamSubjects) {
		return fmt.Errorf("natsconn: stream %q does not capture every subject under %q (needs a subject filter covering %q, e.g. %q or %q; stream subjects: %v) — ask the stream owner to add it",
			natsSinkConfig.StreamName, natsSinkConfig.SubjectPrefix, want, want, natsSinkConfig.SubjectPrefix+".>", streamSubjects)
	}

	// Re-publishing after a retry or restart is only de-duplicated inside the
	// stream's duplicate window, so it has to outlast the retry budget.
	if window := stream.CachedInfo().Config.Duplicates; natsSinkConfig.DedupEnabled && window > 0 && window <= natsSinkConfig.PublishRetryBudget() {
		log.Printf("natsconn: WARNING: stream %q duplicate window (%s) is not longer than the publish retry budget (%s); a record retried after that long may be stored twice — raise the stream's duplicate window or lower the NATS_PUBLISH_* retry settings",
			natsSinkConfig.StreamName, window, natsSinkConfig.PublishRetryBudget())
	}

	return nil
}

// coveredByAny reports whether every subject matched by want is also
// matched by at least one of the stream's subject filters.
func coveredByAny(want string, streamSubjects []string) bool {
	for _, s := range streamSubjects {
		if subjectCovers(s, want) {
			return true
		}
	}
	return false
}

// subjectCovers reports whether filter matches every subject want can
// match, treating "*" as a single-token wildcard and ">" as a
// one-or-more-trailing-tokens wildcard. Overlap isn't enough for a
// publisher: a stream on "orders.eu" overlaps "orders.*" but would leave a
// publish to "orders.us" with no stream to land in.
func subjectCovers(filter, want string) bool {
	tf := strings.Split(filter, ".")
	tw := strings.Split(want, ".")
	for i, w := range tw {
		if i >= len(tf) {
			return false
		}
		f := tf[i]
		switch {
		case f == ">":
			return true
		case w == ">":
			return false
		case f == "*":
		case f != w:
			return false
		}
	}
	return len(tf) == len(tw)
}
