// Package subject decides which NATS subject a Kafka record is published to.
//
// A record with a key goes to <prefix>.<hash>, where <hash> is the 64-bit
// xxHash of the raw key bytes as 16 lowercase hex digits. Hashing accepts any
// key (binary, non-UTF-8, any length) and always yields a single safe subject
// token. A record with a nil or empty key goes to <prefix>.<partition>.
// Ordering is unaffected either way: a given key always maps to the same
// subject, and every partition is published strictly in offset order.
//
// The two routes can't collide: the hash token is always 16 characters,
// while a partition number is at most 10 digits.
package subject

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// For returns the subject a record with the given key and partition is
// published to.
func For(prefix string, key []byte, partition int32) string {
	if len(key) == 0 {
		return prefix + "." + strconv.FormatInt(int64(partition), 10)
	}
	var buf [16]byte
	h := xxhash.Sum64(key)
	const hexDigits = "0123456789abcdef"
	for i := 15; i >= 0; i-- {
		buf[i] = hexDigits[h&0xf]
		h >>= 4
	}
	return prefix + "." + string(buf[:])
}

// ValidatePrefix checks that prefix is a literal subject: one or more
// non-empty tokens of printable ASCII, with no wildcards and no leading '$'
// (reserved for NATS system subjects such as $JS and $SYS).
func ValidatePrefix(prefix string) error {
	if strings.HasPrefix(prefix, "$") {
		return fmt.Errorf("%q must not start with '$' (reserved for NATS system subjects)", prefix)
	}
	for _, token := range strings.Split(prefix, ".") {
		if token == "" {
			return fmt.Errorf("%q has an empty token (leading, trailing or doubled '.')", prefix)
		}
		if token == "*" || token == ">" || strings.ContainsAny(token, "*>") {
			return fmt.Errorf("%q must not contain wildcards", prefix)
		}
		for i := 0; i < len(token); i++ {
			if token[i] <= ' ' || token[i] > '~' {
				return fmt.Errorf("%q must contain only printable ASCII and no whitespace", prefix)
			}
		}
	}
	return nil
}
