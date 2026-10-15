package config

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config/utils"
)

type KafkaSourceConfig struct {
	KafkaBootstrapServers string
	KafkaTopic            string

	// KafkaSecurityProtocol selects how the client authenticates to Kafka —
	// exactly one of "NONE" (default, no authentication) or
	// "SASL_PLAINTEXT" (SASL/SCRAM, requires KafkaUsername/KafkaPassword).
	// Read case-insensitively and normalized to uppercase, as is
	// KafkaSCRAMAuthMechanism ("SHA256" or "SHA512").
	KafkaSecurityProtocol   string
	KafkaSCRAMAuthMechanism string
	KafkaUsername           string
	KafkaPassword           string

	// KafkaTlsEnabled turns on TLS for the connection to the brokers,
	// independently of KafkaSecurityProtocol — e.g. SASL_PLAINTEXT with TLS
	// enabled carries SASL over an encrypted connection (what Kafka itself
	// calls SASL_SSL), while NONE with TLS enabled is server-only TLS with
	// no SASL layer (Kafka's SSL). Parsed as a general boolean.
	KafkaTlsEnabled bool

	// KafkaTlsCaString optionally supplies a private CA to trust, as inline
	// PEM. Leaving it unset falls back to the system's default CA trust
	// store — the common case for managed Kafka (Confluent Cloud, MSK,
	// Aiven, etc.) using publicly-signed broker certificates.
	KafkaTlsCaString string

	// KafkaTlsMode is either empty (server-only TLS) or "MUTUAL_TLS", which
	// additionally presents KafkaTlsCertString/KafkaTlsKeyString (inline
	// PEM, both required) as a client certificate.
	KafkaTlsMode       string
	KafkaTlsCertString string
	KafkaTlsKeyString  string

	// KafkaTlsInsecureSkipVerify disables broker certificate verification.
	// Only ever intended for testing against a self-signed dev cluster —
	// never enable this in production, as it makes the connection
	// vulnerable to on-path tampering.
	KafkaTlsInsecureSkipVerify bool

	KafkaConsumerGroup    string
	KafkaConsumerClientId string

	// KafkaMaxPollRecords caps how many records make up one batch handed to
	// the NATS publisher. KafkaMaxPollWait bounds how long the source keeps
	// polling to fill a batch up to that cap once the first record of the
	// batch has arrived — a partially filled batch is handed over when it
	// expires.
	KafkaMaxPollRecords int
	KafkaMaxPollWait    time.Duration

	// KafkaFetchMaxBytes/KafkaFetchMaxPartitionBytes cap the size of a
	// single fetch response (across all partitions, and per partition).
	// With only one fetch allowed in flight (see kafkasource), these are
	// what bound how many bytes the client holds in memory at once.
	// KafkaFetchMinBytes/KafkaFetchMaxWait make the broker hold a fetch
	// until at least that many bytes are available or the wait elapses, so
	// that fetches arrive as batches rather than a trickle.
	KafkaFetchMaxBytes          int32
	KafkaFetchMaxPartitionBytes int32
	KafkaFetchMinBytes          int32
	KafkaFetchMaxWait           time.Duration

	KafkaSessionTimeout time.Duration

	// KafkaRebalanceTimeout is franz-go's closest analogue of the Java
	// client's max.poll.interval.ms: rebalances are blocked from the moment
	// a batch is polled until it has been fully published and committed
	// (see kafkasource), so a whole poll→publish→commit cycle must fit
	// within it, or the broker kicks this member out of the group.
	KafkaRebalanceTimeout time.Duration

	// KafkaConsumeFromOffset is where to start when the consumer group has
	// no committed offset for a partition — "earliest" or "latest".
	KafkaConsumeFromOffset string
}

func LoadKafkaSourceConfig() (KafkaSourceConfig, error) {

	var errs []string
	req := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, key+" is required")
		}
		return v
	}

	kafkaSourceConfig := KafkaSourceConfig{
		KafkaBootstrapServers: req("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaTopic:            req("KAFKA_TOPIC"),
	}

	kafkaSourceConfig.KafkaSecurityProtocol = strings.ToUpper(utils.GetEnv("KAFKA_SECURITY_PROTOCOL", "NONE"))
	switch kafkaSourceConfig.KafkaSecurityProtocol {
	case "NONE":
	case "SASL_PLAINTEXT":
		kafkaSourceConfig.KafkaUsername = req("KAFKA_USERNAME")
		kafkaSourceConfig.KafkaPassword = req("KAFKA_PASSWORD")
		kafkaSourceConfig.KafkaSCRAMAuthMechanism = strings.ToUpper(utils.GetEnv("KAFKA_SCRAM_AUTH_MECHANISM", "SHA512"))
		if kafkaSourceConfig.KafkaSCRAMAuthMechanism != "SHA256" && kafkaSourceConfig.KafkaSCRAMAuthMechanism != "SHA512" {
			errs = append(errs, fmt.Sprintf("KAFKA_SCRAM_AUTH_MECHANISM must be one of SHA256|SHA512, got %q", kafkaSourceConfig.KafkaSCRAMAuthMechanism))
		}
	default:
		errs = append(errs, fmt.Sprintf("KAFKA_SECURITY_PROTOCOL must be one of NONE|SASL_PLAINTEXT, got %q", kafkaSourceConfig.KafkaSecurityProtocol))
	}

	kafkaSourceConfig.KafkaTlsEnabled = utils.GetEnvBool("KAFKA_TLS_ENABLED", false)
	kafkaSourceConfig.KafkaTlsMode = utils.GetEnv("KAFKA_TLS_MODE", "")
	kafkaSourceConfig.KafkaTlsCaString = os.Getenv("KAFKA_TLS_CA")
	kafkaSourceConfig.KafkaTlsCertString = os.Getenv("KAFKA_TLS_CERT")
	kafkaSourceConfig.KafkaTlsKeyString = os.Getenv("KAFKA_TLS_KEY")
	kafkaSourceConfig.KafkaTlsInsecureSkipVerify = utils.GetEnvBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false)

	hasTlsCert := kafkaSourceConfig.KafkaTlsCertString != ""
	hasTlsKey := kafkaSourceConfig.KafkaTlsKeyString != ""
	if kafkaSourceConfig.KafkaTlsEnabled {
		switch kafkaSourceConfig.KafkaTlsMode {
		case "ONE_WAY_TLS":
			if hasTlsCert || hasTlsKey {
				log.Println("config: KAFKA_TLS_CERT/KAFKA_TLS_KEY are set but KAFKA_TLS_MODE is not MUTUAL_TLS — ignoring them")
			}
		case "MUTUAL_TLS":
			if !hasTlsCert || !hasTlsKey {
				errs = append(errs, "KAFKA_TLS_CERT and KAFKA_TLS_KEY are both required when KAFKA_TLS_MODE=MUTUAL_TLS")
			}
		default:
			errs = append(errs, fmt.Sprintf("KAFKA_TLS_MODE must be empty or MUTUAL_TLS, got %q", kafkaSourceConfig.KafkaTlsMode))
		}
	} else if kafkaSourceConfig.KafkaTlsCaString != "" || hasTlsCert || hasTlsKey || kafkaSourceConfig.KafkaTlsMode != "" {
		errs = append(errs, "KAFKA_TLS_* settings are set but KAFKA_TLS_ENABLED is not true")
	}

	kafkaSourceConfig.KafkaConsumerGroup = req("KAFKA_CONSUMER_GROUP")
	hostName, _ := os.Hostname()
	kafkaSourceConfig.KafkaConsumerClientId = utils.GetEnv("KAFKA_CONSUMER_CLIENT_ID", hostName)

	positiveInt := func(key string, def int) int {
		n, err := utils.GetEnvInt(key, def)
		if err != nil {
			errs = append(errs, err.Error())
			return def
		}
		if n <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be > 0, got %d", key, n))
		}
		return n
	}
	// franz-go takes fetch byte limits as int32.
	positiveInt32 := func(key string, def int) int32 {
		n := positiveInt(key, def)
		if n > math.MaxInt32 {
			errs = append(errs, fmt.Sprintf("%s must be <= %d, got %d", key, math.MaxInt32, n))
			return int32(def)
		}
		return int32(n)
	}
	positiveDuration := func(key string, def time.Duration) time.Duration {
		d, err := utils.GetEnvDuration(key, def)
		if err != nil {
			errs = append(errs, err.Error())
			return def
		}
		if d <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be > 0, got %s", key, d))
		}
		return d
	}

	kafkaSourceConfig.KafkaMaxPollRecords = positiveInt("KAFKA_MAX_POLL_RECORDS", 500)
	kafkaSourceConfig.KafkaMaxPollWait = positiveDuration("KAFKA_MAX_POLL_WAIT", time.Second)
	kafkaSourceConfig.KafkaFetchMaxBytes = positiveInt32("KAFKA_FETCH_MAX_BYTES", 50<<20)
	kafkaSourceConfig.KafkaFetchMaxPartitionBytes = positiveInt32("KAFKA_FETCH_MAX_PARTITION_BYTES", 1<<20)
	kafkaSourceConfig.KafkaFetchMinBytes = positiveInt32("KAFKA_FETCH_MIN_BYTES", 1)
	kafkaSourceConfig.KafkaFetchMaxWait = positiveDuration("KAFKA_FETCH_MAX_WAIT", 500*time.Millisecond)
	kafkaSourceConfig.KafkaSessionTimeout = positiveDuration("KAFKA_SESSION_TIMEOUT", 45*time.Second)
	kafkaSourceConfig.KafkaRebalanceTimeout = positiveDuration("KAFKA_REBALANCE_TIMEOUT", 60*time.Second)

	if kafkaSourceConfig.KafkaFetchMaxPartitionBytes > kafkaSourceConfig.KafkaFetchMaxBytes {
		errs = append(errs, "KAFKA_FETCH_MAX_PARTITION_BYTES must be <= KAFKA_FETCH_MAX_BYTES")
	}
	if kafkaSourceConfig.KafkaFetchMinBytes > kafkaSourceConfig.KafkaFetchMaxBytes {
		errs = append(errs, "KAFKA_FETCH_MIN_BYTES must be <= KAFKA_FETCH_MAX_BYTES")
	}

	kafkaSourceConfig.KafkaConsumeFromOffset = strings.ToLower(utils.GetEnv("KAFKA_CONSUME_FROM_OFFSET", "earliest"))
	if kafkaSourceConfig.KafkaConsumeFromOffset != "earliest" && kafkaSourceConfig.KafkaConsumeFromOffset != "latest" {
		errs = append(errs, fmt.Sprintf("KAFKA_CONSUME_FROM_OFFSET must be one of earliest|latest, got %q", kafkaSourceConfig.KafkaConsumeFromOffset))
	}

	if len(errs) > 0 {
		return kafkaSourceConfig, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	log.Println("Kafka source configuration loaded successfully")
	log.Println("Kafka source configuration details:")
	log.Printf("  KAFKA_BOOTSTRAP_SERVERS: %s", kafkaSourceConfig.KafkaBootstrapServers)
	log.Printf("  KAFKA_TOPIC: %s", kafkaSourceConfig.KafkaTopic)
	log.Printf("  KAFKA_CONSUMER_GROUP: %s", kafkaSourceConfig.KafkaConsumerGroup)
	log.Printf("  KAFKA_CONSUMER_CLIENT_ID: %s", kafkaSourceConfig.KafkaConsumerClientId)
	log.Printf("  KAFKA_SECURITY_PROTOCOL: %s", kafkaSourceConfig.KafkaSecurityProtocol)
	if kafkaSourceConfig.KafkaSecurityProtocol == "SASL_PLAINTEXT" {
		log.Printf("  KAFKA_SCRAM_AUTH_MECHANISM: %s", kafkaSourceConfig.KafkaSCRAMAuthMechanism)
	}
	log.Printf("  KAFKA_TLS_ENABLED: %t", kafkaSourceConfig.KafkaTlsEnabled)
	if kafkaSourceConfig.KafkaTlsEnabled {
		log.Printf("  KAFKA_TLS_MODE: %s", kafkaSourceConfig.KafkaTlsMode)
		log.Printf("  KafkaTlsCaConfigured: %t", kafkaSourceConfig.KafkaTlsCaString != "")
		log.Printf("  KafkaTlsClientCertConfigured: %t", kafkaSourceConfig.KafkaTlsMode == "MUTUAL_TLS")
		log.Printf("  KAFKA_TLS_INSECURE_SKIP_VERIFY: %t", kafkaSourceConfig.KafkaTlsInsecureSkipVerify)
	}
	log.Printf("  KAFKA_MAX_POLL_RECORDS: %d", kafkaSourceConfig.KafkaMaxPollRecords)
	log.Printf("  KAFKA_MAX_POLL_WAIT: %s", kafkaSourceConfig.KafkaMaxPollWait)
	log.Printf("  KAFKA_FETCH_MAX_BYTES: %d", kafkaSourceConfig.KafkaFetchMaxBytes)
	log.Printf("  KAFKA_FETCH_MAX_PARTITION_BYTES: %d", kafkaSourceConfig.KafkaFetchMaxPartitionBytes)
	log.Printf("  KAFKA_FETCH_MIN_BYTES: %d", kafkaSourceConfig.KafkaFetchMinBytes)
	log.Printf("  KAFKA_FETCH_MAX_WAIT: %s", kafkaSourceConfig.KafkaFetchMaxWait)
	log.Printf("  KAFKA_SESSION_TIMEOUT: %s", kafkaSourceConfig.KafkaSessionTimeout)
	log.Printf("  KAFKA_REBALANCE_TIMEOUT: %s", kafkaSourceConfig.KafkaRebalanceTimeout)
	log.Printf("  KAFKA_CONSUME_FROM_OFFSET: %s", kafkaSourceConfig.KafkaConsumeFromOffset)
	return kafkaSourceConfig, nil
}
