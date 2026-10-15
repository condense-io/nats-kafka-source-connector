package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config/utils"
	"condense.zeliot.in/nats-kafka-source-connector/internal/subject"
)

type NatsSinkConfig struct {
	NatsUrl        string
	NatsClientName string

	// NatsAuthMechanism is one of "NONE" (or unset — no auth),
	// "JWT_NKEY_FILE", "JWT_NKEY", "NKEY", "TOKEN" or "USERNAME_PASSWORD",
	// compared case-sensitively. Each requires its own credentials below.
	NatsAuthMechanism string

	// USERNAME_PASSWORD
	NatsUsername string
	NatsPassword string

	// JWT_NKEY_FILE: a .creds file path, or its content inline (the file
	// wins if both are set).
	NatsJwtNKeyCreds string
	NatsCredsFile    string

	// JWT_NKEY: the user JWT and NKey seed as two separate values.
	NatsJwt  string
	NatsNKey string

	// TOKEN / NKEY (a raw NKey seed, no JWT).
	NatsToken    string
	NatsUserSeed string

	// NatsTlsEnabled turns on TLS to NATS (parsed as a general boolean) and
	// then requires NatsTlsCaFileContent, the CA to trust as inline PEM.
	// NatsTlsMode is optional: empty or "ONE_WAY_TLS" is server-only TLS, and
	// "MUTUAL_TLS" (which requires NatsTlsEnabled) additionally presents
	// NatsTlsCertFileContent/NatsTlsKeyFileContent (inline PEM, both
	// required) as a client certificate.
	NatsTlsEnabled            bool
	NatsTlsCaFileContent      string
	NatsTlsMode               string
	NatsTlsInsecureSkipVerify bool
	NatsTlsCertFileContent    string
	NatsTlsKeyFileContent     string

	// StreamName is provisioned out of band, by a separate management
	// service/UI — this connector only looks it up and verifies it captures
	// every subject under SubjectPrefix, it never creates or modifies it.
	StreamName string

	// SubjectPrefix is the literal (wildcard-free) subject every record is
	// published under, as <SubjectPrefix>.<xxhash of kafka key> or
	// <SubjectPrefix>.<kafka partition> — see package subject.
	SubjectPrefix string

	// PublishTimeout bounds the wait for a publish's PubAck. A record that
	// fails is retried up to PublishMaxRetries more times, sleeping
	// PublishRetryBackoff (doubling each attempt, capped at
	// PublishRetryMaxBackoff) in between, before the connector gives up and
	// exits.
	PublishTimeout         time.Duration
	PublishMaxRetries      int
	PublishRetryBackoff    time.Duration
	PublishRetryMaxBackoff time.Duration

	// MaxUnackMsgs is the most publishes that may be in flight
	// (sent, ack not yet received) at once. PublishTimeout is how long each
	// one's ack may take.
	MaxUnackMsgs int

	// DedupEnabled sets Nats-Msg-Id to <topic>-<partition>-<offset> on every
	// publish, so that records re-published after a crash or rebalance
	// (published, but their offsets not yet committed) are dropped by the
	// stream as duplicates — as long as they arrive within the stream's
	// own duplicate window.
	DedupEnabled bool
}

func LoadNatsSinkConfig() (NatsSinkConfig, error) {

	var errs []string
	req := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, key+" is required")
		}
		return v
	}

	natsSinkConfig := NatsSinkConfig{
		NatsUrl:        req("NATS_URL"),
		NatsCredsFile:  os.Getenv("NATS_CREDS_FILE"),
		NatsClientName: utils.GetEnv("NATS_CLIENT_NAME", "kafka-source-connector"),

		StreamName:    req("STREAM_NAME"),
		SubjectPrefix: req("NATS_SUBJECT_PREFIX"),
	}

	if natsSinkConfig.SubjectPrefix != "" {
		if err := subject.ValidatePrefix(natsSinkConfig.SubjectPrefix); err != nil {
			errs = append(errs, "NATS_SUBJECT_PREFIX: "+err.Error())
		}
	}

	natsSinkConfig.NatsAuthMechanism = utils.GetEnv("NATS_AUTH_MECHANISM", "")
	switch natsSinkConfig.NatsAuthMechanism {
	case "", "NONE":
	case "USERNAME_PASSWORD":
		natsSinkConfig.NatsUsername = req("NATS_USERNAME")
		natsSinkConfig.NatsPassword = req("NATS_PASSWORD")
	case "JWT_NKEY_FILE":
		natsSinkConfig.NatsJwtNKeyCreds = os.Getenv("NATS_CREDS")
		if natsSinkConfig.NatsCredsFile == "" && natsSinkConfig.NatsJwtNKeyCreds == "" {
			errs = append(errs, "NATS_CREDS_FILE or NATS_CREDS is required for JWT_NKEY_FILE authentication")
		}
	case "JWT_NKEY":
		natsSinkConfig.NatsJwt = req("NATS_JWT")
		natsSinkConfig.NatsNKey = req("NATS_NKEY")
	case "NKEY":
		natsSinkConfig.NatsUserSeed = req("NATS_USER_SEED")
	case "TOKEN":
		natsSinkConfig.NatsToken = req("NATS_TOKEN")
	default:
		errs = append(errs, fmt.Sprintf("unsupported NATS_AUTH_MECHANISM %q (want NONE, JWT_NKEY_FILE, JWT_NKEY, NKEY, TOKEN or USERNAME_PASSWORD)", natsSinkConfig.NatsAuthMechanism))
	}

	if natsSinkConfig.NatsCredsFile != "" && natsSinkConfig.NatsAuthMechanism != "JWT_NKEY_FILE" {
		errs = append(errs, "NATS_CREDS_FILE is set but NATS_AUTH_MECHANISM is not \"JWT_NKEY_FILE\"")
	}

	natsSinkConfig.NatsTlsEnabled = utils.GetEnvBool("NATS_TLS_ENABLED", false)
	natsSinkConfig.NatsTlsCaFileContent = os.Getenv("NATS_TLS_CA")
	natsSinkConfig.NatsTlsMode = utils.GetEnv("NATS_TLS_MODE", "")
	natsSinkConfig.NatsTlsInsecureSkipVerify = utils.GetEnvBool("NATS_TLS_INSECURE_SKIP_VERIFY", false)
	if natsSinkConfig.NatsTlsEnabled && natsSinkConfig.NatsTlsCaFileContent == "" {
		errs = append(errs, "NATS_TLS_CA is required when NATS_TLS_ENABLED=true")
	}

	switch natsSinkConfig.NatsTlsMode {
	case "", "ONE_WAY_TLS":
		if os.Getenv("NATS_TLS_CERT") != "" || os.Getenv("NATS_TLS_KEY") != "" {
			log.Println("config: NATS_TLS_CERT/NATS_TLS_KEY are set but NATS_TLS_MODE is not MUTUAL_TLS — ignoring them")
		}
	case "MUTUAL_TLS":
		if !natsSinkConfig.NatsTlsEnabled {
			errs = append(errs, "NATS_TLS_MODE=MUTUAL_TLS requires NATS_TLS_ENABLED=true")
		}
		natsSinkConfig.NatsTlsCertFileContent = req("NATS_TLS_CERT")
		natsSinkConfig.NatsTlsKeyFileContent = req("NATS_TLS_KEY")
	default:
		errs = append(errs, fmt.Sprintf("NATS_TLS_MODE must be empty, ONE_WAY_TLS or MUTUAL_TLS, got %q", natsSinkConfig.NatsTlsMode))
	}

	var err error
	if natsSinkConfig.PublishTimeout, err = utils.GetEnvDuration("NATS_PUBLISH_TIMEOUT", 5*time.Second); err != nil {
		errs = append(errs, err.Error())
	} else if natsSinkConfig.PublishTimeout <= 0 {
		errs = append(errs, "NATS_PUBLISH_TIMEOUT must be > 0")
	}
	if natsSinkConfig.PublishMaxRetries, err = utils.GetEnvInt("NATS_PUBLISH_MAX_RETRIES", 5); err != nil {
		errs = append(errs, err.Error())
	} else if natsSinkConfig.PublishMaxRetries < 0 {
		errs = append(errs, "NATS_PUBLISH_MAX_RETRIES must be >= 0")
	}
	if natsSinkConfig.PublishRetryBackoff, err = utils.GetEnvDuration("NATS_PUBLISH_RETRY_BACKOFF", 500*time.Millisecond); err != nil {
		errs = append(errs, err.Error())
	} else if natsSinkConfig.PublishRetryBackoff <= 0 {
		errs = append(errs, "NATS_PUBLISH_RETRY_BACKOFF must be > 0")
	}
	if natsSinkConfig.PublishRetryMaxBackoff, err = utils.GetEnvDuration("NATS_PUBLISH_RETRY_MAX_BACKOFF", 5*time.Second); err != nil {
		errs = append(errs, err.Error())
	} else if natsSinkConfig.PublishRetryMaxBackoff < natsSinkConfig.PublishRetryBackoff {
		errs = append(errs, "NATS_PUBLISH_RETRY_MAX_BACKOFF must be >= NATS_PUBLISH_RETRY_BACKOFF")
	}

	if natsSinkConfig.MaxUnackMsgs, err = utils.GetEnvInt("NATS_MAX_UNACK_MSGS", 1000); err != nil {
		errs = append(errs, err.Error())
	} else if natsSinkConfig.MaxUnackMsgs < 1 {
		errs = append(errs, "NATS_MAX_UNACK_MSGS must be >= 1")
	}

	natsSinkConfig.DedupEnabled = utils.GetEnvBool("NATS_DEDUP_ENABLED", true)

	if len(errs) > 0 {
		return natsSinkConfig, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}

	log.Println("NATS sink configuration loaded successfully")
	log.Println("NATS sink configuration details:")
	log.Printf("  NATS_URL: %s", natsSinkConfig.NatsUrl)
	log.Printf("  NATS_CLIENT_NAME: %s", natsSinkConfig.NatsClientName)
	log.Printf("  NATS_AUTH_MECHANISM: %s", natsSinkConfig.NatsAuthMechanism)
	log.Printf("  NATS_CREDS_FILE: %s", natsSinkConfig.NatsCredsFile)
	log.Printf("  NATS_TLS_ENABLED: %t", natsSinkConfig.NatsTlsEnabled)
	if natsSinkConfig.NatsTlsEnabled {
		log.Printf("  NATS_TLS_MODE: %s", natsSinkConfig.NatsTlsMode)
		log.Printf("  NATS_TLS_INSECURE_SKIP_VERIFY: %t", natsSinkConfig.NatsTlsInsecureSkipVerify)
	}
	log.Printf("  STREAM_NAME: %s", natsSinkConfig.StreamName)
	log.Printf("  NATS_SUBJECT_PREFIX: %s", natsSinkConfig.SubjectPrefix)
	log.Printf("  NATS_PUBLISH_TIMEOUT: %s", natsSinkConfig.PublishTimeout)
	log.Printf("  NATS_PUBLISH_MAX_RETRIES: %d", natsSinkConfig.PublishMaxRetries)
	log.Printf("  NATS_PUBLISH_RETRY_BACKOFF: %s", natsSinkConfig.PublishRetryBackoff)
	log.Printf("  NATS_PUBLISH_RETRY_MAX_BACKOFF: %s", natsSinkConfig.PublishRetryMaxBackoff)
	log.Printf("  NATS_MAX_UNACK_MSGS: %d", natsSinkConfig.MaxUnackMsgs)
	log.Printf("  NATS_DEDUP_ENABLED: %t", natsSinkConfig.DedupEnabled)
	return natsSinkConfig, nil
}

// PublishRetryBudget is the longest a single record can take to publish
// before the connector gives up on it: every attempt timing out, plus every
// backoff sleep in between.
func (c NatsSinkConfig) PublishRetryBudget() time.Duration {
	budget := time.Duration(c.PublishMaxRetries+1) * c.PublishTimeout
	backoff := c.PublishRetryBackoff
	for i := 0; i < c.PublishMaxRetries; i++ {
		budget += backoff
		backoff = min(2*backoff, c.PublishRetryMaxBackoff)
	}
	return budget
}
