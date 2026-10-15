package natsconn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// ConnState tracks NATS connection health so the health package can expose
// it on /readyz without polling nc.Status() (which has a race window
// between a poll and a real disconnect). It's updated only from the
// connection-state callbacks registered in Connect.
type ConnState struct {
	connected atomic.Bool
}

func (s *ConnState) Connected() bool { return s.connected.Load() }
func (s *ConnState) set(v bool)      { s.connected.Store(v) }

// Connect opens the one NATS connection this process uses for its lifetime
// and wraps it in a JetStream context. Callers must Close the returned
// connection during shutdown.
func Connect(natsSinkConfig config.NatsSinkConfig) (*nats.Conn, jetstream.JetStream, *ConnState, error) {
	state := &ConnState{}
	opts := []nats.Option{
		nats.Name(natsSinkConfig.NatsClientName),
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1), // ride out NATS blips instead of crash-looping the pod
		nats.ReconnectWait(2 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			state.set(false)
			log.Printf("natsconn: disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			state.set(true)
			log.Printf("natsconn: reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			state.set(false)
			log.Println("natsconn: connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("natsconn: async error (subject=%v): %v", subjectOf(sub), err)
		}),
	}

	if natsSinkConfig.NatsTlsEnabled {
		tlsConfig, err := buildNatsTlsConfig(natsSinkConfig)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("natsconn: set TLS config: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	opts, err := setNatsAuthConfig(opts, natsSinkConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("natsconn: set auth config: %w", err)
	}

	nc, err := nats.Connect(natsSinkConfig.NatsUrl, opts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("natsconn: connect: %w", err)
	}
	state.set(true)

	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(natsSinkConfig.MaxUnackMsgs),
		jetstream.WithPublishAsyncTimeout(natsSinkConfig.PublishTimeout),
	)
	if err != nil {
		nc.Close()
		return nil, nil, nil, fmt.Errorf("natsconn: jetstream: %w", err)
	}

	return nc, js, state, nil
}

// buildNatsTlsConfig builds the TLS config for NATS from the NATS_TLS_*
// settings: the CA from NATS_TLS_CA (required when TLS is enabled — see
// LoadNatsSinkConfig), plus a client certificate for NATS_TLS_MODE=MUTUAL_TLS.
func buildNatsTlsConfig(cfg config.NatsSinkConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.NatsTlsInsecureSkipVerify}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(unescapePEM(cfg.NatsTlsCaFileContent)); !ok {
		return nil, fmt.Errorf("NATS_TLS_CA does not contain a valid PEM certificate")
	}
	tlsConfig.RootCAs = pool

	if cfg.NatsTlsMode == "MUTUAL_TLS" {
		cert, err := tls.X509KeyPair(unescapePEM(cfg.NatsTlsCertFileContent), unescapePEM(cfg.NatsTlsKeyFileContent))
		if err != nil {
			return nil, fmt.Errorf("parse NATS_TLS_CERT/NATS_TLS_KEY: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// unescapePEM turns escaped "\n" sequences back into newlines, since env
// vars commonly can't carry literal ones.
func unescapePEM(pem string) []byte {
	return []byte(strings.ReplaceAll(pem, "\\n", "\n"))
}

// setNatsAuthConfig configures NATS authentication based on the provided configuration from the environment variables.
func setNatsAuthConfig(opts []nats.Option, cfg config.NatsSinkConfig) ([]nats.Option, error) {
	switch cfg.NatsAuthMechanism {
	case "TOKEN":
		opts = append(opts, nats.Token(cfg.NatsToken))
	case "USERNAME_PASSWORD":
		opts = append(opts, nats.UserInfo(cfg.NatsUsername, cfg.NatsPassword))
	case "JWT_NKEY_FILE":
		if cfg.NatsCredsFile != "" {
			opts = append(opts, nats.UserCredentials(cfg.NatsCredsFile))
		} else {
			opts = append(opts, nats.UserCredentialBytes([]byte(cfg.NatsJwtNKeyCreds)))
		}
	case "JWT_NKEY":
		opts = append(opts, nats.UserJWTAndSeed(cfg.NatsJwt, cfg.NatsNKey))
	case "NKEY":
		kp, err := nkeys.FromSeed([]byte(cfg.NatsUserSeed))
		if err != nil {
			return nil, fmt.Errorf("parse NATS_USER_SEED: %w", err)
		}
		publicKey, err := kp.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("derive public key from NATS_USER_SEED: %w", err)
		}
		opts = append(opts, nats.Nkey(publicKey, kp.Sign))
	}
	return opts, nil
}

// Close drains the connection so any pending outbound work flushes before
// the underlying network connection closes.
func Close(nc *nats.Conn) {
	if nc == nil {
		return
	}
	if err := nc.Drain(); err != nil {
		log.Printf("natsconn: drain failed, closing directly: %v", err)
		nc.Close()
	}
}

func subjectOf(sub *nats.Subscription) string {
	if sub == nil {
		return ""
	}
	return sub.Subject
}
