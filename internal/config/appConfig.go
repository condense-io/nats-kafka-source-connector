package config

import (
	"fmt"
	"log"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config/utils"
)

// AppConfig holds settings that belong to the process itself rather than to
// either end of the pipeline.
type AppConfig struct {
	HealthAddr string
	LogLevel   string

	// ShutdownGrace bounds how long an in-flight batch may keep publishing
	// and committing after shutdown is requested.
	ShutdownGrace time.Duration

	// PprofEnabled exposes Go's profiling endpoints on the health server.
	// Off by default — see health.registerPprof for the cost and exposure
	// this carries.
	PprofEnabled bool
}

func LoadAppConfig() (AppConfig, error) {
	var errs []string

	appConfig := AppConfig{
		HealthAddr:   utils.GetEnv("HEALTH_ADDR", ":8080"),
		LogLevel:     utils.GetEnv("LOG_LEVEL", "info"),
		PprofEnabled: utils.GetEnvBool("PPROF_ENABLED", false),
	}

	var err error
	if appConfig.ShutdownGrace, err = utils.GetEnvDuration("SHUTDOWN_GRACE_PERIOD", 25*time.Second); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return appConfig, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}

	log.Println("App configuration loaded successfully")
	log.Printf("  HEALTH_ADDR: %s", appConfig.HealthAddr)
	log.Printf("  LOG_LEVEL: %s", appConfig.LogLevel)
	log.Printf("  SHUTDOWN_GRACE_PERIOD: %s", appConfig.ShutdownGrace)
	log.Printf("  PPROF_ENABLED: %t", appConfig.PprofEnabled)
	return appConfig, nil
}

// ValidateCombined checks constraints that span the Kafka and NATS configs.
//
// Rebalances are blocked from the moment a batch is polled until it has
// been published and committed, so the slowest possible record — one that
// exhausts every publish retry — plus the time spent filling the batch
// must fit within the Kafka rebalance timeout. Otherwise a pending
// rebalance would get this member kicked from the group while it's still
// legitimately retrying.
func ValidateCombined(kafkaSourceConfig KafkaSourceConfig, natsSinkConfig NatsSinkConfig) error {
	worst := natsSinkConfig.PublishRetryBudget() + kafkaSourceConfig.KafkaMaxPollWait
	if worst >= kafkaSourceConfig.KafkaRebalanceTimeout {
		return fmt.Errorf("invalid configuration: worst-case time for one record (KAFKA_MAX_POLL_WAIT + NATS publish retry budget = %s) must be below KAFKA_REBALANCE_TIMEOUT (%s) — lower NATS_PUBLISH_TIMEOUT/NATS_PUBLISH_MAX_RETRIES or raise KAFKA_REBALANCE_TIMEOUT",
			worst, kafkaSourceConfig.KafkaRebalanceTimeout)
	}
	return nil
}
