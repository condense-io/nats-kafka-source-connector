// main is the composition root: load config, connect once, wire the
// pipeline together, run until asked to stop, then shut down in order.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"condense.zeliot.in/nats-kafka-source-connector/internal/config"
	"condense.zeliot.in/nats-kafka-source-connector/internal/health"
	"condense.zeliot.in/nats-kafka-source-connector/internal/natsconn"
	"condense.zeliot.in/nats-kafka-source-connector/internal/pipeline"
	"condense.zeliot.in/nats-kafka-source-connector/internal/source/kafkasource"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	appConfig, err := config.LoadAppConfig()
	if err != nil {
		return err
	}
	kafkaSourceConfig, err := config.LoadKafkaSourceConfig()
	if err != nil {
		return err
	}
	natsSinkConfig, err := config.LoadNatsSinkConfig()
	if err != nil {
		return err
	}
	if err := config.ValidateCombined(kafkaSourceConfig, natsSinkConfig); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-quit
		log.Printf("main: received %s, starting graceful shutdown", sig)
		cancel()
	}()

	nc, js, connState, err := natsconn.Connect(natsSinkConfig)
	if err != nil {
		return err
	}
	defer natsconn.Close(nc)

	if err := natsconn.VerifyStream(ctx, js, natsSinkConfig); err != nil {
		return err
	}

	// Deferred after the NATS connection, so it runs before it: leave the
	// Kafka group first, then drain NATS.
	src, err := kafkasource.New(kafkaSourceConfig)
	if err != nil {
		return err
	}
	defer func() {
		if err := src.Close(context.Background()); err != nil {
			log.Printf("main: source close error: %v", err)
		}
	}()

	pub := natsconn.NewPublisher(js, natsSinkConfig)

	healthSrv := health.NewServer(appConfig.HealthAddr, appConfig.PprofEnabled,
		func(ctx context.Context) error {
			if !connState.Connected() {
				return fmt.Errorf("nats: not connected")
			}
			return nil
		},
		src.Ready,
	)

	healthDone := make(chan error, 1)
	go func() { healthDone <- healthSrv.Start(ctx) }()

	log.Printf("main: connector started (source=%s topic=%s group=%s stream=%s subject_prefix=%s max_poll_records=%d)",
		src.Name(), kafkaSourceConfig.KafkaTopic, kafkaSourceConfig.KafkaConsumerGroup,
		natsSinkConfig.StreamName, natsSinkConfig.SubjectPrefix, kafkaSourceConfig.KafkaMaxPollRecords)

	orch := pipeline.New(src, pub, appConfig.ShutdownGrace)
	orchErr := orch.Run(ctx)
	if orchErr != nil {
		log.Print(orchErr)
	}

	// Run can also return on its own (a record that couldn't be published),
	// so stop the health server explicitly rather than waiting on a signal.
	cancel()
	if err := <-healthDone; err != nil {
		log.Printf("main: health server error: %v", err)
	}
	return orchErr
}
