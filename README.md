# Kafka → NATS JetStream Source Connector

A Go connector that consumes a Kafka topic as a consumer group, in batches, and publishes every record to a NATS JetStream stream — asynchronously, awaiting every record's PubAck — and commits Kafka offsets only once JetStream has stored the records up to them.

## Features

- **Kafka consumer group via [franz-go](https://github.com/twmb/franz-go)** — scale by adding replicas, up to the topic's partition count; Kafka assigns each partition to exactly one member.
- **Batched consumption** — up to `KAFKA_MAX_POLL_RECORDS` records per batch, or whatever has arrived `KAFKA_MAX_POLL_WAIT` after the batch's first record, with fetch sizes bounded by `KAFKA_FETCH_MAX_BYTES`/`KAFKA_FETCH_MAX_PARTITION_BYTES`.
- **Async, individually acked publishing** — the whole batch is sent with `PublishMsgAsync` (bounded by `NATS_MAX_UNACK_MSGS` unacked messages) and each PubAck is awaited; nothing new is polled until the batch is acked. Failed records are retried alone; `Nats-Msg-Id` dedup keeps retries from duplicating.
- **Key-hashed subjects** — `<NATS_SUBJECT_PREFIX>.<xxhash of the Kafka key>` for any key (binary, any length), `<NATS_SUBJECT_PREFIX>.<partition>` for records without a key — see [Subject routing](#subject-routing).
- **Header pass-through** — Kafka record headers become NATS headers, plus `Kafka-Topic`, `Kafka-Partition`, `Kafka-Offset`, `Kafka-Timestamp` and `Kafka-Key` — see [Headers](#headers).
- **At-least-once, with stream-side dedup** — offsets are committed only after publish; records replayed after a crash or rebalance carry the same `Nats-Msg-Id` and are dropped by the stream's duplicate window.
- **Fail loudly instead of skipping** — a record that can't be published after all retries is never skipped: the contiguous published prefix of each partition is committed and the process exits, so it restarts from the committed offsets.
- **Stream ownership stays external** — the connector never creates or modifies the stream; it verifies at startup that the stream exists and captures every subject under the prefix.
- **Same auth/TLS env contract as the sink connector** — NATS: `JWT_NKEY_FILE`, `JWT_NKEY`, `NKEY`, `TOKEN`, `USERNAME_PASSWORD`, TLS and mTLS; Kafka: SASL/SCRAM, TLS and mTLS. One ConfigMap/Secret works for both connectors.
- **Kubernetes-friendly** — `/healthz` and `/readyz`, graceful shutdown that lets an in-flight batch finish, and every setting is an env var.

## How it works

```
loop:
  batch := poll Kafka               (rebalances are blocked from here…)
  send every record with PublishMsgAsync      (≤ NATS_MAX_UNACK_MSGS unacked at once)
  await every record's PubAck
  retry only the records that failed          (backoff between rounds, up to NATS_PUBLISH_MAX_RETRIES)
  commit, per partition, the last record of its contiguous published prefix
  allow rebalance                   (…to here)
  if any record couldn't be published: exit
```

Rebalances are held off (`kgo.BlockRebalanceOnPoll`) from poll until after the commit, so a partition is never handed to another member while this one still has in-flight records from it. The cost is that a whole poll → publish → commit cycle must fit in `KAFKA_REBALANCE_TIMEOUT`; the connector refuses to start if the worst-case single-record retry budget plus `KAFKA_MAX_POLL_WAIT` doesn't.

Code layout mirrors the sink connector: [`internal/config`](internal/config) (one file per side — `kafkaSourceConfig.go`, `natsSinkConfig.go`, plus `appConfig.go` for process settings), [`internal/source`](internal/source/source.go) (broker-agnostic interface) with [`kafkasource`](internal/source/kafkasource/kafkasource.go), [`internal/natsconn`](internal/natsconn) (connection, stream verification, publisher, headers), [`internal/subject`](internal/subject/subject.go) (routing), and [`internal/pipeline`](internal/pipeline/pipeline.go), the only package that knows about both sides.

## Prerequisites

- Go 1.26.4 or later (only for building from source)
- A NATS server or cluster with JetStream enabled, and the **target stream already created** with a subject filter covering `<NATS_SUBJECT_PREFIX>.*` (e.g. `kafka.*` or `kafka.>`)
- A reachable Kafka cluster and an existing source topic

## Getting started (local)

```bash
cp .env.example .env
```

Fill in at least `NATS_URL`, `STREAM_NAME`, `NATS_SUBJECT_PREFIX`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` and `KAFKA_CONSUMER_GROUP`.

```bash
go run .
```

`.env` is loaded automatically on startup as a local-dev convenience and is never required — in Kubernetes the variables come from a ConfigMap/Secret.

## Subject routing

Each record goes to `<NATS_SUBJECT_PREFIX>.<hash>`, where `<hash>` is the 64-bit [xxHash](https://xxhash.com/) (XXH64) of the raw key bytes, as 16 lowercase hex digits. Any key works — binary, non-UTF-8, any length — and the result is always one safe subject token.

A record with a nil or empty key goes to `<NATS_SUBJECT_PREFIX>.<kafka partition>` instead.

| Kafka key | Partition | Subject (`NATS_SUBJECT_PREFIX=kafka`) |
|---|---|---|
| `a` | 0 | `kafka.d24ec4f1a98c6e5b` |
| `device-42` | 3 | `kafka.fa14548796d419f1` (same for every partition) |
| *(none)* | 3 | `kafka.3` |
| *(empty)* | 1 | `kafka.1` |

A hash token is always 16 characters and a partition number at most 10 digits, so the two routes never collide. Every subject is exactly one token below the prefix, so `<prefix>.*` always matches all of them. The original key is still available in the `Kafka-Key` header.

### Ordering guarantees

- **On the stream:** while nothing fails, records of a partition are stored in offset order, and so is every subject. If a publish fails mid-flight and is retried, that record is stored after records published behind it, so order on its subject can break. This is an accepted trade-off of async publishing.
- **Across partitions:** there is no ordering between partitions, same as in Kafka.
- **Replays:** records published but not yet committed when the pod dies are re-published by the partition's next owner. With `NATS_DEDUP_ENABLED=true` they're dropped by the stream — **as long as they arrive within the stream's `duplicate_window`** (JetStream default 2 minutes). If they arrive later, or with dedup disabled, the stream sees e.g. `A B C B C`. Size the window above your worst-case restart time.
- **Repartitioning the topic** remaps keys to different partitions, so for a short while the same key can be in flight from two partitions and be reordered. Avoid resizing topics whose per-key order matters.
- **Consumers:** the stream is ordered, but a JetStream consumer with `MaxAckPending > 1` and several workers processes messages of one subject concurrently. To process a key's messages in order downstream, consume that subject (or the whole stream) with a single in-order worker per subject, or `MaxAckPending=1`.
- **Subject cardinality:** every distinct key becomes its own subject. JetStream keeps per-subject state, so unbounded key spaces (e.g. a UUID per record) grow the stream's subject index and make `MaxMsgsPerSubject` apply per key. Topics like that are better served unkeyed, which falls back to partition subjects.

## Headers

Every Kafka record header is copied onto the NATS message (repeated keys are kept, in order), then these are added **unless the record already has a header of that name** (case-insensitive), in which case the record's value wins:

| Header | Value |
|---|---|
| `Kafka-Topic` | Source topic |
| `Kafka-Partition` | Partition number |
| `Kafka-Offset` | Record offset |
| `Kafka-Timestamp` | Record timestamp, Unix milliseconds |
| `Kafka-Key` | Record key — only for records that have one; carries the original key since the subject only holds its hash |

Two adjustments, both to keep upstream data from breaking the publish:

- **`Nats-*` headers are renamed to `Kafka-Orig-Nats-*`.** JetStream acts on headers in that namespace (`Nats-Msg-Id`, `Nats-Expected-*`, `Nats-TTL`, …); records that previously went NATS → Kafka through the sink connector carry them, and passing them through would override this connector's dedup ID or trigger publish expectations.
- **Headers whose name isn't a valid NATS header name** (e.g. containing spaces or `:`) are dropped, logged once per name — nats.go would otherwise reject the whole message. Header values containing CR/LF have them replaced by spaces by nats.go.

With dedup on, `Nats-Msg-Id` is set to `<topic>-<partition>-<offset>`.

## Failure handling

| Failure | What happens |
|---|---|
| Publish timeout, no responders, 5xx JetStream error | Retried up to `NATS_PUBLISH_MAX_RETRIES` times with backoff (`NATS_PUBLISH_RETRY_BACKOFF`, doubling, capped at `NATS_PUBLISH_RETRY_MAX_BACKOFF`) |
| Retries exhausted | The rest of the batch is still published and acked; for every partition the contiguous published prefix is committed (records after a failed one are not, even if they were stored — a restart re-sends them and dedup drops them); the process exits non-zero and restarts from the committed offsets |
| Non-retryable error — message over `max_payload`, invalid subject or headers, 4xx JetStream error (e.g. over the stream's max message size, subject bound to another stream) | Same as retries exhausted, without retrying that record |
| Offset commit fails | Logged. Commits are a high-water mark, so the next successful one covers it; until then a restart replays those records, absorbed by dedup |
| Stream missing or doesn't cover `<prefix>.*` | Startup fails |
| Stream's `duplicate_window` ≤ the publish retry budget (and dedup on) | Warning logged at startup: a record retried after the window may be stored twice |

Exiting on failure means a record that can **never** be published (e.g. larger than the stream allows) puts the pod in a crash loop until the record, stream limits or NATS `max_payload` are fixed — deliberately, rather than silently skipping data.

## Configuration reference

Every setting is an environment variable. **Required** variables make the connector exit at startup with a descriptive error if unset. See [`kafkaSourceConfig.go`](internal/config/kafkaSourceConfig.go), [`natsSinkConfig.go`](internal/config/natsSinkConfig.go) and [`appConfig.go`](internal/config/appConfig.go) for the exact validation.

### NATS & stream

| Variable | Required | Default | Description |
|---|---|---|---|
| `NATS_URL` | Yes | — | NATS server URL(s); comma-separate multiple URLs for a cluster. |
| `NATS_CLIENT_NAME` | No | `kafka-source-connector` | Client name reported to NATS (visible in `/connz`). |
| `STREAM_NAME` | Yes | — | Target stream. Must already exist and have a subject filter covering `<NATS_SUBJECT_PREFIX>.*`. Every publish also sets `Nats-Expected-Stream` to this name. |
| `NATS_SUBJECT_PREFIX` | Yes | — | Literal subject prefix (no wildcards, no leading `$`), e.g. `kafka` or `kafka.orders`. |
| `NATS_PUBLISH_TIMEOUT` | No | `5s` | How long one publish waits for its PubAck. Must be > 0. |
| `NATS_PUBLISH_MAX_RETRIES` | No | `5` | Retries after the first attempt before giving up and exiting. Must be ≥ 0. |
| `NATS_PUBLISH_RETRY_BACKOFF` | No | `500ms` | Initial sleep between retries; doubles each retry. Must be > 0. |
| `NATS_PUBLISH_RETRY_MAX_BACKOFF` | No | `5s` | Cap on the retry sleep. Must be ≥ `NATS_PUBLISH_RETRY_BACKOFF`. |
| `NATS_MAX_UNACK_MSGS` | No | `1000` | Max unacked async publishes in flight (must be ≥ 1). The whole batch is sent async and each ack awaited; failed records are retried, which can reorder them behind later records. |
| `NATS_DEDUP_ENABLED` | No | `true` | Set `Nats-Msg-Id=<topic>-<partition>-<offset>` so replayed records are dropped within the stream's duplicate window. |

### NATS authentication (optional)

The same variables and values as the sink connector. `NATS_AUTH_MECHANISM` is compared **case-sensitively**.

| `NATS_AUTH_MECHANISM` | Required variables |
|---|---|
| unset or `NONE` | — (no authentication) |
| `JWT_NKEY_FILE` | `NATS_CREDS_FILE` (path to a `.creds` file) or `NATS_CREDS` (its content inline); the file wins if both are set |
| `JWT_NKEY` | `NATS_JWT` (user JWT) and `NATS_NKEY` (user NKey seed) |
| `NKEY` | `NATS_USER_SEED` (raw NKey seed, no JWT) |
| `TOKEN` | `NATS_TOKEN` |
| `USERNAME_PASSWORD` | `NATS_USERNAME`, `NATS_PASSWORD` |

`NATS_CREDS_FILE` set with any other mechanism is rejected at startup.

### NATS TLS (optional)

| Variable | Default | Description |
|---|---|---|
| `NATS_TLS_ENABLED` | `false` | Enable TLS to NATS (parsed as a general boolean: `true`, `1`, …). |
| `NATS_TLS_CA` | — | CA to trust, inline PEM (escaped `\n` allowed). **Required** when TLS is enabled. |
| `NATS_TLS_MODE` | *(empty)* | Optional. `ONE_WAY_TLS` (or unset) for server TLS only, or `MUTUAL_TLS` to also present a client certificate; any other value is rejected at startup. `MUTUAL_TLS` requires `NATS_TLS_ENABLED=true`. |
| `NATS_TLS_CERT` / `NATS_TLS_KEY` | — | Client certificate and key, inline PEM. Both required for `MUTUAL_TLS`; ignored (with a warning) otherwise. |
| `NATS_TLS_INSECURE_SKIP_VERIFY` | `false` | Skip server certificate verification. Development only. |

### Kafka consumer

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | — | Comma-separated broker addresses. |
| `KAFKA_TOPIC` | Yes | — | The single topic to consume. |
| `KAFKA_CONSUMER_GROUP` | Yes | — | Consumer group ID; all replicas share it. |
| `KAFKA_CONSUMER_CLIENT_ID` | No | hostname | Client ID (the pod name in Kubernetes). |
| `KAFKA_CONSUME_FROM_OFFSET` | No | `earliest` | `earliest` or `latest` — where to start on partitions the group has no committed offset for. |
| `KAFKA_MAX_POLL_RECORDS` | No | `500` | Maximum records per batch. Like every numeric and duration setting in this table, must be > 0. |
| `KAFKA_MAX_POLL_WAIT` | No | `1s` | Once a batch's first record arrives, how long to keep polling to fill it before publishing a partial batch. |
| `KAFKA_FETCH_MAX_BYTES` | No | `52428800` (50MiB) | Maximum bytes per fetch response. The client keeps up to one fetch in flight per broker, so buffered data is bounded by roughly brokers × this. |
| `KAFKA_FETCH_MAX_PARTITION_BYTES` | No | `1048576` (1MiB) | Maximum bytes per partition per fetch. Must be ≤ `KAFKA_FETCH_MAX_BYTES`. |
| `KAFKA_FETCH_MIN_BYTES` | No | `1` | Broker holds a fetch until this many bytes are available (or `KAFKA_FETCH_MAX_WAIT` passes). Raise to get fuller batches on busy topics. Must be ≤ `KAFKA_FETCH_MAX_BYTES`. |
| `KAFKA_FETCH_MAX_WAIT` | No | `500ms` | Broker-side wait for `KAFKA_FETCH_MIN_BYTES`. |
| `KAFKA_SESSION_TIMEOUT` | No | `45s` | Group session timeout (heartbeat liveness). |
| `KAFKA_REBALANCE_TIMEOUT` | No | `60s` | franz-go's analogue of `max.poll.interval.ms`: a poll → publish → commit cycle must fit in it. Startup fails unless `KAFKA_MAX_POLL_WAIT` + (`NATS_PUBLISH_MAX_RETRIES`+1) × `NATS_PUBLISH_TIMEOUT` + all retry backoffs is below it. |

### Kafka authentication & TLS

The same variables and values as the sink connector.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_SECURITY_PROTOCOL` | `NONE` | `NONE` or `SASL_PLAINTEXT` (SASL/SCRAM). Case-insensitive, normalized to uppercase. |
| `KAFKA_SCRAM_AUTH_MECHANISM` | `SHA512` | `SHA256` or `SHA512`, for `SASL_PLAINTEXT`. Case-insensitive. |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | — | Required for `SASL_PLAINTEXT`. |
| `KAFKA_TLS_ENABLED` | `false` | Enable TLS to the brokers (general boolean), independently of `KAFKA_SECURITY_PROTOCOL` — `SASL_PLAINTEXT` + TLS is Kafka's `SASL_SSL`, `NONE` + TLS is Kafka's `SSL`. |
| `KAFKA_TLS_CA` | — | CA to trust, inline PEM. Optional — unset uses the system trust store (managed Kafka with public certificates). |
| `KAFKA_TLS_MODE` | — (**required** when TLS is enabled) | `ONE_WAY_TLS` for server-only TLS, or `MUTUAL_TLS` to also present a client certificate. Unset or any other value is rejected when `KAFKA_TLS_ENABLED=true`. |
| `KAFKA_TLS_CERT` / `KAFKA_TLS_KEY` | — | Client certificate and key, inline PEM (escaped `\n` allowed). Both required for `MUTUAL_TLS`; ignored (with a warning) for `ONE_WAY_TLS`. |
| `KAFKA_TLS_INSECURE_SKIP_VERIFY` | `false` | Skip broker certificate verification. Development only. |

Any of `KAFKA_TLS_CA`, `KAFKA_TLS_MODE`, `KAFKA_TLS_CERT` or `KAFKA_TLS_KEY` set while `KAFKA_TLS_ENABLED` is false is rejected at startup, rather than silently connecting without TLS.

### Runtime

| Variable | Required | Default | Description |
|---|---|---|---|
| `SHUTDOWN_GRACE_PERIOD` | No | `25s` | On `SIGINT`/`SIGTERM`, how long an in-flight batch may keep publishing and committing. Keep below the pod's `terminationGracePeriodSeconds`. |
| `HEALTH_ADDR` | No | `:8080` | Health server address. |
| `LOG_LEVEL` | No | `info` | Accepted but not yet wired to log verbosity. |
| `PPROF_ENABLED` | No | `false` | Expose `/debug/pprof/` on the health server. |

## Health & readiness endpoints

- **`/healthz`** — `200` once the process is up.
- **`/readyz`** — `200` only when the NATS connection is up and a Kafka broker `Ping` succeeds; otherwise `503`.


## Running with Docker

```bash
docker build -t kafka-source-connector:local .
docker run --rm --env-file .env -p 8080:8080 kafka-source-connector:local
```

