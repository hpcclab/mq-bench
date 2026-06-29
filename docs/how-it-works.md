# How mq-bench Works

This document explains the repository in simple words. It is meant for someone
who wants to understand the project before changing code, running benchmarks, or
reading the result folders.

`mq-bench` is a Rust benchmark tool for message brokers. It can talk to several
messaging systems through one common interface, run publisher/subscriber or
request/reply workloads, and write measurements such as throughput, latency,
connection counts, failures, duplicates, and gaps.

The short version is:

1. You start one or more brokers, usually with Docker Compose or a remote setup.
2. You run the `mq-bench` binary in a role such as `pub`, `sub`, `req`, or `qry`.
3. The CLI chooses a transport such as Zenoh, MQTT, Redis, NATS, or AMQP.
4. The selected role sends or receives benchmark payloads through that transport.
5. Shared metric code records counts and latency.
6. CSV output and orchestration scripts turn those raw snapshots into benchmark
   results and plots.

## Big Picture

The repository has three main layers:

- The Rust benchmark binary in `src/`.
- Shell and Python automation in `scripts/`.
- Broker configs, Docker setup, and saved results in `config/`, `setup/`,
  `final_results/`, `journal-results/`, and `example_results/`.

The Rust binary is the core. The scripts mostly run the binary many times with
different numbers of publishers, subscribers, payload sizes, rates, transports,
and broker endpoints.

## Main Folders

`src/`

The Rust implementation. This contains the CLI, role runners, transport
adapters, payload format, rate control, metrics, output, and crash injection.

`src/main.rs`

The command-line entry point. It defines all CLI commands and turns CLI flags
into config structs for the role modules.

`src/lib.rs`

Exports the internal modules so tests and other code can reuse them.

`src/roles/`

The workload roles:

- `publisher.rs` sends messages to one topic.
- `subscriber.rs` subscribes to a topic expression and measures received
  messages.
- `requester.rs` sends request/reply queries and measures response time.
- `queryable.rs` replies to request/reply queries.
- `multi_topic.rs` runs many logical publishers or subscribers inside one
  process.
- `reliable_publisher.rs` is a special MQTT publisher that waits for broker ACKs
  and resumes from the last confirmed sequence after reconnects.

`src/transport/`

Adapters for each messaging system. All adapters implement the same `Transport`
trait so the role code does not need to know which broker is behind it.

`scripts/`

Experiment automation. These scripts build the binary if needed, start or stop
services, run combinations of benchmark settings, collect Docker statistics, and
produce summary CSVs or plots.

`config/`

Broker configuration files, for example MQTT broker configs and Zenoh router
configs.

`docker-compose.yml`

Local broker stack. It exposes services for Redis, NATS, MQTT brokers,
RabbitMQ/AMQP, Artemis MQTT, and Zenoh routers depending on the compose setup.

`tests/`

Integration and smoke tests for transport behavior, mock transport behavior, and
crash/failure paths.

`final_results/`, `journal-results/`, `example_results/`

Saved benchmark outputs, plots, summaries, and analysis documents. These are
not required to understand the runtime logic, but they show what the tool
produces after larger experiment runs.

## The CLI Flow

When you run:

```bash
./target/release/mq-bench pub --engine mqtt --connect host=127.0.0.1 --connect port=1883
```

the following happens:

1. `src/main.rs` parses the command with `clap`.
2. It reads global options such as `--run-id`, `--log-level`, and
   `--snapshot-interval`.
3. It matches the subcommand, in this case `pub`.
4. It parses `--engine` into an `Engine` enum.
5. It parses repeated `--connect KEY=VALUE` flags into `ConnectOptions`.
6. It creates shared `Stats` and an `OutputWriter`.
7. It builds a role config such as `PublisherConfig`.
8. It calls the role function, such as `run_publisher`.

Most commands follow this same shape. `main.rs` does the outer wiring. The role
modules do the actual benchmark work.

## Roles

The tool is organized around roles. A role is one behavior the process performs
during a benchmark.

### Publisher: `pub`

The publisher sends messages to a topic.

Important options:

- `--engine` chooses the transport.
- `--connect KEY=VALUE` passes transport-specific connection settings.
- `--topic-prefix` chooses the topic.
- `--topics` spreads publishers across multiple topic suffixes.
- `--publishers` starts multiple logical publishers in one process.
- `--payload` chooses message size in bytes.
- `--rate` limits messages per second per publisher.
- `--duration` decides how long the run lasts.
- `--csv` writes metric snapshots to a file.

The normal publisher uses this loop:

1. Connect to the selected transport.
2. Create a publisher handle for the topic.
3. Wait for the rate controller if a rate was configured.
4. Generate a payload with a sequence number and timestamp.
5. Publish the payload.
6. Record a sent-message metric.
7. Repeat until duration, Ctrl+C, or a failure condition.

If crash injection is enabled, the publisher can force-disconnect, sleep for a
repair time, reconnect, and continue with the next sequence number.

### Subscriber: `sub`

The subscriber receives messages and measures latency.

Important options:

- `--expr` is the subscription expression, such as `bench/topic` or `bench/**`.
- `--subscribers` starts multiple logical subscribers.
- `--csv` writes metric snapshots.

The subscriber tries to do as little work as possible inside the transport
callback. For each received message it:

1. Copies the first 24 bytes of the payload. That is the benchmark header.
2. Records an estimated receive timestamp.
3. Sends the small header and timestamp into an internal channel.

A background worker drains that channel in batches. It parses headers, updates
sequence tracking, computes latency, and records metrics.

This design keeps the hot receive callback light, which matters when a broker is
delivering many messages per second.

### Requester: `req`

The requester tests request/reply behavior. It sends requests to a key or
subject and waits for replies.

Important options:

- `--key-expr` chooses where requests are sent.
- `--qps` limits requests per second.
- `--concurrency` limits how many requests can be in flight.
- `--timeout` sets the maximum wait per request.
- `--duration` sets run length.

The requester keeps a set of in-flight async tasks. It starts new requests until
the concurrency limit is reached, records response latency when replies arrive,
and records errors on timeout or transport failure.

### Queryable: `qry`

The queryable is the responder side for request/reply tests.

Important options:

- `--serve-prefix` chooses one or more prefixes to answer.
- `--reply-size` chooses reply payload size.
- `--proc-delay` simulates processing delay before replying.

For every incoming query, it optionally waits for the processing delay, sends a
generated reply payload, and records a sent metric.

### Multi-topic Publisher: `mt-pub`

The multi-topic publisher drives many logical topics from one process. It is
useful when experiments need many keys without starting thousands of OS
processes.

It builds topic names like:

```text
{prefix}/t{tenant}/r{region}/svc{service}/k{shard}
```

The dimensions come from:

- `--tenants`
- `--regions`
- `--services`
- `--shards`

The `--mapping` option controls how publisher indexes map to those dimensions:

- `mdim` walks the dimensions in a predictable order.
- `hash` spreads indexes using a hash.

`mt-pub` can use either:

- one shared transport, or
- one transport per key.

Shared transport mode reduces connection overhead. Per-key transport mode is
useful when the benchmark is about many independent client connections or when
per-topic crash injection is needed.

### Multi-topic Subscriber: `mt-sub`

The multi-topic subscriber mirrors `mt-pub`. It subscribes to many generated
keys and aggregates receive metrics across them.

Like `mt-pub`, it can share one transport or use one transport per key.

### Reliable MQTT Publisher: `rel-pub`

`rel-pub` is special. It only supports MQTT.

The normal publisher counts a message as sent once the publish call succeeds.
The reliable publisher waits for MQTT broker confirmation:

- QoS 1 waits for `PUBACK`.
- QoS 2 waits for `PUBCOMP`.
- QoS 0 does not wait for an ACK.

It tracks the last confirmed sequence number. If a crash or reconnect happens,
it resumes from the last confirmed sequence instead of blindly moving forward.
This is useful for QoS and reliability experiments.

## Transport Abstraction

The central abstraction is in `src/transport/mod.rs`.

The `Transport` trait exposes a small common API:

- `subscribe(expr, handler)`
- `create_publisher(topic)`
- `request(subject, payload)`
- `register_queryable(subject, handler)`
- `shutdown()`
- `health_check()`
- `force_disconnect()`

The role code calls this trait. The transport adapters hide broker-specific
details.

For example:

- The publisher role calls `create_publisher()` and then `publish()`.
- The subscriber role calls `subscribe()` and receives messages in a handler.
- The requester role calls `request()`.
- The queryable role calls `register_queryable()`.

This is why the same benchmark role can run against MQTT, Redis, NATS, Zenoh,
or RabbitMQ/AMQP.

## Transport Adapters

### Zenoh

File: `src/transport/zenoh.rs`

Zenoh uses a `zenoh::Session`. It supports:

- pub/sub through declared publishers and subscribers,
- request/reply through Zenoh `get` and queryables,
- optional reliability mapping from the `qos` connect option.

Connection options include `endpoint`, `endpoints`, and `mode`.

### MQTT

File: `src/transport/mqtt.rs`

MQTT uses `rumqttc`. It supports pub/sub and MQTT QoS settings.

Connection options include:

- `host`
- `port`
- `username`
- `password`
- `qos`
- `client_id`
- `clean_session`
- packet size settings such as `max_packet`, `max_in`, and `max_out`

The adapter creates dedicated MQTT clients for publishers and subscribers. It
also maps the benchmark's slash-style topic expressions to MQTT topic filters.

The normal MQTT transport does not implement request/reply. Request/reply tests
should use transports that implement it, such as Zenoh, Redis, or NATS.

### Redis

File: `src/transport/redis.rs`

Redis uses:

- Redis Pub/Sub for publish/subscribe.
- Redis lists for a simple request/reply baseline.

For request/reply, the requester pushes a request into a Redis list and waits on
a reply list. The queryable blocks on the request list and pushes the reply.

Connection option:

- `url`, defaulting to `redis://127.0.0.1:6379`

### NATS

File: `src/transport/nats.rs`

NATS uses `async-nats`. It supports:

- publish/subscribe,
- request/reply.

The adapter maps slash-separated benchmark topics to dot-separated NATS
subjects. For example:

```text
bench/topic/1 -> bench.topic.1
bench/**      -> bench.>
```

Connection options can be `url`, `endpoint`, or `host` plus `port`.

### AMQP / RabbitMQ

File: `src/transport/amqp.rs`

AMQP uses `lapin` and RabbitMQ's built-in `amq.topic` exchange.

It supports publish/subscribe. It does not currently implement request/reply.

The adapter maps slash-separated benchmark topics to dot-separated AMQP routing
keys.

Connection options can include:

- `url`
- `host`
- `port`
- `user`
- `pass`
- `vhost`

## Topic and Expression Conventions

The CLI uses slash-style topics everywhere:

```text
bench/topic
bench/topic/1
bench/**
```

Adapters translate this into each broker's native style:

- MQTT can use slash-style topics directly.
- Redis maps `/**` to Redis glob-like patterns.
- NATS maps slashes to dots and `/**` to `>`.
- AMQP maps slashes to dots and uses topic routing keys.
- Zenoh can use the key expression directly.

This keeps benchmark commands consistent across engines.

## Payload Format

Benchmark messages are generated in `src/payload.rs`.

Every payload starts with a 24-byte header:

```text
bytes 0..8    sequence number
bytes 8..16   UNIX timestamp in nanoseconds
bytes 16..24  payload size
```

After the header, the rest of the payload is filled with a repeating byte
pattern.

The sequence number helps detect missing or duplicate messages. The timestamp
lets subscribers compute end-to-end latency:

```text
latency = subscriber_receive_time - publisher_timestamp
```

The configured payload size includes the 24-byte header. So `--payload 1024`
means 1024 total bytes, not 1024 bytes plus a header.

## Time and Latency

Publishers write a UNIX nanosecond timestamp into each message header.

Subscribers estimate receive time using `src/time_sync.rs`. That module caches a
base `SystemTime` plus `Instant`, then computes current UNIX nanoseconds from
the monotonic clock. This is faster than calling `SystemTime::now()` for every
received message.

For accurate cross-machine latency, publisher and subscriber machines still need
reasonably synchronized clocks. The code can compute latency only from the
timestamps it sees.

## Rate Control

Rate limiting is implemented in `src/rate.rs`.

The `RateController` is a token bucket:

- tokens represent permission to send messages,
- tokens are refilled on a timer,
- small bursts are allowed to smooth scheduler jitter,
- fractional tokens are tracked with fixed-point math.

If `--rate` or `--qps` is omitted, zero, or negative, most roles run without
rate limiting and try to go as fast as possible.

## Metrics

Metrics are collected by `src/metrics/stats.rs`.

The main counters are:

- sent messages,
- received messages,
- errors,
- total connections,
- active connections,
- connection attempts,
- connection failures,
- injected crashes,
- reconnects,
- reconnect failures,
- duplicates,
- gaps.

Latency is stored in an HDR histogram. Snapshots include:

- p25 latency,
- p50 latency,
- p75 latency,
- p95 latency,
- p99 latency,
- min latency,
- max latency,
- mean latency,
- standard deviation,
- sample count.

Throughput is calculated two ways:

- total throughput over the full run,
- interval throughput since the previous snapshot.

## CSV Output

CSV output is handled by `src/output.rs`.

If `--csv path/to/file.csv` is provided, the parent directory is created and
snapshots are written to that file. Otherwise snapshots go to stdout.

The CSV columns are:

```text
timestamp,
sent_count,
received_count,
error_count,
total_throughput,
interval_throughput,
latency_ns_p25,
latency_ns_p50,
latency_ns_p75,
latency_ns_p95,
latency_ns_p99,
latency_ns_min,
latency_ns_max,
latency_ns_mean,
latency_ns_stddev,
latency_sample_count,
connections,
active_connections,
connection_attempts,
connection_failures,
crashes_injected,
reconnects,
reconnect_failures,
duplicate_count,
gap_count
```

`head_loss` is tracked internally in `Stats`, but the current CSV row does not
include it.

## Sequence Tracking

Sequence tracking is implemented in `src/metrics/sequence.rs`.

The subscriber records every sequence number it sees. From that it can report:

- duplicates: the same sequence was seen more than once,
- gaps: sequence numbers missing between the smallest and largest seen values,
- head loss: messages missing before the first received sequence.

This is useful for reliability and failure testing because a test can show not
only how many messages arrived, but also whether they arrived once and in a
complete sequence range.

## Crash Injection

Crash injection is implemented in `src/crash.rs`.

It simulates failures using:

- MTTF: mean time to failure,
- MTTR: mean time to repair,
- crash count,
- optional RNG seed.

Failure times are sampled from a capped exponential distribution. In plain
words, crashes are random but centered around the configured average.

When a crash is triggered, roles try to force-close the transport instead of
doing a graceful shutdown. That matters for reliability tests because a graceful
disconnect can tell the broker what happened, while a hard disconnect is closer
to network loss, process death, or power loss.

If retry is enabled, the role waits for a repair time, reconnects, and continues
the run.

## Retry Behavior

Connection retry settings live in `ConnectOptions`:

- `retry_enabled`
- `retry_count`
- `retry_delay_ms`
- `retry_max_delay_ms`

The CLI exposes these as flags on several roles:

- `--enable-retry`
- `--retry-count`
- `--retry-delay`

Retries are mainly useful for crash injection and remote experiments where a
broker might not be immediately ready.

## Shared Stats and Aggregation

When `main.rs` starts multiple publishers or subscribers in one process, it
usually creates one shared `Stats` object.

Each logical worker records into the same stats object. A separate snapshot task
writes aggregate CSV rows every `--snapshot-interval` seconds.

This is why a command like:

```bash
mq-bench pub --publishers 10 --csv pub.csv
```

produces one aggregate CSV stream instead of ten unrelated files.

## How Scripts Fit In

The scripts in `scripts/` are wrappers around the Rust binary.

Common responsibilities:

- build the release binary if it is missing,
- start or stop Docker Compose services,
- choose the right `--engine` and `--connect` flags,
- run publishers and subscribers together,
- collect broker CPU, memory, and network data from Docker,
- sweep over payload sizes, rates, subscriber counts, or transports,
- write raw CSV files,
- produce summary CSVs and plots.

`scripts/lib.sh` contains shared helpers used by many scripts.

Examples:

- `run_baseline.sh` runs simpler baseline cases.
- `run_fanout.sh` runs fanout scenarios.
- `run_queries.sh` runs request/reply scenarios.
- `orchestrate_latency_vs_payload.sh` sweeps latency tests by payload size.
- `orchestrate_throughput_vs_pairs.sh` sweeps throughput with client pairs.
- `orchestrate_fanout_under_steady_load.sh` runs fanout where subscriber count
  grows and publisher count grows more slowly.
- `plot_results.py`, `plot_fanout_sweep.py`, `plot_latency_bars.py`, and
  `plot_qos_comparison.py` turn CSV outputs into figures.

The scripts are useful for paper-scale experiments. For quick development, it
is usually easier to run the Rust binary directly.

## A Simple Pub/Sub Run

Start local services:

```bash
docker compose up -d
```

Build the binary:

```bash
cargo build --release
```

Start a subscriber:

```bash
./target/release/mq-bench sub \
  --engine mqtt \
  --connect host=127.0.0.1 \
  --connect port=1883 \
  --expr bench/topic \
  --csv artifacts/simple/sub.csv
```

Start a publisher in another terminal:

```bash
./target/release/mq-bench pub \
  --engine mqtt \
  --connect host=127.0.0.1 \
  --connect port=1883 \
  --topic-prefix bench/topic \
  --payload 1024 \
  --rate 100 \
  --duration 30 \
  --csv artifacts/simple/pub.csv
```

The publisher records send-side metrics. The subscriber records receive-side
metrics and latency.

## A Simple Request/Reply Run

Start a queryable:

```bash
./target/release/mq-bench qry \
  --engine nats \
  --connect host=127.0.0.1 \
  --connect port=4222 \
  --serve-prefix bench/topic \
  --reply-size 256
```

Start a requester:

```bash
./target/release/mq-bench req \
  --engine nats \
  --connect host=127.0.0.1 \
  --connect port=4222 \
  --key-expr bench/topic \
  --qps 1000 \
  --concurrency 32 \
  --timeout 2000 \
  --duration 30 \
  --csv artifacts/simple/req.csv
```

The requester records query throughput, errors, and response latency.

## A Simple Multi-topic Run

Run subscribers:

```bash
./target/release/mq-bench mt-sub \
  --engine nats \
  --connect host=127.0.0.1 \
  --connect port=4222 \
  --topic-prefix bench/mtopic \
  --tenants 10 \
  --regions 2 \
  --services 5 \
  --shards 10 \
  --subscribers 100 \
  --duration 60 \
  --csv artifacts/simple/mt-sub.csv
```

Run publishers:

```bash
./target/release/mq-bench mt-pub \
  --engine nats \
  --connect host=127.0.0.1 \
  --connect port=4222 \
  --topic-prefix bench/mtopic \
  --tenants 10 \
  --regions 2 \
  --services 5 \
  --shards 10 \
  --publishers 100 \
  --payload 1024 \
  --rate 10 \
  --duration 60 \
  --csv artifacts/simple/mt-pub.csv
```

This sends traffic across many generated keys while keeping all metrics
aggregated inside each process.

## How a Message Travels Through the System

For a basic pub/sub latency test:

1. The publisher role asks the transport adapter for a publisher handle.
2. The publisher generates a payload with sequence number and timestamp.
3. The transport adapter sends the bytes to the broker.
4. The broker delivers those bytes to one or more subscribers.
5. The subscriber adapter receives the broker message.
6. The subscriber callback copies the 24-byte header and records receive time.
7. The subscriber worker parses the sequence and timestamp.
8. The worker computes latency and updates `Stats`.
9. The snapshot task writes a CSV row.
10. Plot scripts or analysis scripts read the CSV later.

## Adding a New Transport

To add another broker:

1. Add a new file in `src/transport/`.
2. Implement the `Transport` trait for that broker.
3. Implement `Publisher`, `Subscription`, and optionally request/reply support.
4. Add a new `Engine` variant in `src/transport/mod.rs`.
5. Add parsing for the engine name in `src/transport/config.rs`.
6. Add the builder case in `TransportBuilder`.
7. Add Cargo dependencies and feature flags if needed.
8. Add smoke tests or mock-style tests.
9. Update README/docs with connection examples.

The role modules should not need major changes if the new adapter follows the
existing trait.

## Adding a New Workload Role

To add another kind of benchmark:

1. Create a new module under `src/roles/`.
2. Define a config struct for role settings.
3. Use `TransportBuilder::connect_with_retry` to connect.
4. Use the transport trait instead of directly using a broker client.
5. Record measurements through `Stats`.
6. Write snapshots through `OutputWriter` or use shared stats from `main.rs`.
7. Add a new CLI subcommand in `src/main.rs`.
8. Add tests or a small script example.

## Common Development Checks

Format:

```bash
cargo fmt
```

Build:

```bash
cargo build
```

Run tests:

```bash
cargo test
```

Build optimized binary:

```bash
cargo build --release
```

Run with logs:

```bash
./target/release/mq-bench --log-level debug sub --engine nats --expr bench/**
```

## Important Things to Remember

- Payload size includes the 24-byte benchmark header.
- Latency assumes publisher and subscriber clocks are synchronized.
- `--rate` is per publisher for publisher-style roles.
- `--qps` is request rate for request/reply roles.
- AMQP currently supports pub/sub but not request/reply.
- The normal MQTT adapter supports pub/sub; reliable ACK-focused publishing is
  handled by `rel-pub`.
- Shared stats mean a CSV can represent many logical workers in one process.
- Stored result folders are outputs, not part of the runtime path.
- The scripts are experiment orchestration around the Rust binary, not a
  replacement for the binary itself.

## Mental Model

Think of the project like this:

```text
CLI command
  -> role config
  -> role runner
  -> common Transport trait
  -> broker-specific adapter
  -> broker
  -> metrics in Stats
  -> CSV snapshots
  -> scripts/plots/results
```

That is the central shape of the repo. Once that shape is clear, most files fit
into place: roles describe what workload to run, transports describe how to talk
to a broker, metrics describe what happened, and scripts repeat those runs at
larger scale.
