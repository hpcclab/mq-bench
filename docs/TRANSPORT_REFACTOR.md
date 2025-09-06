# Transport Abstraction & Baselines Design

Date: 2025-09-06
Status: Draft
Owners: core

## Goals

- Decouple roles (publisher, subscriber, requester, queryable) from a specific messaging stack (Zenoh).
- Enable pluggable transports (Zenoh, TCP, NATS, Redis, MQTT, gRPC) behind a common trait.
- Preserve current CLIs and metrics; add `--engine` and `--connect` to select and configure transports.
- Keep non-Zenoh dependencies optional via Cargo features.
- Provide a mock transport for tests.

## Non-goals

- Feature-parity across all engines (advanced QoS, discovery). Baselines focus on comparable pub/sub and req/rep.
- Reinventing schema formats. We keep current CSV schema and payload headers.

## High-level architecture

- `transport::Transport` trait captures the minimum viable API used by roles.
- One adapter module per engine implements the trait (e.g., `transport::zenoh`, `transport::tcp`).
- A factory selects an adapter based on CLI `--engine` and `--connect` options.
- Roles depend only on `Transport`. They receive a `TransportHandle` at startup.

```
+---------------------+       +--------------------+
|        roles        |-----> |   transport API    |
| pub/sub/req/qry     |       |  (trait + factory) |
+---------------------+       +--------------------+
                                     |  |  |  |
                       +-------------+  |  |  +-------------+
                       v                v  v                v
                 zenoh adapter     tcp adapter        nats/redis/...
```

## Modules & files

- src/
  - transport/
    - mod.rs                  (Transport trait, types, factory)
    - config.rs               (Engine-agnostic connect options parsing)
    - zenoh.rs                (Zenoh adapter, feature `transport-zenoh` default)
    - tcp.rs                  (Raw TCP baseline, feature `transport-tcp`)
    - nats.rs                 (Feature `transport-nats`)
    - redis.rs                (Feature `transport-redis`)
    - mqtt.rs                 (Feature `transport-mqtt`)
    - grpc.rs                 (Feature `transport-grpc`)
    - mock.rs                 (Test-only adapter)
  - roles/
    - publisher.rs            (swapped to Transport; no Zenoh types)
    - subscriber.rs           (swapped to Transport)
    - requester.rs            (swapped to Transport)
    - queryable.rs            (swapped to Transport)
  - wire/
    - mod.rs                  (simple length-prefixed framing, header encode/decode)
  - metrics/                  (unchanged)
  - payload.rs                (unchanged public API)

## Public API (Transport)

Rust sketch:

- `TransportBuilder` creates a `Transport` from `Engine` + `ConnectOptions`.
- `Topic` is a `String`; engines may map to subject/expr.
- Streams use `tokio_stream::wrappers` and `futures::Stream`.

Contract:
- All methods are async-safe and cancel-safe.
- Backpressure: publish returns when message is accepted by local client; engine may buffer.
- Errors: unified `TransportError` with engine-specific source for diagnostics.

```rust
#[derive(Clone, Debug)]
pub enum Engine { Zenoh, Tcp, Nats, Redis, Mqtt, Grpc }

#[derive(Clone, Debug)]
pub struct ConnectOptions { pub params: std::collections::BTreeMap<String, String> }

#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    #[error("connect: {0}")] Connect(String),
    #[error("publish: {0}")] Publish(String),
    #[error("subscribe: {0}")] Subscribe(String),
    #[error("request: {0}")] Request(String),
  #[error("timeout")] Timeout,
  #[error("disconnected")] Disconnected,
    #[error("other: {0}")] Other(String),
}

impl TransportError {
  /// Returns true if the error is likely recoverable (temporary network issue, backoff/retry recommended)
  pub fn is_recoverable(&self) -> bool {
    matches!(self, Self::Timeout | Self::Disconnected)
  }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, topic: &str, payload: bytes::Bytes) -> Result<(), TransportError>;
    async fn subscribe(&self, expr: &str) -> Result<TransportStream, TransportError>;
    async fn request(&self, subject: &str, payload: bytes::Bytes) -> Result<bytes::Bytes, TransportError>;
  // Serve queries (request/reply server-side). Returns a stream of incoming queries.
  async fn register_queryable(&self, subject: &str) -> Result<QueryStream, TransportError>;
  /// Gracefully shutdown the transport, closing sessions/connections.
  async fn shutdown(&self) -> Result<(), TransportError>;
  /// Health probe to detect disconnected states for fast-fail or reconnection.
  async fn health_check(&self) -> Result<(), TransportError>;
}

pub type TransportStream = 
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<TransportMessage, TransportError>> + Send>>;

#[derive(Clone, Debug)]
pub struct TransportMessage { pub topic: String, pub payload: bytes::Bytes }

// Query serving API
pub type QueryStream =
  std::pin::Pin<Box<dyn futures::Stream<Item = Result<IncomingQuery, TransportError>> + Send>>;

#[derive(Debug)]
pub struct IncomingQuery {
  pub subject: String,
  pub payload: bytes::Bytes,
  // Optional correlation metadata (engine-defined); exposed as plain strings for logging/metrics.
  pub correlation: Option<String>,
  pub responder: QueryResponder,
}

// A lightweight responder object allowing one or many replies.
// Engines that only support unary replies can implement `send` once and make `end` a no-op.
pub struct QueryResponder {
  inner: std::sync::Arc<dyn QueryResponderInner + Send + Sync>,
}

#[async_trait::async_trait]
pub trait QueryResponderInner: Send + Sync {
  async fn send(&self, payload: bytes::Bytes) -> Result<(), TransportError>;
  async fn end(&self) -> Result<(), TransportError>;
}

impl QueryResponder {
  pub async fn send(&self, payload: bytes::Bytes) -> Result<(), TransportError> {
    self.inner.send(payload).await
  }
  pub async fn end(&self) -> Result<(), TransportError> { 
    self.inner.end().await 
  }
}

pub struct TransportBuilder;
impl TransportBuilder {
    pub async fn connect(engine: Engine, opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> { /* ... */ }
}
```

Notes:
- We keep a simple single-response `request` for clients. For serving-side, `register_queryable` supports one or many replies via `QueryResponder::send`; engines without streaming emulate unary.
- For subscription, topic expressions follow engine semantics (`/topic/**` vs `>`). The roles pass through the CLI-provided string.

## Implementation guidelines

### Connection management
- Maintain a single session/connection pool per `Transport` instance.
- Zenoh: one session reused for all operations.
- TCP baseline: reuse a client connection for publish; separate listener for subscribe; multiplex topics in user space.
- NATS: single connection with subject multiplexing.
- Redis: use a small connection pool; dedicated pub/sub connection recommended.
- MQTT: long-lived client connection; automatic reconnect enabled where available.

### Error recovery & retries
- Treat `Timeout` and `Disconnected` as recoverable; the harness can backoff and retry.
- Adapters should surface disconnections promptly via `health_check()` and fail fast on publish/subscribe.
- Avoid internal infinite retries; defer policy to roles/harness.

## Mock transport capabilities
- Configurable latency injection and message drop rate for tests.
- Record all operations for verification and metrics.
- Scripted responses for `request` and `register_queryable`.
- Example builder sketch:
  - `MockTransport::builder().latency(10ms).drop_rate(0.01).build()`.

## Performance considerations
- Prefer zero-copy with `bytes::Bytes`.
- Allow internal batching (configurable) where the engine supports it.
- Reuse buffers (pools) for high-throughput benchmarks.
- Keep metrics on a side path; avoid per-message heavy locking.

## TCP wire framing (baseline)
- Length-prefixed frames, little-endian:
  - `[len: u32][topic_len: u16][topic: N bytes][payload: M bytes]`
- Minimal header remains the project’s 24-byte `payload::MessageHeader` when measuring latency; placed at the start of `payload`.

## CLI changes

- New flags
  - `--engine <zenoh|tcp|nats|redis|mqtt|grpc>` (default: `zenoh`)
  - `--connect KEY=VALUE` (repeatable). Examples:
    - Zenoh: `--connect endpoint=tcp/127.0.0.1:7447` or `--connect mode=client`.
    - NATS: `--connect url=nats://127.0.0.1:4222`.
    - Redis: `--connect url=redis://127.0.0.1:6379`.
    - TCP: `--connect host=127.0.0.1 --connect port=7000`.
- Backwards compat: legacy `--endpoint` maps to `--connect endpoint=...` when engine=zenoh.

## Features & dependencies

- Default features: `transport-zenoh`.
- Optional: `transport-tcp`, `transport-nats`, `transport-redis`, `transport-mqtt`, `transport-grpc`.
- CI: build matrix toggles feature sets; `cargo build` compiles zenoh only.

## Migration plan

1) Introduce modules: `transport::{mod,config}`, `wire::mod`, and mock adapter.
2) Implement Zenoh adapter to the trait; behind feature `transport-zenoh` (default).
3) Switch roles to depend on `Transport` and receive `Box<dyn Transport>` from main.
4) Preserve CLI by mapping existing flags to new builder.
5) Add TCP baseline adapter; wire CLI `--engine tcp`.
6) Extend `docker-compose.yml` for other baselines (NATS/Redis/MQTT) later.

## Testing strategy

- Unit tests for `wire` framing and `mock` adapter.
- Integration tests: pub/sub and req/rep using mock and zenoh features.
- Smoke scripts: existing `scripts/run_local_baseline.sh` with `--engine zenoh`, plus new `scripts/run_baselines.sh`.

## Risks & mitigations

- API churn in roles: Keep method signatures stable by injecting `Transport` via constructor.
- Perf overhead of dyn dispatch: Allow `#[cfg(feature)]` + concrete generics later if needed.
- Error mapping complexity: Keep unified error thin, store source error as string or boxed error.

## Acceptance criteria

- Building with default features runs all current scenarios unchanged.
- Passing `--engine tcp` runs the same roles against the TCP baseline.
- Adding a new adapter requires only implementing `Transport` and registering it in the builder.
