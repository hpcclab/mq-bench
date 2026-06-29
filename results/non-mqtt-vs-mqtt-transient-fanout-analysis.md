# Why Non-MQTT Brokers Look Better in the `mqtt` vs `non-mqtt` Transient Fan-Out Data

## Short Answer

The data suggests that the apparent advantage of the non-MQTT group comes less from "MQTT vs non-MQTT" as a protocol question and more from broker architecture:

- The strongest non-MQTT brokers here, `nats` and `zenoh`, sustain the offered fan-out rate with low latency, modest CPU, and very small memory footprints.
- Several MQTT brokers hit a saturation point much earlier, then enter backlog-drain behavior: throughput falls behind target, latency jumps from milliseconds to seconds, and memory rises sharply.
- The two MQTT brokers that use heavier managed runtimes in this dataset, `mqtt_artemis` and `mqtt_hivemq`, show the clearest memory-growth and recovery problems.
- The single-threaded MQTT broker, `mqtt_mosquitto`, stays memory-light but runs into a hard dispatch ceiling much earlier.

So the practical answer is: the non-MQTT group performs better here because its top brokers have a lighter runtime and a more efficient fan-out dispatch path under bursty load.

## Important Comparison Caveat

The two folders do **not** use the exact same burst profile:

- `mq-bench/results/mqtt/phase-rate-summary.md` stops at a `500k msg/s` fan-out delivery target.
- `mq-bench/results/non-mqtt/phase-rate-summary.md` continues to `10M msg/s`.

Because of that, the fairest direct comparison is the overlapping burst targets:

- `50k`
- `100k`
- `200k`
- `300k`
- `500k`

The extended non-MQTT phases above `500k` are still useful as supporting evidence, but not as strict apples-to-apples comparisons.

## What the Overlapping Phases Show

### Group-level view

Average delivery-rate attainment across the overlapping burst phases:

| Target delivery rate | MQTT avg hit rate | non-MQTT avg hit rate |
|---|---:|---:|
| `50k msg/s` | `100.52%` | `100.48%` |
| `100k msg/s` | `94.48%` | `100.00%` |
| `200k msg/s` | `85.51%` | `99.75%` |
| `300k msg/s` | `64.89%` | `99.87%` |
| `500k msg/s` | `22.38%` | `99.71%` |

This is the clearest pattern in the data:

- At `50k`, both groups are fine.
- At `100k`, the MQTT group has already started to separate.
- At `200k` and above, the non-MQTT group stays near target, while the MQTT group falls off quickly.

### Resource cost at the same burst levels

Average resource usage over those same overlapping burst phases:

| Target delivery rate | MQTT avg CPU | non-MQTT avg CPU | MQTT avg memory | non-MQTT avg memory |
|---|---:|---:|---:|---:|
| `50k msg/s` | `194.1%` | `178.7%` | `470.9 MiB` | `65.3 MiB` |
| `100k msg/s` | `214.0%` | `186.7%` | `479.9 MiB` | `65.8 MiB` |
| `200k msg/s` | `240.2%` | `188.7%` | `493.7 MiB` | `67.9 MiB` |
| `300k msg/s` | `246.0%` | `191.0%` | `689.7 MiB` | `69.9 MiB` |
| `500k msg/s` | `280.2%` | `200.9%` | `1528.6 MiB` | `50.4 MiB` |

The MQTT group is generally spending more CPU and much more memory to deliver less useful work.

## Concrete Broker Examples

### At `500k msg/s`

Non-MQTT brokers:

| Broker | Delivered | Hit rate | Avg CPU | Avg memory | P50 latency |
|---|---:|---:|---:|---:|---:|
| `nats` | `496,519` | `99.30%` | `258.8%` | `76.9 MiB` | `1.08 ms` |
| `zenoh` | `500,898` | `100.18%` | `263.0%` | `65.5 MiB` | `1.31 ms` |
| `redis` | `498,244` | `99.65%` | `80.7%` | `8.7 MiB` | `22.05 ms` |

MQTT brokers:

| Broker | Delivered | Hit rate | Avg CPU | Avg memory | P50 latency |
|---|---:|---:|---:|---:|---:|
| `mqtt_mosquitto` | `94,323` | `18.86%` | `90.4%` | `13.9 MiB` | `20,968.98 ms` |
| `mqtt_rabbitmq` | `160,620` | `32.12%` | `378.0%` | `466.9 MiB` | `20,698.91 ms` |
| `mqtt_artemis` | `114,935` to `123,072` | `22.99%` to `24.61%` | `297.7%` to `310.4%` | `2404.5` to `2553.0 MiB` | `9,967.33` to `12,041.86 ms` |
| `mqtt_hivemq` | `66,560` | `13.31%` | `324.5%` | `2204.8 MiB` | `3,566.18 ms` |

This is not a subtle difference. The best non-MQTT brokers are still near line rate at `500k`, while the MQTT group is either saturated or in visible backlog collapse.

## Recovery Behavior Explains the "Why"

Steady-state throughput is only part of the story. The recovery phases show whether a broker absorbed the burst cleanly or just accumulated delayed work.

### After the `500k -> 50k` transition

Non-MQTT recovery:

- `nats`: recovery P50 `1.26 ms`
- `zenoh`: recovery P50 `1.75 ms`
- `redis`: recovery P50 `3.61 ms`

MQTT recovery:

- `mqtt_mosquitto`: recovery P50 `7,156.99 ms`
- `mqtt_rabbitmq`: recovery P50 `35,384.09 ms`
- `mqtt_artemis`: recovery P50 `52,273.88 ms` and `54,221.79 ms` in the two runs
- `mqtt_hivemq`: delivered `0` in the final recovery phase

Interpretation:

- `nats` and `zenoh` mostly stay in a low-latency operating regime.
- The MQTT brokers often do not return to a clean baseline after overload. They are still draining queued work, and the latency numbers expose that immediately.

This matches the existing Artemis-specific note in [artemis-burst4-300-instability.md](/home/cc/projects/mq-bench/results/mqtt/analysis/artemis-burst4-300-instability.md), which already identifies broker-side saturation, backlog growth, and uneven drain waves once Artemis crosses its sustainable fan-out limit.

## What Is Actually Driving the Difference

The data supports four main causes.

### 1. Lightweight implementations stay efficient longer

`nats` and `zenoh` keep both latency and memory low while throughput rises. That usually means the broker is not creating large internal queues and is not paying a large per-message management cost.

In this dataset:

- `nats` stays around `74-77 MiB` through the overlapping burst range.
- `zenoh` stays around `57-66 MiB`.
- Several MQTT brokers are already in the hundreds of MiB or multiple GiB well before the end of their profile.

### 2. Fan-out dispatch efficiency matters more than nominal protocol family

This workload is a bursty fan-out test with `10` publishers and `1000` subscribers. The winning brokers are the ones that can copy and dispatch subscriber deliveries efficiently under pressure.

The best evidence is that:

- `nats` and `zenoh` keep meeting target at `300k` and `500k`.
- `mqtt_artemis`, `mqtt_rabbitmq`, and `mqtt_mosquitto` do not.

That points to dispatch-path efficiency, not just raw publish ingestion rate.

### 3. Managed-runtime overhead shows up as memory growth and unstable recovery

The biggest MQTT failures are also the brokers with the heaviest memory growth:

- `mqtt_artemis`: roughly `2.4-2.5 GiB` during the `500k` burst
- `mqtt_hivemq`: roughly `2.2 GiB` during the `500k` burst, then complete collapse in final recovery

That is consistent with a broker that is buffering heavily once it falls behind. Once backlog accumulates, recovery latency explodes.

### 4. Single-threaded designs can stay small but still lose on throughput

`mqtt_mosquitto` is the best counterexample to a pure memory-based explanation:

- It uses very little memory.
- But it still collapses after `50k` and is far behind by `100k+`.

So low memory alone is not enough. A broker also needs a dispatch path that can scale with the offered fan-out rate.

## Nuance: This Is Not "All Non-MQTT Brokers Are Better"

The data does **not** support a blanket statement that every non-MQTT broker is better than every MQTT broker.

- `redis` reaches `500k` with tiny resource usage, but its latency is higher than `nats` and `zenoh`.
- The non-MQTT RabbitMQ-AMQP data in `summary_by_phase.csv` is incomplete, so it cannot be used for a full phase-by-phase comparison here.
- Among MQTT brokers, `mqtt_hivemq` and `mqtt_artemis` handle midrange load better than `mqtt_mosquitto`, but they pay much more in memory and become unstable later.

The strongest version of the conclusion is:

> In this transient fan-out workload, the best non-MQTT brokers outperform the MQTT brokers because they sustain the fan-out rate with much lower memory growth and much cleaner post-burst recovery.

## Concrete MQTT-Favorable Experiment Design

The earlier section gives the right direction. This section turns it into a concrete benchmark that you could actually run and report.

### Experiment name

**Intermittent Edge Telemetry With Durable Sessions**

### Main question

> Which broker behaves best when a large fleet of edge devices publishes small telemetry continuously, disconnects often, reconnects with the same identity, and expects reliable delivery plus state continuity?

That is a much more MQTT-native question than pure transient fan-out throughput.

### Testbed

Use two hardware tiers so the results show both constrained-edge and better-provisioned edge-hub behavior.

| Tier | VM size | Purpose |
|---|---|---|
| `edge-small` | `2 vCPU`, `4 GB RAM` | constrained gateway / edge hub |
| `edge-medium` | `4 vCPU`, `8 GB RAM` | moderately provisioned edge hub |

Run the same experiment on both tiers.

### Brokers to test

Primary MQTT set:

- `mqtt_mosquitto`
- `mqtt_emqx`
- `mqtt_hivemq`
- `mqtt_rabbitmq`
- `mqtt_artemis`

Optional comparison set, but only if durable features are enabled:

- `nats` with `JetStream`
- `redis` with `Streams`
- `zenoh` with comparable storage/session support

Without those durability features, the non-MQTT systems are answering a different question.

### Topic model

Use a realistic per-device topic tree:

- telemetry: `edge/<site>/<device>/telemetry`
- command: `edge/<site>/<device>/cmd`
- reported state: `edge/<site>/<device>/state`
- last-will status: `edge/<site>/<device>/status`

Retain the latest command or desired-state message on the `cmd` topic so reconnecting devices immediately receive the current instruction.

### Client population

Use `10000` devices spread across `20` sites, plus `20` backend consumer clients.

Backend consumers subscribe by wildcard rather than per-device individual subscriptions, for example:

- `edge/+/+/telemetry`
- `edge/+/+/state`
- `edge/+/+/status`

This keeps the broker load centered on connection/session handling and reliable delivery, not giant broker-side fan-out amplification.

### Device classes

Split the `10000` devices into three realistic groups.

| Class | Share | Behavior |
|---|---:|---|
| steady sensors | `70%` | publish `1 msg/s`, `256 B`, always connected |
| sleepy devices | `20%` | connect for `15 s`, publish `1 msg/s`, then disconnect for `45 s`; reconnect with same client ID |
| alerting devices | `10%` | publish `0.2 msg/s` normally, but burst to `5 msg/s` for `30 s` every `5 min` |

This creates a workload with:

- many long-lived connections,
- repeated reconnects,
- moderate telemetry volume,
- and occasional localized bursts.

That is much closer to real IoT edge than the current transient fan-out test.

### MQTT settings

Run three variants of the same experiment.

| Variant | QoS | Session mode | Purpose |
|---|---:|---|---|
| `realtime` | `0` | persistent session still enabled | best-effort baseline |
| `reliable` | `1` | persistent session enabled | primary MQTT comparison |
| `exactly-once` | `2` | persistent session enabled | strongest reliability, highest overhead |

For all three variants:

- use stable client IDs,
- set `clean_session=false` or MQTT 5 session expiry,
- configure a Last Will message on `status`,
- and keep one retained command/state message per device.

The `QoS 1` run should be the headline result, because that is where MQTT often delivers the best balance of reliability and cost.

### Time schedule

Make each run `30 min` long and divide it into explicit phases.

| Phase | Duration | Behavior |
|---|---:|---|
| warm-up | `5 min` | all devices behave normally; establish steady state |
| churn | `10 min` | every `30 s`, randomly disconnect `10%` of currently connected devices for `15 s` |
| site outage | `5 min` | disconnect one full site (`500` devices) for `60 s`; devices reconnect together |
| recovery | `10 min` | normal traffic resumes; retained commands and buffered telemetry continue draining |

This schedule produces three MQTT-relevant stresses:

- ordinary churn,
- a reconnect storm,
- and a delayed-message catch-up period.

### A second, even more MQTT-favorable variant

If you want a stronger showcase for MQTT, add a **subscriber intermittency** variant.

Instead of disconnecting devices, keep publishers online and disconnect a subset of backend consumers while devices continue sending. Then reconnect those consumers with the same session identity.

That version directly tests:

- offline buffering,
- redelivery,
- duplicate control,
- and catch-up latency.

This is especially useful when the paper wants to highlight durable delivery semantics rather than client-connection scalability.

### Metrics to collect

The most important metrics are not peak throughput. They are continuity and recovery metrics.

#### Reliability metrics

- message loss rate at backend consumers
- duplicate delivery rate after reconnect
- percent of offline-period messages successfully delivered after reconnect
- Last Will delivery success rate
- retained-command delivery success rate on reconnect

#### Latency and recovery metrics

- P50/P95/P99 end-to-end latency during steady state
- P50/P95/P99 catch-up latency after reconnect
- backlog drain time after each outage
- time from reconnect to receipt of retained command/state

#### Resource metrics

- broker CPU over time
- broker memory over time
- memory growth as offline sessions accumulate
- reconnect throughput during reconnect storms

#### Derived MQTT-specific metric

A very useful summary number would be:

> **recovery efficiency** = fraction of messages generated during disconnection that are successfully delivered within `T` seconds of reconnect

For example, report recovery efficiency for `T = 5 s`, `15 s`, and `60 s`.

### Success criteria

A broker should be considered strong in this experiment if it satisfies most of the following in the `QoS 1` run:

- near-zero message loss,
- low duplicate rate,
- clean Last Will behavior,
- retained messages delivered immediately on reconnect,
- bounded memory growth during offline periods,
- and short backlog-drain time after reconnect storms.

That definition matches what edge users usually care about much better than maximum burst fan-out rate.

### Why MQTT brokers should flourish here

This design rewards exactly the things MQTT brokers are built to do well:

- maintain many device sessions,
- survive intermittent links,
- preserve identity across reconnects,
- buffer and redeliver messages when needed,
- and maintain the latest control/state information through retained messages.

In this benchmark, broker-side session machinery is no longer just overhead. It is the useful work.

### Which MQTT brokers are likely to look best

Based on your current results, I would expect something like this:

- `mqtt_emqx` should look strong when the workload has many concurrent clients plus QoS 1 reliability, because its concurrency model handles acknowledgment work efficiently.
- `mqtt_rabbitmq` may also look good in the reliability-centric runs, especially if the comparison values durable delivery more than raw footprint.
- `mqtt_hivemq` could look strong on the `edge-medium` tier where memory is less constrained.
- `mqtt_mosquitto` may look best in the smallest-footprint deployments or the lower-scale versions of this experiment, but may pay more at QoS 1/2 because of its single-threaded design.
- `mqtt_artemis` may perform reasonably at moderate scale, but I would watch closely for memory growth during offline buffering and reconnect storms.

That is an expectation, not a guaranteed ranking. But it is the kind of workload where MQTT brokers should look much healthier than they do in transient fan-out overload.

## Where MQTT Performs Best In Practice

MQTT does not perform best in every messaging situation. It performs best when the workload rewards **connection continuity, small-message efficiency, and delivery semantics across unstable links**.

### Best-fit workload classes

| Workload | Why MQTT fits well | Why the current transient fan-out test does not capture it |
|---|---|---|
| device telemetry uplink | many devices send small messages to a small analytics/backend set | current test stresses massive broker-side replication instead |
| sleepy battery-powered devices | persistent sessions and reconnect-friendly semantics matter | current test assumes continuously active subscribers |
| command-and-control | retained commands and immediate state sync after reconnect are valuable | retained-state behavior is invisible in pure fan-out throughput |
| unreliable networks | QoS 1/2, redelivery, and session continuity matter more than absolute msg/s | current test rewards lightweight fire-and-forget paths |
| device presence / liveness | Last Will and status topics are first-class features | the test never asks the broker to manage presence semantics |

### The simplest practical answer

If someone asks, "Where is MQTT actually strongest?", the shortest honest answer is:

> MQTT is strongest when you have many edge devices sending small messages over unstable links, and you care about reconnect behavior, reliable delivery, and retained state more than maximum fan-out throughput.

### Where MQTT is not strongest

MQTT is usually not the best fit when the main requirement is:

- ultra-high-rate broker-side fan-out,
- minimal per-message broker work,
- or the absolute highest transient throughput on a clean stable network.

That is where leaner systems like `nats` or `zenoh` can look better, especially in the exact kind of stress test you already ran.

## Bottom Line

If you ask "why are the non-MQTT brokers performing better in this constrained setting?", the data-backed answer is:

1. They hit the same burst targets with less CPU and far less memory.
2. They avoid the backlog-amplification behavior that dominates the MQTT results after `100k-200k msg/s`.
3. Their recovery phases stay in the millisecond range, which means they are not spending the next phase draining old work.
4. The difference is mostly architectural, not merely protocol-label based: `nats` and `zenoh` have a more efficient burst fan-out path than the MQTT brokers in this dataset.

## Files Used

- [mqtt/phase-rate-summary.md](/home/cc/projects/mq-bench/results/mqtt/phase-rate-summary.md)
- [mqtt/raw_data/summary_by_phase.csv](/home/cc/projects/mq-bench/results/mqtt/raw_data/summary_by_phase.csv)
- [non-mqtt/phase-rate-summary.md](/home/cc/projects/mq-bench/results/non-mqtt/phase-rate-summary.md)
- [non-mqtt/raw_data/summary_by_phase.csv](/home/cc/projects/mq-bench/results/non-mqtt/raw_data/summary_by_phase.csv)
- [artemis-burst4-300-instability.md](/home/cc/projects/mq-bench/results/mqtt/analysis/artemis-burst4-300-instability.md)


## Deeper Look: Why `nats` and `zenoh` Beat the MQTT Brokers

The earlier sections showed that `nats` and `zenoh` win. This section explains more precisely **why** they win, and where each one eventually hits its own limit.

### 1. They begin with much more headroom

At the same `20k msg/s` baseline fan-out rate, the stronger non-MQTT brokers start from a much lighter operating point:

| Broker | Baseline P50 | Baseline CPU | Baseline memory |
|---|---:|---:|---:|
| `nats` | `1.17 ms` | `1.11 cores` | `73.0 MiB` |
| `zenoh` | `1.41 ms` | `1.18 cores` | `55.3 MiB` |
| `mqtt_rabbitmq` | `2.29 ms` | `1.91 cores` | `187.4 MiB` |
| `mqtt_artemis` | `1.88-2.00 ms` | `1.82-1.86 cores` | `590.8-605.3 MiB` |
| `mqtt_hivemq` | `2.51 ms` | `2.36 cores` | `929.0 MiB` |
| `mqtt_mosquitto` | `2.15 ms` | `0.72 cores` | `9.7 MiB` |

That matters because the burst test rewards brokers that still have spare CPU cycles and spare queue capacity when the offered rate jumps. `nats` and `zenoh` enter the burst phases with much more room left than `mqtt_rabbitmq`, `mqtt_artemis`, and especially `mqtt_hivemq`.

### 2. Their dispatch path stays cheap while fan-out increases

The strongest difference is not just lower baseline cost, but lower **incremental** cost as fan-out rises.

For `nats` and `zenoh`:

- P50 latency stays around `1-2 ms` through `500k msg/s`.
- Memory stays small and grows slowly.
- Throughput tracks target almost perfectly through the overlapping range.

For many MQTT brokers:

- They are already consuming much more CPU or memory at baseline.
- Once they cross their dispatch limit, latency jumps by three to four orders of magnitude.
- Extra memory turns into backlog, not into sustained throughput.

This is why the data looks like a "clean scaling" curve for `nats` and `zenoh`, but a "fall behind and drain later" curve for most MQTT brokers.

### 3. The MQTT brokers are bottlenecked in different ways, but they all show backlog much earlier

The MQTT brokers do not all fail for the same reason:

- `mqtt_mosquitto` is the clearest single-thread ceiling. It stays tiny in memory, but throughput drops sharply after `50k`, and by `500k` it only delivers `18.86%` of target.
- `mqtt_rabbitmq` uses much more CPU and memory than Mosquitto, but still falls behind by `200k`; after that, recovery latency explodes into tens of seconds.
- `mqtt_artemis` and `mqtt_hivemq` handle the early and midrange bursts better than Mosquitto, but once they cross their sustainable fan-out rate they pay heavily in memory growth and prolonged recovery. In practice, they turn excess offered load into broker-side buffering.

So the non-MQTT advantage is not that MQTT has one universal flaw. It is that the MQTT brokers in this dataset reach their bottlenecks sooner, and their failure mode is much more expensive.

## What Bottleneck Does `nats` Hit?

`nats` is the cleanest example of a broker that remains in a healthy operating regime almost all the way to the top of the non-MQTT profile.

### Observed scaling pattern

From the phase summaries:

| Target | Delivered | Hit rate | Avg CPU | Avg TX | P50 |
|---|---:|---:|---:|---:|---:|
| `1M` | `992,976` | `99.30%` | `2.70 cores` | `0.41 Gbps` | `1.09 ms` |
| `2M` | `1,983,255` | `99.16%` | `2.82 cores` | `0.79 Gbps` | `1.41 ms` |
| `4M` | `3,966,371` | `99.16%` | `3.28 cores` | `1.51 Gbps` | `2.08 ms` |
| `6M` | `5,947,299` | `99.12%` | `3.23 cores` | `2.28 Gbps` | `3.01 ms` |
| `8M` | `7,936,154` | `99.20%` | `3.33 cores` | `2.86 Gbps` | `4.67 ms` |
| `10M` | `9,917,920` | `99.18%` | `3.38 cores` | `3.76 Gbps` | `12.71 ms` |

Measured behavior:

- Throughput stays almost perfectly proportional to target.
- Network transmit rate rises with it, instead of flattening early.
- CPU keeps climbing toward the 4-core ceiling.
- Memory rises slowly, from about `74 MiB` to about `123 MiB`, rather than exploding.

### Likely bottleneck

The most likely bottleneck for `nats` is **aggregate CPU and packet I/O at the top end**, not queue blow-up.

That inference comes from the combination of:

- near-perfect target tracking,
- steadily rising transmit bandwidth,
- CPU approaching full-machine utilization,
- and only a gradual latency increase until the final `10M` phase.

In other words, `nats` looks like a broker that is still "doing useful work" when it gets close to its limit. It does not hit a cliff at `200k` or `500k`; it keeps scaling until the host is simply close to saturated.

## What Bottleneck Does `zenoh` Hit?

`zenoh` performs very well at moderate and midrange load, but its high-end limit is different from `nats`.

### Observed scaling pattern

| Target | Delivered | Hit rate | Avg CPU | Avg TX | P50 |
|---|---:|---:|---:|---:|---:|
| `1M` | `1,001,488` | `100.15%` | `2.87 cores` | `0.46 Gbps` | `2.00 ms` |
| `2M` | `2,001,667` | `100.08%` | `3.10 cores` | `0.89 Gbps` | `5.12 ms` |
| `4M` | `2,867,748` | `71.69%` | `3.21 cores` | `1.20 Gbps` | `8,433.96 ms` |
| `6M` | `3,382,364` | `56.37%` | `3.30 cores` | `1.41 Gbps` | `10,290.49 ms` |
| `8M` | `3,375,966` | `42.20%` | `3.28 cores` | `1.44 Gbps` | `11,868.53 ms` |
| `10M` | `3,378,987` | `33.79%` | `3.30 cores` | `1.52 Gbps` | `12,561.11 ms` |

Measured behavior:

- `zenoh` is excellent through `2M`.
- Between `2M` and `4M`, throughput stops scaling linearly.
- From `6M` onward, throughput mostly plateaus around `3.3M`.
- CPU does not collapse, but it also does not keep rising much beyond about `3.2-3.3` cores on average.
- Network transmit rate also flattens, from about `1.20 Gbps` at `4M` to only about `1.52 Gbps` at `10M`.
- Recovery phases after the larger bursts show large latency and backlog carry-over.

### Likely bottleneck

The data suggests that `zenoh` is **not** primarily limited by raw memory capacity. Memory only rises from about `65 MiB` at `500k` to about `156 MiB` at `10M`, which is still very small.

Instead, the more likely bottleneck is an **internal fan-out dispatch or egress pipeline ceiling**:

- throughput flattens well before the offered load does,
- network transmit also flattens,
- CPU stops scaling much further even though it is not obviously "crashed",
- and latency jumps into the multi-second range, which is the signature of backlog accumulation.

That pattern usually means there is some hot path that stops parallelizing well enough at high fan-out, such as a serialized dispatch stage, an egress scheduling bottleneck, or some other per-delivery path that prevents the broker from turning the remaining CPU into more delivered messages.

This last sentence is an inference from the measured scaling pattern, not something the dataset can prove directly.

## So Why Are `nats` and `zenoh` Better Than the MQTT Brokers Here?

The deeper answer is:

1. They start lighter, so they have more burst headroom.
2. They keep their fast path cheap for much longer as fan-out rises.
3. When they do hit limits, those limits are later and cleaner than the MQTT brokers' limits.
4. `nats` in particular keeps converting extra CPU and TX bandwidth into real throughput almost all the way up the curve.
5. `zenoh` is also much better than the MQTT brokers in the overlapping range, but its own ceiling appears earlier than `nats`, around the `3M-3.5M delivery/s` region in this transient test.

So if the question is "why do `nats` and `zenoh` beat MQTT here?", the answer is:

> They spend less of the machine on runtime/session overhead, keep latency low while dispatch load grows, and postpone backlog formation much longer than the MQTT brokers do.

If the follow-up question is "what stops them eventually?", then:

- `nats` looks mainly CPU/egress-limited near the top of the tested range.
- `zenoh` looks limited by a dispatch/egress scaling ceiling that appears once bursts move beyond about `2M` offered deliveries per second.


## Architectural Interpretation For The Paper

This is the simple version.

What I think is happening is not just "non-MQTT is better than MQTT." It is more about how much work each broker does for every message.

### NATS in simple words

NATS looks fast because its normal path is very simple.

- A publisher sends a message.
- NATS checks which subscribers care about that subject.
- It forwards the message.
- It moves on.

It does not look like it is doing a lot of extra broker-side work in this test. The data matches that idea:

- throughput keeps rising as load rises,
- CPU keeps rising too,
- network TX keeps rising,
- memory stays small.

That usually means the broker is spending most of its effort on real delivery work, not on tracking lots of extra state.

So the simple explanation for NATS is: it has a very short, clean message path, so it keeps scaling until the machine itself becomes the bottleneck. In this run, its likely bottleneck is just CPU plus network sending capacity.

### Zenoh in simple words

Zenoh also looks efficient, but in a slightly different way.

It stays light on memory and handles the low and medium burst phases very well. That suggests its routing path is also pretty lean.

But unlike NATS, Zenoh stops scaling cleanly once the offered load gets very high. After around `2M msg/s`, it does not keep climbing in a straight line anymore. Instead, it flattens around `3.3M delivered msg/s`, and then latency gets much worse during the later bursts and recovery phases.

So Zenoh does not look memory-bound. It looks more like some part of its internal forwarding path stops scaling well enough at very high fan-out. In simple words: Zenoh is efficient, but at some point one part of the router seems to become the choke point.

That last part is my inference from the shape of the data, not something the benchmark proves directly.

### Why the MQTT brokers look worse here

The MQTT brokers seem to do more work inside the broker for each client and each subscription.

A simple way to think about it is:

- NATS and Zenoh look more like "route and send."
- The MQTT brokers here look more like "track session state, manage internal broker objects, then route and send."

That extra work is not useless. It gives you richer broker behavior. But in this particular transient fan-out test, it seems to hurt them.

What the data suggests is:

- they use up more headroom earlier,
- they fall behind sooner,
- once they fall behind, they build backlog,
- and then recovery becomes slow because they are still draining old work.

### How I think about each MQTT broker

`mqtt_rabbitmq`:

- It looks like the MQTT path is tied to heavier internal queue machinery.
- That probably makes it more flexible, but also more expensive under fan-out.
- In the data, it falls behind earlier and then recovery latency becomes huge.

`mqtt_artemis`:

- It looks like it can handle moderate load, but once it crosses its limit it starts buffering a lot.
- That shows up as memory growth plus very bad recovery phases.
- So its failure mode is not "instant crash," but "I accepted too much work and now I am draining it slowly."

`mqtt_hivemq`:

- It also handles moderate load for a while.
- But it seems to pay a high runtime cost, and once overloaded it suffers badly in memory and recovery.
- My simple mental model is that it is carrying a lot more broker machinery in the hot path than NATS or Zenoh.

`mqtt_mosquitto`:

- This one is different.
- It stays tiny in memory, so the problem is probably not heavy broker state.
- But it still saturates early.
- So its issue looks more like a simpler throughput ceiling: small and lightweight, but not able to push fan-out very far.

### The simplest overall explanation

If I had to explain the whole result in one paragraph, I would say this:

> NATS and Zenoh do better mostly because they seem to spend less broker work per delivered message. They keep the fast path lean, so more of the machine goes into actual forwarding. The MQTT brokers in this test appear to spend more effort on broker-side state, session handling, queues, or runtime overhead, so they run out of headroom earlier. Once they fall behind, they do not fail cleanly; they build backlog, and that backlog turns into high memory use and very slow recovery.

### What seems to bottleneck each winner

`NATS`:

- probably limited mainly by CPU and network output near the top end,
- because throughput, CPU, and TX bandwidth all keep rising together.

`Zenoh`:

- probably limited by some internal router/dispatch scaling ceiling,
- because throughput flattens even though memory stays small,
- and latency blows up once it hits that ceiling.

### What is directly from the data vs what is my interpretation

Directly visible in the data:

- NATS scales very well across the whole non-MQTT run.
- Zenoh scales very well at first, then plateaus.
- MQTT brokers fall behind earlier and recover more slowly.

My interpretation:

- NATS has the simplest effective hot path in this test.
- Zenoh is also lean, but some internal forwarding stage seems to stop scaling at very high load.
- The MQTT brokers here are paying for richer internal broker behavior, and that cost shows up clearly under transient fan-out overload.



## Where MQTT Actually Flourishes

The transient fan-out data should not be read as "MQTT is bad for IoT edge." It is better read as:

> This particular workload stresses the exact part of the system where lean non-MQTT brokers are strongest: short-lived, high-rate, broker-side fan-out delivery under overload.

MQTT usually looks best in a different operating regime.

### 1. Many devices, each sending at a low or moderate rate

A lot of IoT edge systems have:

- thousands of devices,
- small payloads,
- message rates measured in fractions of a message per second to a few messages per second per device,
- and only a small number of backend consumers.

That is very different from asking a broker to replicate every message to `1000` subscribers during aggressive bursts.

In those real deployments, the main challenge is often not raw fan-out throughput. It is:

- connection management,
- efficient handling of many long-lived clients,
- and delivering small telemetry records reliably over time.

MQTT was designed for exactly that environment.

### 2. Intermittent connectivity and reconnect-heavy environments

MQTT is especially strong when clients are not always online:

- Wi-Fi links flap,
- cellular links disappear and return,
- devices sleep to save power,
- or gateways reconnect in waves after a local outage.

In those cases, MQTT features such as persistent sessions, QoS 1 and 2, retained messages, and Last Will become much more important than peak fan-out rate.

This is one reason MQTT is so common in IoT edge: the protocol is not just moving bytes quickly, it is helping applications survive unstable links.

### 3. Reliable delivery matters more than maximum throughput

If the system requirement is:

- "do not lose sensor alarms,"
- "deliver commands after the device reconnects,"
- or "buffer data briefly while the subscriber is offline,"

then MQTT can be a better fit than a lighter fire-and-forget pub/sub fabric.

Your own QoS/failure results already point in this direction:

- once QoS 1 or 2 is enabled,
- the important question becomes how well the broker handles acknowledgment state, buffering, and redelivery,
- not how efficiently it handles a giant bursty fan-out hot path.

That is a much more MQTT-favorable benchmark axis.

### 4. Device state and command-and-control patterns

MQTT also fits well when the broker is used for more than plain telemetry forwarding:

- retained configuration topics,
- online/offline presence with Last Will,
- command topics per device,
- and "send the latest desired state when the client reconnects" behavior.

Those are normal edge-control patterns, but they are largely invisible in a pure transient fan-out stress test.

So the real takeaway is:

> MQTT flourishes when the workload is device-centric, connection-sensitive, and reliability-oriented, rather than a pure high-rate fan-out dispatch contest.

## A Benchmark Where MQTT Should Show Good Results

If you want a benchmark that gives MQTT a fair chance to look strong, I would design one around **intermittent edge telemetry with durable sessions**.

This is much closer to a typical IoT deployment than the current transient fan-out test.

### Benchmark goal

The goal is to answer:

> Which broker handles large numbers of edge clients most cleanly when devices publish small telemetry continuously, disconnect unpredictably, reconnect later, and expect reliable delivery semantics?

That is a better "MQTT showcase" question than "which broker survives the biggest bursty fan-out shock?"

### Proposed workload

Topology:

- `5000` to `20000` device clients
- `1` broker
- `10` to `50` backend consumer clients

Traffic shape:

- each device publishes to its own topic, such as `site/<site>/device/<id>/telemetry`
- payload size `128 B` to `1 KB`
- device publish rate `0.2` to `2 msg/s`
- optional periodic alert bursts from a small fraction of devices

MQTT features to enable:

- persistent sessions (`clean_session=false` or MQTT 5 session expiry)
- QoS `0`, `1`, and `2` as separate runs
- retained configuration topic per device or per device group
- Last Will topic for online/offline status

Failure model:

- every `60 s`, force `10%` to `20%` of clients offline
- keep them disconnected for `5` to `30 s`
- then reconnect them with the same client IDs
- include at least one reconnect storm where many clients return at once

This workload should stress:

- session tracking,
- acknowledgment handling,
- offline buffering,
- redelivery,
- and reconnect behavior.

Those are exactly the areas where MQTT brokers are supposed to provide value.

### Why MQTT should look better here

This experiment removes most of the bias that favors `nats` and `zenoh` in the transient fan-out test.

In particular:

- there is no giant "`1` message must immediately become `1000` deliveries" hot path,
- the per-device message rate is realistic for IoT,
- broker-side session management is now useful instead of just overhead,
- and recovery after disconnect becomes a first-class result rather than a side effect.

In this setting, the richer MQTT broker behavior is no longer just "extra work." It becomes the main thing being evaluated.

### What to measure

The most important metrics would be:

- message loss rate,
- duplicate delivery rate,
- reconnect success rate,
- backlog drain time after reconnect,
- end-to-end latency before, during, and after failures,
- broker CPU and memory over time,
- and memory overhead per offline or reconnecting client.

For MQTT specifically, I would also track:

- how long buffered messages remain pending,
- whether retained messages are delivered promptly after reconnect,
- and how QoS level changes throughput and recovery cost.

### Expected outcome

I would expect the ranking to look much better for MQTT brokers here than in transient fan-out.

In particular:

- `EMQX`, `RabbitMQ-MQTT`, and possibly `HiveMQ` should look more competitive,
- because their session and acknowledgment machinery is now helping with the workload instead of getting in the way,
- while `Mosquitto` may still look attractive on footprint for smaller deployments, but may show a stronger QoS penalty because of its single-threaded design.

I would still expect `nats` and `zenoh` to do well if compared in a fire-and-forget mode, but the comparison becomes much more subtle: they may be faster, while MQTT may be more complete for disconnected-device semantics.

### Fairness note

If you compare MQTT brokers against non-MQTT brokers in this experiment, you should be careful about feature equivalence.

For a fair comparison:

- `nats` should be tested with `JetStream` if durable buffering is part of the claim,
- `redis` should use a durable structure such as Streams rather than plain Pub/Sub,
- and `zenoh` should use whatever storage/session support is needed to provide comparable disconnected-delivery behavior.

Otherwise, the cleanest framing is not "MQTT versus non-MQTT throughput," but:

> "MQTT versus lighter pub/sub systems under edge-style disconnected reliability requirements."

That framing is more honest and better matched to the strengths of each system.

## Bottom Line

The current transient fan-out benchmark is useful, but it mainly answers:

> Which brokers have the leanest and most overload-resistant high-rate fan-out dispatch path?

That is not the same as asking where MQTT is strongest.

If the question is "where does MQTT flourish?", the short answer is still:

- large fleets of intermittently connected devices,
- small telemetry messages,
- persistent sessions,
- QoS-based reliability,
- retained state,
- and reconnect-heavy edge environments.

But each of those points means something specific.

### 1. Large fleets of intermittently connected devices

This is probably the most important one.

A lot of IoT systems do not have a small number of very busy clients. They have a very large number of lightly active clients:

- meters,
- environmental sensors,
- industrial controllers,
- cameras sending metadata,
- or vehicles and mobile devices that reconnect as coverage changes.

In that kind of deployment, the broker's main job is not to push the highest possible burst throughput. The harder job is:

- keeping many client identities alive,
- managing subscriptions per device,
- tolerating disconnect/reconnect cycles,
- and letting devices resume service cleanly.

MQTT fits that pattern well because the protocol assumes long-lived clients, stable client identities, and broker-managed session state. In other words, the things that look like overhead in a fan-out stress test become useful features here.

### 2. Small telemetry messages

MQTT performs especially well when messages are small and frequent rather than large and bulky.

Typical edge telemetry is often:

- a temperature reading,
- a machine-health counter,
- a GPS coordinate,
- a binary sensor value,
- or a small JSON record.

Those payloads might be only tens or hundreds of bytes. In that regime, the benefits of a simple publish protocol over a persistent connection matter a lot.

Why this helps MQTT:

- the device opens one connection and reuses it,
- the publish path is simple,
- the topic model maps naturally to per-device or per-sensor data,
- and the protocol was designed for constrained links rather than fat data-center pipes.

So MQTT is usually stronger for "many tiny updates over time" than for "huge message bodies at maximum throughput."

### 3. Persistent sessions

Persistent sessions are one of the clearest reasons MQTT flourishes in edge deployments.

When a client reconnects with the same identity, the broker can preserve useful state such as:

- subscriptions,
- in-flight QoS messages,
- queued messages for offline delivery,
- and the client's logical continuity in the system.

That matters a lot for sleepy or mobile devices. Without persistent sessions, every reconnect behaves almost like a fresh client. With persistent sessions, reconnect becomes "resume where you left off."

This is exactly the kind of behavior edge systems want:

- a field device sleeps,
- wakes up later,
- reconnects,
- and continues receiving commands or draining buffered data without rebuilding everything from scratch.

That is much more valuable than raw maximum throughput in many real deployments.

### 4. QoS-based reliability

MQTT also performs best when the application cares about delivery guarantees more than absolute speed.

The QoS levels give a practical tradeoff space:

- `QoS 0`: lowest overhead, best-effort delivery
- `QoS 1`: at-least-once delivery, usually the practical sweet spot
- `QoS 2`: exactly-once delivery, highest coordination overhead

This is important because IoT traffic is not all equal.

Examples:

- a temperature stream may be fine at `QoS 0`,
- an equipment alarm may need `QoS 1`,
- and a financial or safety-critical actuation event may justify `QoS 2`.

MQTT flourishes when that flexibility matters. A lighter fire-and-forget broker may win a pure speed contest, but MQTT becomes stronger when the user says, "I care that this command or alarm is eventually delivered correctly, even if the link drops in the middle."

### 5. Retained state

Retained messages are another place where MQTT fits edge systems very naturally.

A retained message lets the broker remember the latest value for a topic and send it immediately to a new or returning subscriber.

That is extremely useful for:

- the latest device configuration,
- desired control state,
- the most recent known sensor status,
- or the current online/offline mode of a subsystem.

In practical terms, retained state means a reconnecting device does not have to wait for the next periodic control message. It can reconnect and immediately receive the latest command or configuration.

That is a big operational advantage in command-and-control systems, and a pure throughput benchmark does not really capture it.

### 6. Reconnect-heavy edge environments

This point ties the others together.

MQTT is strongest in environments where disconnects are normal rather than exceptional:

- spotty Wi-Fi,
- cellular dead zones,
- power-saving sleep cycles,
- mobile clients moving between networks,
- or edge gateways restarting after outages.

When reconnects are common, features such as:

- persistent sessions,
- Last Will,
- queued QoS delivery,
- and retained state

become core application behavior.

That is the real reason MQTT is so common in IoT edge. It is not just a transport protocol. It is a convenient operational model for unreliable connectivity.

### The deeper pattern

All of these points reduce to one broader idea:

> MQTT flourishes when broker-side state is useful.

In your transient fan-out benchmark, broker-side state mostly behaves like extra cost:

- more session machinery,
- more acknowledgments,
- more internal tracking,
- more buffering under overload.

But in real edge systems, that same machinery is often exactly what users want. They are not paying that cost by accident; they are buying reconnect continuity, durable delivery semantics, and current-state synchronization.

So if you want MQTT to show good results in a benchmark, the experiment should be built around those properties rather than around extreme burst fan-out amplification.
