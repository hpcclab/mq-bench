# Artemis Burst4 300 Instability

## Summary

In the MQTT bursty fan-out run, Artemis becomes unstable during burst4, the `300 msg/s` per-publisher phase. With `10` publishers and `100` subscribers per publisher topic, that phase asks Artemis to deliver about `300k` fan-out messages per second.

The publishers remain steady, but Artemis cannot sustain that fan-out rate. Delivery falls behind, broker-side buffering grows, and then Artemis drains the backlog in uneven waves. That makes the per-second delivery rate look like it is going sharply up and down.

## Evidence

For run `fanout_bursty_20260623_021728_mqtt_artemis_p64_s1000_u10_bursty`:

| Phase | Target fan-out | Avg delivered | Notes |
|---|---:|---:|---|
| burst3, `200 msg/s/pub` | `200k msg/s` | ~`199k msg/s` | Stable, low loss |
| burst4, `300 msg/s/pub` | `300k msg/s` | ~`211k msg/s` | Falls behind, ~`30%` deficit |
| recovery after burst4, `50 msg/s/pub` | `50k msg/s` | ~`143k msg/s` | Still draining backlog |

The raw per-second subscriber aggregate shows burst4 delivery ranging from roughly `20k` to `405k msg/s`, even though publisher input stays near `300 msg/s` per publisher. That means the oscillation is not publisher-rate jitter; it is the broker alternately accumulating and draining queued work.

Latency confirms the same pattern. Burst4 average latency rises to about `10s`, and recovery remains high because subscribers are still receiving delayed messages from the overloaded phase.

Docker stats also line up with backlog pressure:

| Window | Avg delivered | Avg CPU | Memory change |
|---|---:|---:|---|
| burst4 first 20s | ~`172k msg/s` | ~`176%` | ~`633 MiB` to `635 MiB` |
| burst4 middle 20s | ~`189k msg/s` | ~`201%` | ~`635 MiB` to `636 MiB` |
| burst4 last 20s | ~`260k msg/s` | ~`388%` | ~`712 MiB` to `1.8 GiB` |
| recovery first 20s | ~`293k msg/s` | ~`391%` | ~`1.9 GiB` to `2.6 GiB` |

## Interpretation

Burst4 is the point where this Artemis setup crosses its sustainable fan-out capacity. It accepts the steady publish stream, but it cannot deliver all subscriber copies at the requested rate. Messages build up in broker memory, CPU ramps toward roughly four saturated cores, and delivery becomes bursty as Artemis catches up in waves.

The active subscriber count remains at `1000` and the run reports no client errors during the phase, so the most likely cause is Artemis broker-side saturation or backpressure, not publisher instability or subscriber churn.
