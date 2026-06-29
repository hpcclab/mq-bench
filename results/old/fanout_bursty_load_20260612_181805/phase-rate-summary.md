# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:20,baseline:60:20,burst:120:6000,recovery:180:20`

## Run Setup

- Subscribers: `1000`
- Publishers: `10`
- Subscribers per publisher/topic: `100`
- Payload: `64 bytes`

## Rate Definitions

```text
total publish rate = rate_per_publisher * publishers
controlled fan-out delivery target = total publish rate * subscribers_per_publisher
```

## Phase Message Rates

| Phase | Duration | Rate per publisher | Total publish rate | Fan-out delivery target |
|---|---:|---:|---:|---:|
| warmup | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
| baseline | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 120s | 6k msg/s | 60k msg/s | 6.00M msg/s |
| recovery | 180s | 20 msg/s | 200 msg/s | 20k msg/s |
