# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:20,baseline:60:20,burst:60:2000,recovery:60:20`

## Run Setup

- Subscribers: `3000`
- Publishers: `30`
- Subscribers per publisher/topic: `100`
- Payload: `128 bytes`

## Rate Definitions

```text
total publish rate = rate_per_publisher * publishers
controlled fan-out delivery target = total publish rate * subscribers_per_publisher
```

## Phase Message Rates

| Phase | Duration | Rate per publisher | Total publish rate | Fan-out delivery target |
|---|---:|---:|---:|---:|
| warmup | 60s | 20 msg/s | 600 msg/s | 60k msg/s |
| baseline | 60s | 20 msg/s | 600 msg/s | 60k msg/s |
| burst | 60s | 2k msg/s | 60k msg/s | 6.00M msg/s |
| recovery | 60s | 20 msg/s | 600 msg/s | 60k msg/s |
