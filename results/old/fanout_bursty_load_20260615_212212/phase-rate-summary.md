# Fan-Out Bursty Load Phase Rates

Source profile: `baseline:60:20,burst:60:5000,recovery:60:20`

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
| baseline | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 60s | 5k msg/s | 50k msg/s | 5.00M msg/s |
| recovery | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
