# Fan-Out Bursty Load Phase Rates

Source profile: `baseline:60:200,burst:60:2000,recovery:60:200`

## Run Setup

- Subscribers: `1000`
- Publishers: `10`
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
| baseline | 60s | 200 msg/s | 2k msg/s | 200k msg/s |
| burst | 60s | 2k msg/s | 20k msg/s | 2.00M msg/s |
| recovery | 60s | 200 msg/s | 2k msg/s | 200k msg/s |
