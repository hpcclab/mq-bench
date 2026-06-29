# Fan-Out Bursty Load Phase Rates

Source profile: `burst:60:11000,recovery:60:100`

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
| burst | 60s | 11k msg/s | 110k msg/s | 11.00M msg/s |
| recovery | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
