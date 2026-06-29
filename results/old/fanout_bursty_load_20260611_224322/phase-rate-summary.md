# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:10,baseline:60:10,burst:120:50,recovery:90:10,burst:120:100,recovery:90:10,burst:120:250,recovery:90:10,burst:120:500,recovery:120:10`

## Run Setup

- Subscribers: `1000`
- Publishers: `10`
- Subscribers per publisher/topic: `100`
- Payload: `1024 bytes`

## Rate Definitions

```text
total publish rate = rate_per_publisher * publishers
controlled fan-out delivery target = total publish rate * subscribers_per_publisher
```

## Phase Message Rates

| Phase | Duration | Rate per publisher | Total publish rate | Fan-out delivery target |
|---|---:|---:|---:|---:|
| warmup | 60s | 10 msg/s | 100 msg/s | 10k msg/s |
| baseline | 60s | 10 msg/s | 100 msg/s | 10k msg/s |
| burst | 120s | 50 msg/s | 500 msg/s | 50k msg/s |
| recovery | 90s | 10 msg/s | 100 msg/s | 10k msg/s |
| burst | 120s | 100 msg/s | 1k msg/s | 100k msg/s |
| recovery | 90s | 10 msg/s | 100 msg/s | 10k msg/s |
| burst | 120s | 250 msg/s | 2k msg/s | 250k msg/s |
| recovery | 90s | 10 msg/s | 100 msg/s | 10k msg/s |
| burst | 120s | 500 msg/s | 5k msg/s | 500k msg/s |
| recovery | 120s | 10 msg/s | 100 msg/s | 10k msg/s |
