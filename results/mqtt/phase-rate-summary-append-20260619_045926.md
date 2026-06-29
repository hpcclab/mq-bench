# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:20,baseline:60:20,burst:60:50,recovery:60:20,burst:60:100,recovery:60:20,burst:60:200,recovery:60:20,burst:60:300,recovery:60:50,burst:60:500,recovery:60:50`

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
| burst | 60s | 50 msg/s | 500 msg/s | 50k msg/s |
| recovery | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
| recovery | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 60s | 200 msg/s | 2k msg/s | 200k msg/s |
| recovery | 60s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 60s | 300 msg/s | 3k msg/s | 300k msg/s |
| recovery | 60s | 50 msg/s | 500 msg/s | 50k msg/s |
| burst | 60s | 500 msg/s | 5k msg/s | 500k msg/s |
| recovery | 60s | 50 msg/s | 500 msg/s | 50k msg/s |
