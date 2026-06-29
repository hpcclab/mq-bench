# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:20,baseline:60:100,burst:60:1000,recovery:60:100,burst:60:2000,recovery:60:100,burst:60:3000,recovery:60:100,burst:60:4000,recovery:60:100,burst:60:5000,recovery:60:100`

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
| baseline | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
| burst | 60s | 1k msg/s | 10k msg/s | 1.00M msg/s |
| recovery | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
| burst | 60s | 2k msg/s | 20k msg/s | 2.00M msg/s |
| recovery | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
| burst | 60s | 3k msg/s | 30k msg/s | 3.00M msg/s |
| recovery | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
| burst | 60s | 4k msg/s | 40k msg/s | 4.00M msg/s |
| recovery | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
| burst | 60s | 5k msg/s | 50k msg/s | 5.00M msg/s |
| recovery | 60s | 100 msg/s | 1k msg/s | 100k msg/s |
