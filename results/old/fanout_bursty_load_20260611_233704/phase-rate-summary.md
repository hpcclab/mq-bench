# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:20,baseline:60:20,burst:120:1000,recovery:90:20,burst:120:2000,recovery:90:20,burst:120:3000,recovery:90:20,burst:120:4000,recovery:90:20,burst:120:6000,recovery:180:20`

## Run Setup

- Subscribers: `1000`
- Publishers: `10`
- Subscribers per publisher/topic: `100`
- Payload: `256 bytes`

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
| burst | 120s | 1k msg/s | 10k msg/s | 1.00M msg/s |
| recovery | 90s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 120s | 2k msg/s | 20k msg/s | 2.00M msg/s |
| recovery | 90s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 120s | 3k msg/s | 30k msg/s | 3.00M msg/s |
| recovery | 90s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 120s | 4k msg/s | 40k msg/s | 4.00M msg/s |
| recovery | 90s | 20 msg/s | 200 msg/s | 20k msg/s |
| burst | 120s | 6k msg/s | 60k msg/s | 6.00M msg/s |
| recovery | 180s | 20 msg/s | 200 msg/s | 20k msg/s |
