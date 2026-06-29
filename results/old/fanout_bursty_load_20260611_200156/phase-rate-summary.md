# Fan-Out Bursty Load Phase Rates

Source profile: `warmup:60:5,baseline:60:5,burst:120:20,recovery:90:5,burst:120:50,recovery:90:5,burst:120:100,recovery:90:5,burst:120:150,recovery:120:5`

## Run Setup

- Subscribers: `1000`
- Publishers: `10`
- Subscribers per publisher: `100`
- Payload: `1024 bytes`

## Rate Definitions

```text
total publish rate = rate_per_publisher * publishers
fan-out delivery target = total publish rate * subscribers
```

## Phase Message Rates

| Phase | Duration | Rate per publisher | Total publish rate | Fan-out delivery target |
|---|---:|---:|---:|---:|
| warmup | 60s | 5 msg/s | 50 msg/s | 50k msg/s |
| baseline | 60s | 5 msg/s | 50 msg/s | 50k msg/s |
| burst | 120s | 20 msg/s | 200 msg/s | 200k msg/s |
| recovery | 90s | 5 msg/s | 50 msg/s | 50k msg/s |
| burst | 120s | 50 msg/s | 500 msg/s | 500k msg/s |
| recovery | 90s | 5 msg/s | 50 msg/s | 50k msg/s |
| burst | 120s | 100 msg/s | 1k msg/s | 1.00M msg/s |
| recovery | 90s | 5 msg/s | 50 msg/s | 50k msg/s |
| burst | 120s | 150 msg/s | 2k msg/s | 1.50M msg/s |
| recovery | 120s | 5 msg/s | 50 msg/s | 50k msg/s |
