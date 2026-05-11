# Benchmark plots

- Summary: `/home/cc/mq-bench/results/fanout_steady_load_20260508_044509/raw_data/summary.csv`

## Table of contents

- [Throughput vs Offered Rate](#throughput-vs-offered-rate)
- [P99 latency vs Offered Rate](#p99-latency-vs-offered-rate)
- [Max CPU% vs Offered Rate](#max-cpu-vs-offered-rate)
- [Max Memory% vs Offered Rate](#max-memory-vs-offered-rate)
- [Latency vs Payload](#latency-vs-payload)
- [Resource Usage vs Payload](#resource-usage-vs-payload)
- [Throughput vs Subscribers](#throughput-vs-subscribers)
- [Latency vs Subscribers](#latency-vs-subscribers)
- [Resource Usage vs Subscribers](#resource-usage-vs-subscribers)
- [Network Usage vs Subscribers](#network-usage-vs-subscribers)

## Throughput vs Offered Rate

### payload=1024B

![throughput payload 1024](throughput_vs_rate_payload1024.png)

## P99 latency vs Offered Rate

### payload=1024B

![p99 payload 1024](p99_vs_rate_payload1024.png)

## Max CPU% vs Offered Rate

### payload=1024B

![cpu payload 1024](max_cpu_vs_rate_payload1024.png)

## Max Memory% vs Offered Rate

### payload=1024B

![mem payload 1024](max_mem_vs_rate_payload1024.png)

## Latency vs Payload

### P50 latency

#### rate=50/s

![p50 vs payload r50](p50_ms_vs_payload_rate50.png)

#### rate=100/s

![p50 vs payload r100](p50_ms_vs_payload_rate100.png)

#### rate=200/s

![p50 vs payload r200](p50_ms_vs_payload_rate200.png)

#### rate=300/s

![p50 vs payload r300](p50_ms_vs_payload_rate300.png)

#### rate=400/s

![p50 vs payload r400](p50_ms_vs_payload_rate400.png)

#### rate=500/s

![p50 vs payload r500](p50_ms_vs_payload_rate500.png)

#### rate=600/s

![p50 vs payload r600](p50_ms_vs_payload_rate600.png)

#### rate=700/s

![p50 vs payload r700](p50_ms_vs_payload_rate700.png)

### P95 latency

#### rate=50/s

![p95 vs payload r50](p95_ms_vs_payload_rate50.png)

#### rate=100/s

![p95 vs payload r100](p95_ms_vs_payload_rate100.png)

#### rate=200/s

![p95 vs payload r200](p95_ms_vs_payload_rate200.png)

#### rate=300/s

![p95 vs payload r300](p95_ms_vs_payload_rate300.png)

#### rate=400/s

![p95 vs payload r400](p95_ms_vs_payload_rate400.png)

#### rate=500/s

![p95 vs payload r500](p95_ms_vs_payload_rate500.png)

#### rate=600/s

![p95 vs payload r600](p95_ms_vs_payload_rate600.png)

#### rate=700/s

![p95 vs payload r700](p95_ms_vs_payload_rate700.png)

### P99 latency

#### rate=50/s

![p99 vs payload r50](p99_ms_vs_payload_rate50.png)

#### rate=100/s

![p99 vs payload r100](p99_ms_vs_payload_rate100.png)

#### rate=200/s

![p99 vs payload r200](p99_ms_vs_payload_rate200.png)

#### rate=300/s

![p99 vs payload r300](p99_ms_vs_payload_rate300.png)

#### rate=400/s

![p99 vs payload r400](p99_ms_vs_payload_rate400.png)

#### rate=500/s

![p99 vs payload r500](p99_ms_vs_payload_rate500.png)

#### rate=600/s

![p99 vs payload r600](p99_ms_vs_payload_rate600.png)

#### rate=700/s

![p99 vs payload r700](p99_ms_vs_payload_rate700.png)

## Resource Usage vs Payload

### Max CPU%

#### rate=50/s

![cpu vs payload r50](max_cpu_vs_payload_rate50.png)

#### rate=100/s

![cpu vs payload r100](max_cpu_vs_payload_rate100.png)

#### rate=200/s

![cpu vs payload r200](max_cpu_vs_payload_rate200.png)

#### rate=300/s

![cpu vs payload r300](max_cpu_vs_payload_rate300.png)

#### rate=400/s

![cpu vs payload r400](max_cpu_vs_payload_rate400.png)

#### rate=500/s

![cpu vs payload r500](max_cpu_vs_payload_rate500.png)

#### rate=600/s

![cpu vs payload r600](max_cpu_vs_payload_rate600.png)

#### rate=700/s

![cpu vs payload r700](max_cpu_vs_payload_rate700.png)

### Max Memory%

#### rate=50/s

![mem vs payload r50](max_mem_perc_vs_payload_rate50.png)

#### rate=100/s

![mem vs payload r100](max_mem_perc_vs_payload_rate100.png)

#### rate=200/s

![mem vs payload r200](max_mem_perc_vs_payload_rate200.png)

#### rate=300/s

![mem vs payload r300](max_mem_perc_vs_payload_rate300.png)

#### rate=400/s

![mem vs payload r400](max_mem_perc_vs_payload_rate400.png)

#### rate=500/s

![mem vs payload r500](max_mem_perc_vs_payload_rate500.png)

#### rate=600/s

![mem vs payload r600](max_mem_perc_vs_payload_rate600.png)

#### rate=700/s

![mem vs payload r700](max_mem_perc_vs_payload_rate700.png)

## Throughput vs Subscribers

### payload=1024B

**Log Scale:**

![throughput vs subs payload 1024 log](throughput_vs_subs_payload1024_log.png)

**Linear Scale:**

![throughput vs subs payload 1024 linear](throughput_vs_subs_payload1024_linear.png)

## Latency vs Subscribers

### P50 latency

#### payload=1024B

![p50 vs subs payload 1024](p50_ms_vs_subs_payload1024.png)

### P95 latency

#### payload=1024B

![p95 vs subs payload 1024](p95_ms_vs_subs_payload1024.png)

### P99 latency

#### payload=1024B

![p99 vs subs payload 1024](p99_ms_vs_subs_payload1024.png)

## Resource Usage vs Subscribers

### Max CPU%

#### payload=1024B

![max cpu vs subs payload 1024](max_cpu_vs_subs_payload1024.png)

### Max Memory%

#### payload=1024B

![max mem vs subs payload 1024](max_mem_perc_vs_subs_payload1024.png)

### Avg CPU%

#### payload=1024B

![avg cpu vs subs payload 1024](avg_cpu_vs_subs_payload1024.png)

### Avg Memory%

#### payload=1024B

![avg mem vs subs payload 1024](avg_mem_perc_vs_subs_payload1024.png)

### Avg CPU Cores Used

#### payload=1024B

![avg cpu cores vs subs payload 1024](avg_cpu_cores_vs_subs_payload1024.png)

### Avg Memory (GB)

#### payload=1024B

![avg mem gb vs subs payload 1024](avg_mem_mb_vs_subs_payload1024.png)

## Network Usage vs Subscribers

### Peak Receive Bandwidth

#### payload=1024B

![max net rx vs subs payload 1024](max_net_rx_bps_vs_subs_payload1024.png)

### Peak Transmit Bandwidth

#### payload=1024B

![max net tx vs subs payload 1024](max_net_tx_bps_vs_subs_payload1024.png)

### Average Receive Bandwidth

#### payload=1024B

![avg net rx vs subs payload 1024](avg_net_rx_bps_vs_subs_payload1024.png)

### Average Transmit Bandwidth

#### payload=1024B

![avg net tx vs subs payload 1024](avg_net_tx_bps_vs_subs_payload1024.png)

