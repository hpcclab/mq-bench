# Baremetal to VM Network Bandwidth Report

Date: 2026-06-11

## Hosts

| Role | Host | Address | Interface | Notes |
|---|---:|---:|---:|---|
| Local baremetal | `mq-bench-1` | `192.168.0.194` | `enp152s0np0` | Physical link reports `25000Mb/s`, full duplex |
| Remote VM | `vm48` | `192.168.0.245` | `eth0` | Virtual NIC; `ethtool` reports speed as unknown |

The route from the baremetal to the VM uses `enp152s0np0` directly on the `192.168.0.0/24` network:

```text
192.168.0.245 dev enp152s0np0 src 192.168.0.194
```

The VM can ping the baremetal with low latency. A sample VM-to-baremetal ping showed about `0.190 ms` average RTT.

## Summary

| Direction | Best observed throughput | Test shape |
|---|---:|---|
| Baremetal -> VM | `18.1 Gbit/s` | 4 parallel TCP streams |
| VM -> baremetal | `23.5 Gbit/s` | 8 parallel TCP streams, reverse-mode connection |

The local baremetal NIC is linked at 25 Gbit/s. The VM-to-baremetal direction gets close to the practical maximum for a 25GbE link after protocol and virtualization overhead. The baremetal-to-VM direction was lower, peaking around 18 Gbit/s in this measurement set.

## Measurement Method

`iperf3` is installed on the baremetal, but it was not installed on the VM. To avoid changing the VM package state, I used a temporary Python TCP streaming test instead.

The SSH connection was used only to start short-lived Python processes on the VM. The bandwidth payload itself was sent over direct TCP sockets between `192.168.0.194` and `192.168.0.245`; it was not tunneled through SSH.

Each test used:

| Setting | Value |
|---|---:|
| Test duration | `10 s` |
| TCP buffer chunk | `1 MiB` |
| Stream counts | `1`, `4`, `8` parallel streams |
| Reported unit | Gbit/s, calculated as `bytes * 8 / seconds / 1e9` |

For baremetal -> VM, the VM ran a TCP receiver on port `5201`, and the baremetal sent data for 10 seconds.

For VM -> baremetal, I used a reverse-mode test on port `5203`: the VM listened, the baremetal initiated the TCP connection, and then the VM sent data back over that established connection. This is similar in spirit to `iperf3 -R` and avoids the local inbound TCP issue observed during testing. Direct VM-initiated TCP to a local test listener failed with `No route to host`, while ICMP ping still worked.

## Results

### Baremetal -> VM

| Parallel streams | Bytes transferred | Duration | Throughput |
|---:|---:|---:|---:|
| 1 | `14,800,650,240` | `10.001 s` | `11.839 Gbit/s` |
| 4 | `22,627,221,504` | `10.006 s` | `18.091 Gbit/s` |
| 8 | `20,297,285,632` | `10.020 s` | `16.206 Gbit/s` |

### VM -> Baremetal

| Parallel streams | Bytes transferred | Duration | Throughput |
|---:|---:|---:|---:|
| 1 | `20,661,141,504` | `10.002 s` | `16.526 Gbit/s` |
| 4 | `28,381,806,592` | `10.001 s` | `22.702 Gbit/s` |
| 8 | `29,385,293,824` | `10.004 s` | `23.499 Gbit/s` |

## Interpretation

The available network capacity between the baremetal and the VM is well above 10 Gbit/s and can approach the 25GbE link rate in the VM -> baremetal direction.

The asymmetric result matters: baremetal -> VM peaked at about 18 Gbit/s, while VM -> baremetal reached about 23.5 Gbit/s. That suggests the VM receive path, virtualization overhead, queueing, or CPU scheduling in the VM may limit receive-side throughput before the physical 25GbE link is fully saturated.

For the bursty fan-out benchmark, a target of `1.5 million msg/s` with `1024 byte` payloads requires at least:

```text
1,500,000 msg/s * 1024 bytes * 8 = 12.288 Gbit/s
```

That is payload-only bandwidth. TCP/IP, broker protocol overhead, fan-out copies, batching behavior, acknowledgements, container networking, and user-space processing all add extra cost. So even though CPU and memory may not appear fully consumed, delivery throughput can still miss the target if the bottleneck is network egress/ingress, virtual NIC queues, socket buffers, broker backpressure, or subscriber receive capacity.

## Limitations

These measurements are useful for a practical bandwidth estimate, but they are not a perfect substitute for `iperf3`:

- The VM did not have `iperf3` installed, so this used a Python TCP fallback.
- Python user-space send/receive loops can under-report the maximum possible throughput.
- The test measured host-to-host TCP capacity, not broker-level throughput through containers or application protocols.
- The reverse test was needed because direct VM-initiated TCP to a local listener failed, likely due host firewall or routing policy for inbound TCP.

## Recommended Follow-up

For a cleaner baseline, install `iperf3` on the VM and repeat:

```bash
# On the VM
iperf3 -s

# On the baremetal, baremetal -> VM
iperf3 -c 192.168.0.245 -t 30 -P 4

# On the baremetal, VM -> baremetal reverse mode
iperf3 -c 192.168.0.245 -t 30 -P 4 -R
```

If the benchmark runs inside containers, also test from inside the same containers or network namespace used by the broker/subscribers. That will include container bridge, veth, offload, MTU, and queueing effects that a host-level test may miss.
