# Docker-based Test Harness (Static Topology)

This document specifies a simple, Docker-first design for stress-testing a Zenoh cluster with static, hardcoded topology and auto-discovery disabled. It complements `DESIGN.md` and includes minimal code snippets for clarity (not a full implementation).

Pinned decisions (Phase 0):
- Zenoh version: 1.5.1 (use Docker tag and crate version aligned to 1.5.1)
- Transport: TCP
- Topology: 3-router star (router2 -> router1, router3 -> router1)
- Metrics: CSV-only (Prometheus disabled initially)

## Objectives

- Use Docker Compose to run a fixed set of Zenoh routers and client roles.
- Disable auto connecting/discovery; use explicit listeners and peer lists.
- Keep runs deterministic and reproducible; sweep scenarios by restarting client containers with new parameters.

## Topologies

- Single-router baseline: `router1` only; all clients connect to `router1:7447`.
- 3-router cluster (choose one):
  - Star: `router2` <-> `router1`, `router3` <-> `router1`.
  - Ring: `router1` <-> `router2` <-> `router3` <-> `router1`.

## Compose file skeleton

Minimal `docker-compose.yml` skeleton (to be refined later):

```yaml
version: "3.9"

networks:
  zenoh-net:
    driver: bridge

services:
  router1:
    image: eclipse/zenoh:1.5.1 # pinned
    container_name: router1
    command: ["zenohd", "-c", "/etc/zenoh/router1.json5"]
    ports:
      - "7447:7447" # TCP listener
    volumes:
      - ./config/router1.json5:/etc/zenoh/router1.json5:ro
    networks: [zenoh-net]
    environment:
      - RUST_LOG=info

  router2:
    image: eclipse/zenoh:1.5.1
    container_name: router2
    command: ["zenohd", "-c", "/etc/zenoh/router2.json5"]
    volumes:
      - ./config/router2.json5:/etc/zenoh/router2.json5:ro
    networks: [zenoh-net]
    environment:
      - RUST_LOG=info

  router3:
    image: eclipse/zenoh:1.5.1
    container_name: router3
    command: ["zenohd", "-c", "/etc/zenoh/router3.json5"]
    volumes:
      - ./config/router3.json5:/etc/zenoh/router3.json5:ro
    networks: [zenoh-net]
    environment:
      - RUST_LOG=info

  # Example client roles (images are placeholders until harness exists)
  publisher:
    image: alpine:3.19
    container_name: publisher
    command: ["/bin/sh", "-c", "echo placeholder; sleep infinity"]
    networks: [zenoh-net]
    depends_on: [router1]

  subscriber:
    image: alpine:3.19
    container_name: subscriber
    command: ["/bin/sh", "-c", "echo placeholder; sleep infinity"]
    networks: [zenoh-net]
    depends_on: [router1]
```

Notes
- Replace `eclipse/zenoh:latest` with a pinned tag for reproducibility.
- Client images are placeholders; they will be replaced by the bench harness container later.

## Router config skeletons (JSON5)

Disable discovery/scouting, define TCP listeners, and hardcode peers (3-router star).

`config/router1.json5`
```json5
{
  // Disable discovery/autoconnect
  scouting: { enabled: false },
  // Listen on TCP 7447
  listeners: ["tcp/0.0.0.0:7447"],
  // Peers for star topology (optional to peer back to leaves)
  peers: [],
  // Optional: runtime, plugins, storage, logging
}
```

`config/router2.json5`
```json5
{
  scouting: { enabled: false },
  listeners: ["tcp/0.0.0.0:7447"],
  peers: ["tcp/router1:7447"]
}
```

`config/router3.json5`
```json5
{
  scouting: { enabled: false },
  listeners: ["tcp/0.0.0.0:7447"],
  peers: ["tcp/router1:7447"]
}
```

Notes
- Service names `router1`, `router2`, `router3` resolve via Docker DNS on `zenoh-net`.
- Adjust `peers` to match “star” or “ring” exactly; avoid mixed topologies in the same run.

## Client connection policy

- All clients connect via explicit endpoints; no discovery:
  - Publishers/Subscribers: `tcp://router1:7447` (single-router) or bind to a specific router in 3-router tests.
  - Requesters/Queryables: same explicit endpoints; queryables register under known key prefixes.

## Minimal run flow

- Bring up routers only:
  ```bash
  docker compose up -d router1 router2 router3
  ```
- Verify peering via logs; ensure no discovery is active.
- Start client containers with parameters (will be replaced by harness later):
  ```bash
  docker compose up -d publisher subscriber
  ```
- For scenario sweeps, restart client containers with new env/args while keeping routers up.
- Collect CSV artifacts via bind mounts or `docker cp`.

## Resource and isolation notes

- Consider `deploy.resources.limits` to avoid host overcommit during local tests.
- For network impairment, use `tc netem` inside dedicated containers with `CAP_NET_ADMIN`.

## Harness interface contract (minimal, no code yet)

- Publisher CLI flags (planned): endpoint, topics, topic-count, pubs, payload-size, rate, duration, reliability, csv-path.
- Subscriber CLI flags (planned): endpoint, topics/expr, subs, reliability, csv-path, prom-port.
- Requester CLI flags (planned): endpoint, key-expr, qps, concurrency, timeout, agg-window, duration, csv-path.
- Queryable CLI flags (planned): endpoint, key-prefixes, reply-size, proc-delay, reliability, csv-path.

These map to container args/env in Compose for repeatability.

## Next steps

- Pin the Zenoh image version and validate config keys against that version.
- Add the actual `docker-compose.yml` and config files to the repo.
- Replace placeholder client images with the bench harness image once implemented.
- Provide a few ready-made Compose profiles for single-router, star, and ring.
