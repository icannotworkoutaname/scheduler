# Monitoring

Prometheus + Grafana, dashboard pre-provisioned. The scheduler runs on the
host (`java -jar`); Prometheus scrapes it via `/actuator/prometheus`.

## Start

```
./scripts/monitoring-up.sh          # brings up postgres + prometheus + grafana + renderer
java -jar build/libs/scheduler-0.0.1-SNAPSHOT.jar --server.port=8080 &
java -jar build/libs/scheduler-0.0.1-SNAPSHOT.jar --server.port=8081 &
```

- Grafana: http://localhost:3000/d/chronos-overview  (anonymous, no login)
- Prometheus: http://localhost:9090

On WSL2 the script writes `.env` with the distro's IP because
`host.docker.internal` there points at Windows and can't reach the app inside
the Linux distro. On native Linux `.env` is removed and `host-gateway` is used.

## The three README figures

Each of these panels is built to be screenshotted on its own — title, axis
units, and reference lines carry the meaning without surrounding context.

| # | panel | how to get the screenshot |
|---|-------|---------------------------|
| ① | **Failover — shard takeover vs. trigger throughput** | run `scripts/scenario1_node_kill.sh` against two live nodes under a steady task load. Crop to include the throughput dip AND its recovery — the recovery is the point. |
| ② | **Duplicate triggers absorbed by the optimistic lock** | run `scripts/scenario6_freeze_double_fire.sh`; crop to that ~2min window. Flat at zero otherwise. |
| ③ | **Trigger delay — P50 / P99 vs. SLO** | any steady-state window with load; the two dashed lines are the 200ms / 1s SLOs, both percentile lines should sit below them. Do NOT include a failover window — the tail spike during takeover is real but muddies this figure. |

## Rendering panels to PNG (for the README)

The `renderer` service does server-side PNG rendering, so 8/28 can script it:

```
G=http://localhost:3000/render/d-solo/chronos-overview/chronos-scheduler
curl -o fig1.png "$G?panelId=1&from=now-12m&to=now&width=1400&height=560&theme=light&tz=UTC"
curl -o fig2.png "$G?panelId=2&from=<epoch_ms>&to=<epoch_ms>&width=1100&height=560&theme=light"
curl -o fig3.png "$G?panelId=3&from=now-10m&to=now&width=1100&height=560&theme=light"
```

Panel ids: 1 failover, 2 duplicate, 3 trigger-delay, 4 pending-spread,
5 poll-duration, 6 sink-duration, 7 dead-letter.

## dashboard.json

Provisioned read-only (`allowUiUpdates: false`) — the file is the source of
truth, edits in the UI won't stick. To change it, edit the JSON and Grafana
picks it up within 10s.
