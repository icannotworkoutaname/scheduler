#!/usr/bin/env bash
#
# Brings up Prometheus + Grafana (docker-compose) with the dashboard
# pre-provisioned. The scheduler itself runs on the host (java -jar), so
# Prometheus has to reach back out to it — see grafana/prometheus.yml.
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if grep -qi microsoft /proc/version 2>/dev/null; then
  # WSL2 + Docker Desktop: host.docker.internal resolves to Windows, which
  # cannot reach a port bound inside the Linux distro. Pin the distro's own IP.
  ip="$(ip -4 addr show eth0 | awk '/inet /{print $2}' | cut -d/ -f1)"
  [ -n "$ip" ] || { echo "could not determine WSL eth0 IP" >&2; exit 1; }
  echo "CHRONOS_HOST=$ip" > .env
  echo "WSL2 detected — pinned Prometheus scrape host to $ip (.env)"
else
  rm -f .env
fi

docker compose up -d
echo ""
echo "Grafana:    http://localhost:3000/d/chronos-overview/chronos-scheduler"
echo "Prometheus: http://localhost:9090/targets"
echo ""
echo "Start the scheduler on the host so there's something to scrape:"
echo "  java -jar build/libs/scheduler-0.0.1-SNAPSHOT.jar --server.port=8080 &"
echo "  java -jar build/libs/scheduler-0.0.1-SNAPSHOT.jar --server.port=8081 &"
