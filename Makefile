.PHONY: demo demo-slow demo-down test build up down

# One command: bring everything up, run the node-freeze scenario, narrate it,
# clean up on any exit. Accelerated timings (8s lease TTL) so it runs in ~30s.
demo:
	@bash scripts/demo.sh

# Same, with production timings (30s lease TTL) — ~90s.
demo-slow:
	@DEMO_FAST=0 bash scripts/demo.sh

# Stop the compose stack the demo left running.
demo-down down:
	@docker compose down

build:
	@./gradlew bootJar -q --console=plain

# Full test suite (unit + Testcontainers chaos scenarios). Needs Docker.
test:
	@./gradlew test

# Just the monitoring stack (Grafana at :3000, dashboard pre-provisioned).
up:
	@bash scripts/monitoring-up.sh
