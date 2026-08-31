#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="scenario2-$(date +%s)-"

echo "=== resetting environment ==="
reset_environment

echo "=== configuring receiver to fail 100% ==="
configure_receiver 1

PORT=$(any_live_port)
echo "=== submitting 3 tasks (via port $PORT — scenario 1 may have killed the other node) ==="
for i in 1 2 3; do
  curl -s -X POST http://localhost:${PORT}/tasks -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$(date -u -d '+2 seconds' +%Y-%m-%dT%H:%M:%SZ)"'",
    "idempotencyKey": "'"${PREFIX}${i}"'", "callbackUrl": "http://localhost:9000/hook"
  }' > /dev/null
done

echo "=== waiting for retry/backoff to exhaust (2+4+8+16s ≈ 30s, measured 8/9) ==="
sleep 40

echo "=== restoring receiver to healthy for subsequent runs ==="
configure_receiver 0

echo "=== consistency report ==="
python3 "${SCRIPT_DIR}/consistency_check.py" --idempotency-prefix "$PREFIX" --expected-terminal-state dead
