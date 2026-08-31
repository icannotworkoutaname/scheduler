#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="smoke-$(date +%s)-"

echo "=== resetting environment ==="
reset_environment

echo "=== submitting 5 tasks ==="
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8080/tasks -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$(date -u -d '+2 seconds' +%Y-%m-%dT%H:%M:%SZ)"'",
    "idempotencyKey": "'"${PREFIX}${i}"'", "callbackUrl": "http://localhost:9000/hook"
  }' > /dev/null
done

echo "=== waiting for processing ==="
sleep 6

echo "=== consistency report ==="
python3 "${SCRIPT_DIR}/consistency_check.py" --idempotency-prefix "$PREFIX"
