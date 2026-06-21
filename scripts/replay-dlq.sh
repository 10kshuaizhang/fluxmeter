#!/usr/bin/env bash
# Replay DLQ events to token-events. See docs/runbooks/dlq-replay.md
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BROKERS="${KAFKA_BROKERS:-localhost:9094}"
python3 "$ROOT/scripts/dlq_replay.py" --brokers "$BROKERS" "$@"
