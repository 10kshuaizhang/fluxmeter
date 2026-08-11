#!/usr/bin/env bash
# Apply baseline/init.sql via clickhouse-client --multiquery.
# The ClickHouse 24.1 image entrypoint initdb heuristic is unreliable
# (pre-populated data dir + inverted ALWAYS_RUN flag), so we apply DDL explicitly.
set -euo pipefail

CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-fluxmeter-clickhouse}"
INIT_SQL="${INIT_SQL:-baseline/init.sql}"

for i in $(seq 1 60); do
  if docker exec "$CLICKHOUSE_CONTAINER" wget -q -O- http://127.0.0.1:8123/ping 2>/dev/null | grep -q Ok; then
    break
  fi
  sleep 1
done

docker exec -i "$CLICKHOUSE_CONTAINER" clickhouse-client --multiquery < "$INIT_SQL"
echo "Applied $INIT_SQL to $CLICKHOUSE_CONTAINER"
