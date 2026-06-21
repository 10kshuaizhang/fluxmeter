#!/usr/bin/env bash
# Pre-pull images via DaoCloud mirror when Docker Hub is unreachable.
set -euo pipefail

MIRROR="${DOCKER_MIRROR:-docker.m.daocloud.io}"

pull_and_tag() {
  local src="$1"
  local dst="$2"
  echo "  $dst <- $MIRROR/$src"
  docker pull "$MIRROR/$src"
  docker tag "$MIRROR/$src" "$dst"
}

echo "==> Pulling images via $MIRROR ..."
pull_and_tag "library/redis:7-alpine" "redis:7-alpine"
pull_and_tag "apache/kafka:3.7.0" "apache/kafka:3.7.0"
pull_and_tag "library/flink:1.18.1-java17" "flink:1.18.1-java17"
pull_and_tag "grafana/grafana:10.3.1" "grafana/grafana:10.3.1"
pull_and_tag "clickhouse/clickhouse-server:24.1" "clickhouse/clickhouse-server:24.1"
pull_and_tag "library/python:3.11-slim" "python:3.11-slim"
echo "==> Mirror pull complete"
