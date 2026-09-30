# Outreach preparation validation — 2026-09-30

Base: `6573af8ceae190647b7132a3a2272dca08c67621`. Changes target engine/API 4.8.4 and Python SDK 2.0.1; JS SDK remains 2.0.0. A local version
bump does not imply published packages, images, a merged release, or site deployment.

## Verified

- Java 17 / Gradle 8.5: `./gradlew clean build shadowJar` passed. All 56 Java tests
  passed, including the two Redis-backed tests using a temporary, isolated Redis
  instance bound to loopback. No skipped tests in the final run.
- The Shadow JAR and thin `-plain.jar` have separate paths; the Docker staging
  copy is produced. `make` selects the exact current Shadow artifact.
- 119 Python API/core tests passed, including HTTP batch schema validation and
  the trial script's real-local-HTTP tests. The trial HTTP regression suite was rerun after the Docker fixes: 4 passed, including loopback proxy bypass and empty startup-error bodies.
- 21 Python SDK tests passed, including warning/callback delivery failures,
  callback failures, stream reconciliation/tracking failures, and preserving the
  provider result without repeating the provider call.
- `make validate-spec` and `git diff --check` passed.
- Helm 3.16.4 lint and rendering passed with a Redis secret configured. Compose,
  OpenAPI and CI YAML parse successfully.

## Docker follow-up verified

Docker Engine 28.3.2 / Compose 2.39.1 on a local 4-CPU, ~8GB Linux VM:

- `make demo-verify` passed against the actual Kafka → Flink → Redis path:
  one event, 1,600 tokens and numeric cost USD 0.00042.
- Full `make demo` passed after the fixes, including image builds and automatic
  Flink submission; its unique event produced the same exact tokens/cost.
- `make demo-proof PYTHON=<Python 3 environment with httpx>` passed reserve →
  meter → kill → settle → audit. The hold was released to zero; settled spend
  was USD 0.000181; ClickHouse matched all 301 output tokens and the kill marker.
- Local proof uses one API worker and two Flink slots, not benchmark sizing.
  The Flink image reuses its bundled curl and matching Prometheus JAR.

The first attempts exposed Docker Desktop stuck in startup (recovered without
resetting data), an external image-auth timeout (retry succeeded), and a system
proxy returning empty 503 responses for localhost. The loopback proxy handling
was fixed and regression-tested. One stack restart also left the API Kafka client
returning transport errors despite healthy fresh metadata queries; rebuilding
services restored the path. This run does not establish automatic recovery from
that transient connection condition or production failover guarantees.

## Still requires the target environment

- Kubernetes/Flink Operator rollout, provider streaming, failover, and production
  restore were not tested here. Deployment examples identify operator-provided
  registry, credentials, storage plugins and sizing.
- No throughput benchmark was rerun. Issue #3 sustained gates stay open;
  ADR-024's per-row batch amendment still needs reconciliation with the original
  issue's whole-batch acceptance language.
- VHS is not installed, so the recording source is updated but `demo.gif` has not
  been regenerated. Its wait now follows the actual proof command's success.

## Changes that affect callers

The batch runtime behavior is unchanged; the documented enums now match actual
wire responses. Python `wrap` preserves its default response behavior but warns
on metering/reconciliation failure and optionally calls `on_metering_error`.
`make demo` now fails when the first customer-usage proof fails; `make start`
remains the startup-only command.
