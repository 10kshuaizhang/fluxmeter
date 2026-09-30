# Production Deployment Guide

Engine/API **4.8.4**. Use [the local trial](quickstart.md) first. The base Compose
stack and its production overlay are a single-host reference, not an HA deployment.
The [Helm chart](../deploy/helm/README.md) deploys the API only; Kafka, Redis,
Flink, and any optional Gateway/webhook worker must be provisioned separately.

## One accounting path

Applications / SDK / optional Gateway → HTTP API (Custody) → private Kafka →
Flink accounting → Redis projections → Usage API. Redis also stores admission
state, retry identities, reservations, and the Gateway outbox. ClickHouse is an
optional audit copy, not billing truth.

`202` means Kafka acknowledgement **and identity finalization**, not completed
billing. Queries are eventually consistent. Monitor settlement lag as well as
HTTP throughput. See [API reference](api-reference.md).

## Build the images you deploy

From the repository root (Java 17 and Docker required):

```bash
./gradlew clean build shadowJar
# Deployable JAR: build/libs/fluxmeter-4.8.4.jar
# Docker staging copy: build/docker/fluxmeter.jar
# fluxmeter-4.8.4-plain.jar is the thin application artifact, not the Flink job.
docker build -f api/Dockerfile -t your-registry/fluxmeter-api:4.8.4 .
docker build -f api/Dockerfile.webhook -t your-registry/fluxmeter-webhook:4.8.4 .
docker build -f flink/Dockerfile -t your-registry/fluxmeter-flink:4.8.4 .
```

Replace `your-registry` with your registry, push the built images there, and pin
immutable digests for rollout. These tags describe artifacts **you build**; they
are not a claim that public images or SDK packages have been published.
Use the actual `api/Dockerfile`: it includes all API modules, the pricing catalog,
and the canonical OpenAPI document. Copying `main.py` alone is insufficient.

## Kafka

Keep broker access private. Provision at least three brokers for replication
factor 3 with `min.insync.replicas=2`. Create the topics used by the base stack:

```bash
for topic in token-events budget-alerts token-events-dlq token-events-quarantine; do
  kafka-topics.sh --bootstrap-server kafka-bootstrap:9092 --create --if-not-exists \
    --topic "$topic" --partitions 12 --replication-factor 3 \
    --config min.insync.replicas=2 --config retention.ms=2592000000
done
kafka-topics.sh --bootstrap-server kafka-bootstrap:9092 --create --if-not-exists \
  --topic metering-watermarks --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2
```

Twelve partitions is a starting configuration, not a capacity guarantee. Match
parallelism to measured workload and partition count. Plan retention and storage
for your replay policy; verify broker connectivity from all server components.
Managed brokers requiring TLS/SASL need verified client configuration in each
component; setting `KAFKA_BROKERS` alone does not configure authentication.

## Redis

Current Python clients use `redis.Redis`; Java sinks use `JedisPool` with
multi-key operations. Use a compatible **single writable endpoint**. Redis
Cluster sharding is not a drop-in supported configuration; do not infer cluster
support from a customer-count threshold. Test any managed failover endpoint and
its authentication against both client implementations.

```conf
appendonly yes
appendfsync everysec
maxmemory 8gb
maxmemory-policy noeviction
```

Choose memory from measurements, not this illustrative limit. `noeviction`
prevents silent eviction of billing state; memory exhaustion still stops writes.
AOF `everysec` has a durability tradeoff and is not a zero-loss guarantee. Test
backup/restore and failover.

The 30-day HTTP retry identity registry can dominate memory:

```
identity memory ≈ measured bytes/identity × unique accepted events/sec × 2,592,000
```

Add counters, rollups, outbox, reservations, replication, and operational headroom.
Flink's default 600-second projection deduplication is a separate crash-safety
window; it is not the client retry window. Measure `MEMORY USAGE` using realistic
IDs and payload traffic before selecting memory or retention.

## Flink (Kubernetes Operator example)

Install a compatible Flink Kubernetes Operator and provide the service account,
checkpoint storage credentials, and network access. The entry point reads
**environment variables**, not `--KAFKA_BROKERS=...` CLI arguments. The same
pricing catalog must reach the API and Flink; the repository Dockerfiles include
`config/pricing.json`.

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: fluxmeter
spec:
  image: your-registry/fluxmeter-flink:4.8.4
  flinkVersion: v1_18
  serviceAccount: flink
  flinkConfiguration:
    state.backend: rocksdb
    state.backend.incremental: "true"
    state.checkpoints.dir: s3://your-bucket/fluxmeter/checkpoints
    state.savepoints.dir: s3://your-bucket/fluxmeter/savepoints
    taskmanager.numberOfTaskSlots: "2"
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: KAFKA_BROKERS
              value: kafka-bootstrap:9092
            - name: REDIS_HOST
              value: redis-primary
            - name: REDIS_PORT
              value: "6379"
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: fluxmeter-secrets
                  key: redis-password
            - name: CHECKPOINT_DIR
              value: s3://your-bucket/fluxmeter/checkpoints
            - name: PRICING_FILE
              value: /opt/flink/usrlib/pricing.json
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-1.18.1.jar
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
  job:
    jarURI: local:///opt/flink/usrlib/fluxmeter.jar
    entryClass: io.fluxmeter.job.TokenUsageAggregator
    parallelism: 2
    upgradeMode: savepoint
```

Enable the S3 filesystem plugin and provision bucket permissions before using
this example; storage credentials and Operator installation are environment
specific. Resource settings are illustrative. The entry point currently sets
30-second checkpoints when `CHECKPOINT_DIR` is nonempty and a fixed-delay restart
strategy (10 attempts, 5 seconds); a contradictory YAML restart strategy will
not override that code. Verify checkpoint completion and a restore before rollout.

## API and readiness

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fluxmeter-api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: fluxmeter-api
  template:
    metadata:
      labels:
        app: fluxmeter-api
    spec:
      containers:
        - name: api
          image: your-registry/fluxmeter-api:4.8.4
          ports:
            - containerPort: 8000
          env:
            - name: REDIS_HOST
              value: redis-primary
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: fluxmeter-secrets
                  key: redis-password
            - name: KAFKA_BROKERS
              value: kafka-bootstrap:9092
            - name: FLUXMETER_AUTH_OPTIONAL
              value: "false"
            - name: BUDGET_FAIL_POLICY
              value: closed
            - name: WATERMARK_HEARTBEAT_ENABLED
              value: "true"
            - name: FLUXMETER_API_KEY
              valueFrom:
                secretKeyRef:
                  name: fluxmeter-secrets
                  key: api-key
            - name: FLUXMETER_ADMIN_KEY
              valueFrom:
                secretKeyRef:
                  name: fluxmeter-secrets
                  key: admin-key
          readinessProbe:
            httpGet:
              path: /ready
              port: 8000
            timeoutSeconds: 15
            periodSeconds: 20
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: fluxmeter-api
spec:
  selector:
    app: fluxmeter-api
  ports:
    - port: 8000
      targetPort: 8000
```

Create `fluxmeter-secrets` with `api-key`, `admin-key`, and `redis-password` before
applying. `/health` is process liveness; `/ready` checks Redis, Kafka, and consumption
of a causal Flink probe. Set probe timeouts above configured Kafka ACK + Flink
probe timeouts; dependency failure should remove traffic, not trigger restart
loops. Watermark heartbeats let low-traffic event-time windows close.

Run the optional Gateway with the API image and
`uvicorn gateway_app:app --host 0.0.0.0 --port 8080`; configure its provider access
and outbox as described in [gateway.md](gateway.md). Deploy the webhook image
separately if using alerts. Both need the correct Kafka/Redis endpoints and secrets.

For a single-host evaluation of the production security overlay:

```bash
./gradlew shadowJar
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
# Required environment: REDIS_PASSWORD, FLUXMETER_API_KEY,
# FLUXMETER_ADMIN_KEY, GRAFANA_ADMIN_PASSWORD.
```

The overlay retains single-node services and base host-port mappings. Restrict
those ports to the intended network; it does not provide HA or a public ingress.

## SDK and control behavior

```python
import os
from fluxmeter import FluxMeter

meter = FluxMeter(
    api_url="https://metering.example.com",
    api_key=os.environ["FLUXMETER_API_KEY"],
    environment="production",
)
```

SDKs use HTTP only; they have no Kafka SASL settings or local WAL in the 2.x line.
Handle typed delivery errors and stable-ID retries. The Python wrapper warns and
supports `on_metering_error`; see [SDK docs](../sdk/python/README.md).
A check creates no hold. Redis failure can use a recent cached decision before
fail policy; streaming token estimates are not a zero-overspend guarantee.

## Capacity and rollout gates

There is no validated 100K sustained full-pipeline capacity, linear-scaling, or
$1,550/month claim. Existing [benchmark evidence](load-testing.md) distinguishes:

| Recorded scope | Result | Limit |
| --- | --- | --- |
| 5m warmup + 30m HTTP Custody measurement | 10,034.90 events/s; p99 173ms | 0.149% generator offers dropped; downstream lag accumulated |
| Corrected p12 full stack, 60s sample | 7,772.28 events/s; p99 1,472ms; end lag 333 | Short sample, not sustained acceptance |
| Batch sample | 32,567.14 events/s; p99 3,959ms | Below the 100K gate |
| Historical 1M | Internal Kafka/Flink burst | Not HTTP sustained capacity |

These are repository records, not new measurements for this release. Size using
representative sustained traffic, settlement lag, checkpoint recovery, identity
retention, and current infrastructure quotes. Track the remaining gates in
[Issue #3 status](issue-3-status.md).

Before rollout, verify:

- Authentication and customer isolation; private Kafka/Redis endpoints.
- `/ready` failure on unavailable Redis/Kafka or stalled Flink; recovery after restart.
- A unique HTTP event appears with exact expected tokens and cost in the Usage API.
- Stable-ID retry, quarantine handling, reservation lifecycle, and delivery-error recovery.
- Flink checkpoint restore, Redis backup/restore, retention, memory, and consumer lag.
- Workload-specific HTTP and settlement SLOs at representative peak load; no assumed check latency.
