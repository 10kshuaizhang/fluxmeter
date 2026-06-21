# Production Deployment Guide

This guide covers deploying FluxMeter to production on Kubernetes. The docker-compose setup is for development only.

## Architecture (Production)

```
[SDK / HTTP Ingest]
        ↓
[Kafka Cluster (3 brokers, RF=3)]
        ↓
[Flink on K8s (JobManager + N TaskManagers)]
        ↓
[Redis Cluster (3 primary + 3 replica)]
        ↓
[API (2+ replicas behind load balancer)]
```

---

## 1. Kafka

### Requirements
- 3+ brokers (minimum for RF=3)
- `min.insync.replicas=2`
- Retention: 7 days minimum (for replay/debugging), 30 days recommended (for re-rating)

### Topic Configuration

```bash
# token-events: main event stream
kafka-topics.sh --create --topic token-events \
  --partitions 24 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000  # 30 days

# budget-alerts: budget enforcement signals
kafka-topics.sh --create --topic budget-alerts \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000  # 7 days

# token-events-dlq: dead letter queue for late/failed events
kafka-topics.sh --create --topic token-events-dlq \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=2592000000  # 30 days
```

### Partition count

Rule of thumb: `partitions = max(target_throughput_eps / 50000, flink_parallelism * 2)`

- 100K eps → 12 partitions
- 500K eps → 24 partitions
- 1M+ eps → 48 partitions

### Managed alternatives
- AWS MSK
- Confluent Cloud
- Redpanda Cloud

---

## 2. Flink

### Kubernetes deployment (Flink Operator)

Use the [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/).

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: fluxmeter
spec:
  image: flink:1.18.1-java17
  flinkVersion: v1_18
  flinkConfiguration:
    state.backend: rocksdb
    state.checkpoints.dir: s3://your-bucket/fluxmeter/checkpoints
    state.savepoints.dir: s3://your-bucket/fluxmeter/savepoints
    execution.checkpointing.interval: "30000"
    execution.checkpointing.min-pause: "10000"
    restart-strategy: exponential-delay
    restart-strategy.exponential-delay.initial-backoff: "1s"
    restart-strategy.exponential-delay.max-backoff: "5min"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 4
  job:
    jarURI: s3://your-bucket/fluxmeter/fluxmeter-0.8.1.jar
    entryClass: io.fluxmeter.job.TokenUsageAggregator
    parallelism: 8
    args:
      - "--KAFKA_BROKERS=kafka-bootstrap:9092"
      - "--REDIS_HOST=redis-master"
      - "--CHECKPOINT_DIR=s3://your-bucket/fluxmeter/checkpoints"
```

### Key settings

| Setting | Development | Production |
|---------|-------------|------------|
| State backend | hashmap (in-memory) | rocksdb (disk-backed) |
| Checkpoints | disabled or local | S3/GCS every 30s |
| Parallelism | 2 | 8-16 (match Kafka partitions) |
| TM memory | 4GB | 4-8GB |
| TM count | 2 | 4-8 |
| Restart strategy | fixed-delay (10, 5s) | exponential (1s → 5min) |

### State backend: RocksDB

RocksDB stores state on local disk (SSD), not JVM heap. This eliminates OOM at high key cardinality and enables incremental checkpoints.

```yaml
flinkConfiguration:
  state.backend: rocksdb
  state.backend.rocksdb.localdir: /tmp/rocksdb
  state.backend.incremental: "true"
```

### Monitoring

Flink exposes Prometheus metrics. Key metrics to alert on:

| Metric | Alert threshold |
|--------|----------------|
| `flink_jobmanager_job_uptime` | < 60s (job restarting) |
| `flink_taskmanager_job_task_numRecordsInPerSecond` | < expected (backpressure) |
| `flink_jobmanager_job_numberOfFailedCheckpoints` | > 3 consecutive |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | > 80% of max |

---

## 3. Redis

### Requirements
- Redis 7+ with AOF persistence
- Cluster mode for >100K customers
- Memory: ~1KB per customer key set × number of customers

### Redis Cluster (3 primary + 3 replica)

```bash
# Minimum 6 nodes for production Redis Cluster
redis-cli --cluster create \
  redis-1:6379 redis-2:6379 redis-3:6379 \
  redis-4:6379 redis-5:6379 redis-6:6379 \
  --cluster-replicas 1
```

### Configuration

```conf
# redis.conf
appendonly yes
appendfsync everysec
maxmemory 8gb
maxmemory-policy noeviction
```

`noeviction` is critical — FluxMeter keys are billing data. Never silently drop them.

### Key space sizing

```
Per customer:
  ~10 keys (input_tokens, output_tokens, total_tokens, cost_usd, event_count, ...)
  ~50 bytes per key value
  × models used (~5 average)
  = ~2.5 KB per customer

Per span (24h TTL):
  ~5 keys × ~50 bytes = 250 bytes per active span

Idempotency keys (1h TTL):
  ~100 bytes per window result
  At 10K customers × 9 models × 6 windows/minute = ~540K keys/minute
  = ~54 MB rolling

Global keys: negligible
```

For 100K customers: ~250 MB base + ~54 MB idempotency = ~300 MB.
For 1M customers: ~2.5 GB + ~540 MB = ~3 GB.

### Managed alternatives
- AWS ElastiCache (Redis)
- GCP Memorystore
- Redis Cloud

---

## 4. API

### Dockerfile (production)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

### Kubernetes deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fluxmeter-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fluxmeter-api
  template:
    spec:
      containers:
        - name: api
          image: your-registry/fluxmeter-api:0.8.1
          ports:
            - containerPort: 8000
          env:
            - name: REDIS_HOST
              value: redis-master
            - name: KAFKA_BROKERS
              value: kafka-bootstrap:9092
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "512Mi"
              cpu: "1000m"
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 5
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
  type: ClusterIP
```

### API authentication (add before production)

FluxMeter's API has no built-in auth. Add one of:

1. **API key header** — simplest, add middleware checking `X-API-Key`
2. **OAuth2 / JWT** — use FastAPI's built-in OAuth2 support
3. **Service mesh** — Istio/Linkerd handles auth at the network level
4. **API gateway** — Kong, AWS API Gateway, or Cloudflare in front

---

## 5. SDK Configuration (Production)

```python
from fluxmeter import FluxMeter

meter = FluxMeter(
    kafka_brokers="kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    topic="token-events",
    environment="production",
    wal_path="/var/lib/fluxmeter/wal",  # Persistent volume, not /tmp
    producer_config={
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "fluxmeter-producer",
        "sasl.password": "${KAFKA_PASSWORD}",
    },
)
```

### WAL directory

In production, the WAL path must be on persistent storage (not ephemeral container filesystem):
- Kubernetes: mount a PersistentVolumeClaim
- EC2: use EBS-backed directory
- Serverless: use `/tmp` (accept limited durability) or disable WAL and rely on HTTP ingest

---

## 6. Observability

### Health check endpoint

```bash
# Basic health (API + Redis connectivity)
GET /health → {"status": "ok"}

# For comprehensive monitoring, check:
# 1. API health
curl http://fluxmeter-api:8000/health

# 2. Flink job running
curl http://flink-jobmanager:8081/jobs/overview | jq '.jobs[] | select(.state=="RUNNING")'

# 3. Kafka consumer lag
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group fluxmeter-aggregator

# 4. Redis connectivity
redis-cli ping
```

### Alerting rules

| Alert | Condition | Severity |
|-------|-----------|----------|
| Flink job down | No RUNNING job for > 2 min | Critical |
| Kafka consumer lag | Lag > 100K events for > 5 min | High |
| Redis unreachable | /health returns 500 | Critical |
| Checkpoint failing | > 3 consecutive failures | High |
| API latency | p99 > 100ms on /budget/check | Medium |

---

## 7. Capacity Planning

### Throughput to infrastructure mapping

| Events/sec | Kafka partitions | Flink parallelism | TMs (4GB each) | Redis memory |
|-----------|-----------------|-------------------|-----------------|-------------|
| 10K | 12 | 4 | 2 | 1 GB |
| 100K | 24 | 8 | 4 | 2 GB |
| 500K | 24 | 12 | 6 | 4 GB |
| 1M | 48 | 16 | 8 | 8 GB |

### Cost estimate (AWS, us-east-1)

| Component | Sizing | Monthly cost |
|-----------|--------|--------------|
| MSK (Kafka) | 3× m5.large, 1TB storage | ~$600 |
| EKS (Flink) | 4× m5.xlarge spot | ~$400 |
| ElastiCache (Redis) | r6g.large cluster (6 nodes) | ~$500 |
| ECS/EKS (API) | 3× 0.5 vCPU / 1GB | ~$50 |
| S3 (checkpoints) | ~100 GB | ~$3 |
| **Total** | | **~$1,550/month** |

For 100K events/sec sustained. Scales linearly.

---

## 8. Deployment Checklist

Before going live:

- [ ] Kafka: RF=3, min.insync.replicas=2, 30-day retention
- [ ] Flink: RocksDB state backend, S3 checkpoints, exponential restart
- [ ] Redis: AOF enabled, noeviction policy, cluster mode if >100K customers
- [ ] API: 3+ replicas, health checks, authentication middleware
- [ ] SDK: WAL on persistent storage, SASL_SSL to Kafka
- [ ] Monitoring: Flink job uptime, consumer lag, checkpoint health, API latency
- [ ] Alerting: PagerDuty/Opsgenie for critical alerts
- [ ] Backup: Redis RDB snapshot daily, Kafka topic mirroring to DR region
- [ ] Load test: run at 2× expected peak for 1 hour before launch
- [ ] Budget enforcement: verify /budget/check returns <10ms at load
