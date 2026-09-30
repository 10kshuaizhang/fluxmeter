# FluxMeter Helm Chart

Deploy the API only. Provision the webhook worker separately if using alerts. Flink uses [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) — see `docs/production-deploy.md`.

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · **Production guide:** [docs/production-deploy.md](../../docs/production-deploy.md)

## Install

```bash
kubectl create secret generic fluxmeter-secrets \
  --from-literal=api-key="$FLUXMETER_API_KEY" \
  --from-literal=admin-key="$FLUXMETER_ADMIN_KEY" \
  --from-literal=redis-password="$REDIS_PASSWORD"

helm install fluxmeter ./deploy/helm/fluxmeter \
  -f deploy/helm/fluxmeter/values.yaml \
  --set image.api.repository=your-registry/fluxmeter-api \
  --set image.api.tag=4.8.4 \
  --set redis.host=redis-primary \
  --set redis.existingSecret=fluxmeter-secrets \
  --set kafka.brokers=kafka-bootstrap:9092
```

Build and publish your API image first using the [production guide](../../docs/production-deploy.md). Default image paths are placeholders, not published-image promises. API liveness uses `/health`; readiness uses `/ready` with a 15-second timeout. Increase it if configured Kafka/Flink probe deadlines increase.

## Monitoring

When `monitoring.enabled=true`, PrometheusRule alerts cover:

- Kafka consumer lag on `token-events`
- `global:last_window_end` stall (export `fluxmeter_last_window_end_ms` from Redis exporter)
- Reconciliation drift (`metrics:reconciliation_drift` in Redis)

## External dependencies

- Kafka cluster (MSK / Confluent / Redpanda)
- Redis single writable endpoint (current clients are not Redis Cluster clients)
- RocksDB + S3 checkpoints for Flink (not bundled)
