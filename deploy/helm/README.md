# FluxMeter Helm Chart

Deploy API + webhook worker. Flink uses [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) — see `docs/production-deploy.md`.

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · **Production guide:** [docs/production-deploy.md](../../docs/production-deploy.md)

## Install

```bash
kubectl create secret generic fluxmeter-secrets \
  --from-literal=api-key="$FLUXMETER_API_KEY" \
  --from-literal=admin-key="$FLUXMETER_ADMIN_KEY"

helm install fluxmeter ./deploy/helm/fluxmeter \
  -f deploy/helm/fluxmeter/values.yaml
```

## Monitoring

When `monitoring.enabled=true`, PrometheusRule alerts cover:

- Kafka consumer lag on `token-events`
- `global:last_window_end` stall (export `fluxmeter_last_window_end_ms` from Redis exporter)
- Reconciliation drift (`metrics:reconciliation_drift` in Redis)

## External dependencies

- Kafka cluster (MSK / Confluent / Redpanda)
- Redis (ElastiCache / Memorystore)
- RocksDB + S3 checkpoints for Flink (not bundled)
