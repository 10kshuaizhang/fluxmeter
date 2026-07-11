.PHONY: build demo demo-full demo-lite demo-gateway start start-full start-lite start-saas stop-saas stop clean generate submit-job benchmark correctness-bench validate-spec load-test load-test-quick test-e2e test-lite test-unit test-unit-redis test-java

JAR = $(shell ls -t build/libs/fluxmeter-*.jar 2>/dev/null | head -1)

# Build the fat JAR (only needed for full/Flink mode)
build:
	./gradlew shadowJar

# --- LITE MODE (default) ---

# One-command lite demo: Redis + API + Grafana
demo: start
	@echo ""
	@echo "==================================="
	@echo " FluxMeter Demo Running (Lite Mode)"
	@echo "==================================="
	@echo " API:     http://localhost:8000/docs"
	@echo " Gateway: http://localhost:8080/v1/chat/completions (OpenAI-compatible proxy)"
	@echo " Grafana: http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Try: curl -X POST localhost:8000/ingest -H 'Content-Type: application/json' \\"
	@echo "   -d '{\"customerId\":\"cust_1\",\"modelId\":\"gpt-4o\",\"inputTokens\":100,\"outputTokens\":50}'"
	@echo "==================================="

# Backward-compatible alias
demo-lite: demo

# Gateway mock self-check (no live OpenAI)
demo-gateway:
	PYTHONPATH=api python demos/gateway_demo.py

# Start lite infrastructure (default)
start:
	docker compose up -d --build
	@echo "Lite stack started. API aggregates directly to Redis (no Flink)."

# Backward-compatible alias
start-lite: start

# --- FULL MODE (Kafka + Flink) ---

# Full demo: build + start infra + submit job + run generator
demo-full: build start-full
	@echo "Waiting for Flink cluster to be ready..."
	@sleep 10
	@$(MAKE) submit-job
	@echo ""
	@echo "==================================="
	@echo " FluxMeter Demo Running (Full Mode)"
	@echo "==================================="
	@echo " API:       http://localhost:8000/docs"
	@echo " Flink UI:  http://localhost:8081"
	@echo " Grafana:   http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Starting load generator (Ctrl+C to stop)..."
	@echo "==================================="
	@$(MAKE) generate

# Start full infrastructure (Kafka, Flink, Redis, API, Grafana)
start-full:
	docker compose -f docker-compose.full.yml up -d --build
	@echo "Full stack started. Kafka, Flink, Redis, API, Grafana running."

# --- SHARED ---

# Stop everything
stop:
	docker compose down 2>/dev/null || true
	docker compose -f docker-compose.full.yml down 2>/dev/null || true
	docker compose -f docker-compose.saas.yml down 2>/dev/null || true

# Clean build artifacts and containers
clean: stop
	./gradlew clean
	docker compose down -v 2>/dev/null || true
	docker compose -f docker-compose.full.yml down -v 2>/dev/null || true
	docker compose -f docker-compose.saas.yml down -v 2>/dev/null || true

# Validate open spec artifacts
validate-spec:
	./scripts/validate-spec.sh

# Tests
test-lite:
	pip install -q -r tests/requirements.txt
	pytest tests/test_lite_production.py -v --timeout=60

test-e2e:
	pip install -q -r tests/requirements.txt
	pytest tests/test_integration.py -v --timeout=300
	pytest tests/test_e2e_v2.py -v --timeout=300 -m v2

test-unit:
	pip install -q -r tests/requirements.txt
	pytest tests/test_auth_unit.py tests/test_billing_export.py tests/test_billing_export_partners.py \
		tests/test_hierarchy_reserve.py tests/test_api_key_budget.py tests/test_billing_dims.py \
		tests/test_control_plane_models.py tests/test_tenant_keys.py \
		tests/test_pricing_loader.py tests/test_pricing_validate.py \
		tests/test_rerate_tier.py tests/test_phase2_billing.py tests/test_gateway.py -v --timeout=60
	./gradlew test -q

test-unit-redis:
	pytest tests/test_lite_aggregate_unit.py tests/test_rollup.py \
		tests/test_usage_buckets.py tests/test_tier_e2e.py -v --timeout=60

test-java:
	./gradlew test

# Submit the Flink job to the cluster (parallelism 12 = 4 TM × 4 slots, capped for local Redis)
FLINK_PARALLELISM ?= 12

submit-job:
	docker cp $(JAR) fluxmeter-jobmanager:/opt/flink/fluxmeter.jar
	docker exec fluxmeter-jobmanager flink run \
		-d \
		-p $(FLINK_PARALLELISM) \
		-c io.fluxmeter.job.TokenUsageAggregator \
		/opt/flink/fluxmeter.jar

# Staged load test (full mode, 10K → 1M eps bursts)
load-test:
	./scripts/load-test.sh

# Quick load test (10K–500K only)
load-test-quick:
	QUICK=1 ./scripts/load-test.sh

# Run the baseline comparison (Flink vs ClickHouse)
benchmark:
	./baseline/benchmark.sh

# Known-event correctness + Flink checkpoint health (full mode)
correctness-bench:
	chmod +x scripts/correctness-bench.sh
	./scripts/correctness-bench.sh

# Run the load generator locally (requires Java 17, full mode)
generate:
	KAFKA_BROKERS=localhost:9094 \
	NUM_CUSTOMERS=10000 \
	NUM_THREADS=8 \
	TARGET_EPS=1000000 \
	java -cp $(JAR) io.fluxmeter.generator.LoadGenerator

# --- SAAS MODE ---

start-saas:
	docker compose -f docker-compose.saas.yml up -d --build
	@echo "SaaS stack started. API :8000, Control Plane :8001, Grafana :3000"

stop-saas:
	docker compose -f docker-compose.saas.yml down
