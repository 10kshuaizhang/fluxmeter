.PHONY: build demo demo-proof demo-gateway demo-record start start-saas start-benchmark stop stop-saas clean generate benchmark correctness-bench validate-spec load-test load-test-quick http-load-test http-load-test-single http-load-test-batch test-e2e test-unit test-java test-cold-store apply-cold-store-init

JAR = $(shell ls -t build/libs/fluxmeter-*.jar 2>/dev/null | head -1)

# Build the Flink metering engine embedded in the runtime image.
build:
	./gradlew shadowJar

# One-command demo for the only architecture.
demo: start
	@echo ""
	@echo "==================================="
	@echo " FluxMeter 4.7 — HTTP → Kafka → Flink → Redis"
	@echo "==================================="
	@echo " API:           http://localhost:8000/docs"
	@echo " Intelligence:  http://localhost:8000/docs#/intelligence"
	@echo " Gateway:       http://localhost:8080/v1/chat/completions"
	@echo " Grafana:       http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Flink UI:      http://localhost:8081"
	@echo " Record demo.gif: make demo-record  (requires vhs)"
	@echo ""
	@echo " Gateway example:"
	@echo "   curl localhost:8080/v1/chat/completions \\"
	@echo "     -H 'X-FluxMeter-Customer-Id: cust_1' -H 'Authorization: Bearer \$$OPENAI_API_KEY' \\"
	@echo "     -d '{\"model\":\"gpt-4o-mini\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}'"
	@echo "==================================="

# Re-record demo.gif from demo.tape (brew install vhs)
demo-record:
	vhs demo.tape

# Gateway mock self-check (no live OpenAI)
demo-gateway:
	PYTHONPATH=api python demos/gateway_demo.py

# Deterministic reserve → meter → kill → audit proof (no provider key).
demo-proof: build
	docker compose -f docker-compose.yml -f docker-compose.benchmark.yml -f docker-compose.proof.yml up -d --build
	./scripts/apply-cold-store-init.sh
	@set -e; trap 'docker compose -f docker-compose.yml -f docker-compose.benchmark.yml up -d --no-deps --force-recreate gateway >/dev/null' EXIT; python -u demos/reserve_meter_kill_audit_demo.py

# Build and start Kafka, Flink, Redis, API, Gateway, webhook worker, and Grafana.
start: build
	docker compose up -d --build
	@echo "FluxMeter started. The Flink job is submitted automatically."

start-benchmark: build
	docker compose -f docker-compose.yml -f docker-compose.benchmark.yml up -d --build

# Stop everything
stop:
	docker compose down 2>/dev/null || true
	docker compose -f docker-compose.yml -f docker-compose.saas.yml down 2>/dev/null || true
	docker compose -f docker-compose.yml -f docker-compose.benchmark.yml down 2>/dev/null || true

# Clean build artifacts and containers
clean: stop
	./gradlew clean
	docker compose down -v 2>/dev/null || true
	docker compose -f docker-compose.yml -f docker-compose.saas.yml down -v 2>/dev/null || true
	docker compose -f docker-compose.yml -f docker-compose.benchmark.yml down -v 2>/dev/null || true

# Validate open spec artifacts
validate-spec:
	./scripts/validate-spec.sh

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
		tests/test_rerate_tier.py tests/test_phase2_billing.py tests/test_gateway.py \
		tests/test_ingestion_contract.py tests/test_reservation_expiry.py \
		tests/test_webhook_worker.py tests/test_deep_modules.py tests/test_proof_demo.py \
		tests/test_custody_unit.py -v --timeout=60
	PYTHONPATH=sdk/python pytest sdk/python/tests -q
	cd sdk/js && npm run build
	./gradlew test -q

test-java:
	./gradlew test

# Staged internal engine load test (10K → 1M eps bursts)
load-test:
	./scripts/load-test.sh

# Quick load test (10K–500K only)
load-test-quick:
	QUICK=1 ./scripts/load-test.sh

# Formal HTTP gates: 5m warm-up + 30m measured, typical payload, open-loop.
# Run both targets even if one fails: make -k http-load-test
HTTP_LOAD_SINGLE_TARGET_EPS ?= 10050
HTTP_LOAD_SINGLE_CONCURRENCY ?= 4000
HTTP_LOAD_SINGLE_MAX_P50_MS ?= 50
HTTP_LOAD_SINGLE_MAX_P99_MS ?= 200

http-load-test: http-load-test-single http-load-test-batch

http-load-test-single: build
	java -cp $(JAR) io.fluxmeter.generator.HttpLoadGenerator --url http://127.0.0.1:8000/ingest --target-eps $(HTTP_LOAD_SINGLE_TARGET_EPS) --warmup-seconds 300 --duration-seconds 1800 --concurrency $(HTTP_LOAD_SINGLE_CONCURRENCY) --min-eps 10000 --max-p50-ms $(HTTP_LOAD_SINGLE_MAX_P50_MS) --max-p99-ms $(HTTP_LOAD_SINGLE_MAX_P99_MS)

http-load-test-batch:
	python3 scripts/http-load-test.py --mode batch --batch-size 1000 --target-eps 100000 --processes 1 --concurrency 100 --min-eps 100000 --max-p99-ms 500

# ADR-025 ClickHouse cold store DDL (benchmark overlay must be up)
apply-cold-store-init:
	chmod +x scripts/apply-cold-store-init.sh
	./scripts/apply-cold-store-init.sh

# ADR-025 acceptance A1–A7 (needs kafka + clickhouse from start-benchmark)
test-cold-store:
	chmod +x scripts/test-cold-store.sh scripts/apply-cold-store-init.sh
	./scripts/apply-cold-store-init.sh
	./scripts/test-cold-store.sh

# Run the baseline comparison (Flink vs ClickHouse)
benchmark:
	./baseline/benchmark.sh

# Known-event correctness + Flink checkpoint health
correctness-bench:
	chmod +x scripts/correctness-bench.sh
	./scripts/correctness-bench.sh

# Trusted operator load generator (benchmark overlay exposes Kafka locally).
generate:
	KAFKA_BROKERS=localhost:9094 \
	NUM_CUSTOMERS=10000 \
	NUM_THREADS=8 \
	TARGET_EPS=1000000 \
	java -cp $(JAR) io.fluxmeter.generator.LoadGenerator

start-saas: build
	docker compose -f docker-compose.yml -f docker-compose.saas.yml up -d --build
	@echo "SaaS stack started. API :8000, Control Plane :8001, Grafana :3000"

stop-saas:
	docker compose -f docker-compose.yml -f docker-compose.saas.yml down
