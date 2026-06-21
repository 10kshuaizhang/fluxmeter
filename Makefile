.PHONY: build demo demo-lite start start-lite stop clean generate submit-job benchmark validate-spec

JAR = $(shell ls -t build/libs/fluxmeter-*.jar 2>/dev/null | head -1)

# Build the fat JAR
build:
	./gradlew shadowJar

# Lite demo: Redis + API + Grafana (no Flink/Kafka)
demo-lite: start-lite
	@echo ""
	@echo "==================================="
	@echo " FluxMeter Lite Demo Running!"
	@echo "==================================="
	@echo " API:     http://localhost:8000/docs"
	@echo " Grafana: http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Try: curl -X POST localhost:8000/ingest -H 'Content-Type: application/json' \\"
	@echo "   -d '{\"customerId\":\"cust_1\",\"modelId\":\"gpt-4o\",\"inputTokens\":100,\"outputTokens\":50}'"
	@echo "==================================="

# One-command demo: build, start infra, submit job, run generator
demo: build start
	@echo "Waiting for Flink cluster to be ready..."
	@sleep 10
	@$(MAKE) submit-job
	@echo ""
	@echo "==================================="
	@echo " FluxMeter Demo Running!"
	@echo "==================================="
	@echo " API:       http://localhost:8000/docs"
	@echo " Flink UI:  http://localhost:8081"
	@echo " Grafana:   http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Starting load generator (Ctrl+C to stop)..."
	@echo "==================================="
	@$(MAKE) generate

# Start lite infrastructure (Redis + API + Grafana)
start-lite:
	docker compose -f docker-compose-lite.yml up -d --build
	@echo "Lite stack started. API aggregates directly to Redis (no Flink)."

# Start all infrastructure
start:
	docker compose up -d --build
	@echo "Infrastructure started. Kafka, Flink, Redis, API, Grafana running."

# Stop everything
stop:
	docker compose down
	docker compose -f docker-compose-lite.yml down

# Clean build artifacts and containers
clean: stop
	./gradlew clean
	docker compose down -v
	docker compose -f docker-compose-lite.yml down -v

# Validate open spec artifacts
validate-spec:
	./scripts/validate-spec.sh

# Submit the Flink job to the cluster
submit-job:
	docker cp $(JAR) fluxmeter-jobmanager:/opt/flink/fluxmeter.jar
	docker exec fluxmeter-jobmanager flink run \
		-d \
		-c io.fluxmeter.job.TokenUsageAggregator \
		/opt/flink/fluxmeter.jar

# Run the baseline comparison (Flink vs ClickHouse)
benchmark:
	./baseline/benchmark.sh

# Run the load generator locally (requires Java 17)
generate:
	KAFKA_BROKERS=localhost:9094 \
	NUM_CUSTOMERS=10000 \
	NUM_THREADS=4 \
	TARGET_EPS=1000000 \
	java -cp $(JAR) io.fluxmeter.generator.LoadGenerator
