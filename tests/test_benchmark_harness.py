"""Regression tests for the public HTTP benchmark harness and capacity model."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_plans_use_all_requested_processes_without_changing_offered_load():
    runner = load_script("http_load_test", ROOT / "scripts/http-load-test.py")

    plans = runner.worker_plans(target_eps=10_000, concurrency=800, processes=8)

    assert len(plans) == 8
    assert sum(plan.target_eps for plan in plans) == 10_000
    assert sum(plan.concurrency for plan in plans) == 800
    assert {plan.target_eps for plan in plans} == {1_250}
    assert {plan.concurrency for plan in plans} == {100}


def test_worker_results_merge_counts_rates_and_latency_histograms():
    runner = load_script("http_load_test", ROOT / "scripts/http-load-test.py")
    workers = [
        {
            "accepted": 50,
            "failed": 0,
            "completed_requests": 50,
            "elapsed": 10.0,
            "latency_sum_ms": 500.0,
            "latency_buckets": {10: 50},
            "per_second_events": {0: 50},
            "status_counts": {"202": 50},
            "error_samples": [],
        },
        {
            "accepted": 50,
            "failed": 0,
            "completed_requests": 50,
            "elapsed": 10.0,
            "latency_sum_ms": 680.0,
            "latency_buckets": {10: 48, 100: 2},
            "per_second_events": {0: 50},
            "status_counts": {"202": 50},
            "error_samples": [],
        },
    ]

    result = runner.aggregate_worker_results(workers, duration=10)

    assert result["accepted"] == 100
    assert result["failed"] == 0
    assert result["events_per_second"] == 10.0
    assert result["latency_ms"] == {
        "mean": 11.8,
        "p50": 10.0,
        "p95": 10.0,
        "p99": 100.0,
    }
    assert result["status_counts"] == {"202": 100}


def test_capacity_model_separates_benchmark_window_from_production_retention():
    capacity = load_script(
        "benchmark_capacity", ROOT / "scripts/benchmark_capacity.py"
    )

    result = capacity.project_capacity(
        before_bytes=1_000,
        after_bytes=1_145_000,
        accepted=1_000,
        target_eps=10_000,
        benchmark_ttl_seconds=300,
        production_ttl_seconds=30 * 24 * 60 * 60,
        redis_maxmemory_bytes=6 * 1024**3,
    )

    assert result["estimated_bytes_per_event"] == 1_144.0
    assert result["benchmark_steady_state_bytes"] == 3_432_000_000
    assert result["production_retention_bytes"] == 29_652_480_000_000
    assert result["benchmark_safe_at_80_percent"] is True


def test_benchmark_overlay_bounds_identity_retention_and_sizes_redis_for_it():
    compose = yaml.safe_load((ROOT / "docker-compose.benchmark.yml").read_text())
    redis = compose["services"]["redis"]
    api = compose["services"]["api"]

    assert "--maxmemory 6gb" in redis["command"]
    assert redis["mem_limit"] == "8g"
    assert (
        api["environment"]["EVENT_ID_TTL_SECONDS"]
        == "${BENCHMARK_EVENT_ID_TTL_SECONDS:-300}"
    )
    assert api["environment"]["EVENT_IDENTITY_SHARDS"] == "${BENCHMARK_EVENT_IDENTITY_SHARDS:-8}"
    assert api["environment"]["EVENT_IDENTITY_CLEANUP_LIMIT"] == "${BENCHMARK_EVENT_IDENTITY_CLEANUP_LIMIT:-64}"


def test_benchmark_api_avoids_per_request_logging_and_extra_kafka_linger():
    compose = yaml.safe_load((ROOT / "docker-compose.benchmark.yml").read_text())
    api = compose["services"]["api"]

    assert "--no-access-log" in api["command"]
    assert "${BENCHMARK_API_WORKERS:-12}" in api["command"]
    assert api["environment"]["KAFKA_PRODUCER_LINGER_MS"] == "0"
    assert api["environment"]["INGEST_MICROBATCH_SIZE"] == "${INGEST_MICROBATCH_SIZE:-64}"
    assert api["environment"]["INGEST_MICROBATCH_WAIT_SECONDS"] == "${INGEST_MICROBATCH_WAIT_SECONDS:-0.001}"


def test_benchmark_projection_identity_matches_flink_crash_safety_window():
    compose = yaml.safe_load((ROOT / "docker-compose.benchmark.yml").read_text())

    expected = "${BENCHMARK_EVENT_PROJECTION_IDEMPOTENCY_TTL_SECONDS:-600}"
    assert compose["services"]["job-submitter"]["environment"][
        "EVENT_PROJECTION_IDEMPOTENCY_TTL_SECONDS"
    ] == expected


def test_benchmark_uses_all_configured_flink_slots():
    base = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    benchmark = yaml.safe_load((ROOT / "docker-compose.benchmark.yml").read_text())

    assert "$${FLINK_PARALLELISM:-2}" in base["services"]["job-submitter"]["command"][0]
    assert benchmark["services"]["job-submitter"]["environment"][
        "FLINK_PARALLELISM"
    ] == "${BENCHMARK_FLINK_PARALLELISM:-12}"


def test_base_stack_waits_for_redis_loading_before_starting_consumers():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose["services"]

    assert "PONG" in " ".join(services["redis"]["healthcheck"]["test"])
    assert services["job-submitter"]["depends_on"]["redis"]["condition"] == "service_healthy"
    assert services["api"]["depends_on"]["redis"]["condition"] == "service_healthy"
    assert services["api"]["depends_on"]["job-submitter"]["condition"] == "service_completed_successfully"


def test_public_ingest_routes_precede_intelligence_routes():
    import sys

    sys.path.insert(0, str(ROOT / "api"))
    import main

    paths = [getattr(route, "path", "") for route in main.app.routes]
    intelligence_index = next(
        index
        for index, route in enumerate(main.app.routes)
        if getattr(route, "original_router", None) is main.intelligence_router
    )
    assert paths.index("/ingest") < intelligence_index
    assert paths.index("/ingest/batch") < intelligence_index


def test_single_gate_offers_headroom_above_minimum_and_has_enough_slots():
    makefile = (ROOT / "Makefile").read_text()

    assert "HTTP_LOAD_SINGLE_TARGET_EPS ?= 10050" in makefile
    assert "HTTP_LOAD_SINGLE_CONCURRENCY ?= 4000" in makefile
    assert "HTTP_LOAD_SINGLE_MAX_P50_MS ?= 50" in makefile
    assert "HTTP_LOAD_SINGLE_MAX_P99_MS ?= 200" in makefile
    assert "--target-eps $(HTTP_LOAD_SINGLE_TARGET_EPS)" in makefile
    assert "--max-p50-ms $(HTTP_LOAD_SINGLE_MAX_P50_MS)" in makefile
    assert "--max-p99-ms $(HTTP_LOAD_SINGLE_MAX_P99_MS)" in makefile
