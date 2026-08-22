#!/usr/bin/env python3
"""Reproducible HTTP custody benchmark with open- and closed-loop traffic."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, namedtuple
from concurrent.futures import ProcessPoolExecutor
import json
import math
import os
import platform
import random
import time
import uuid
from pathlib import Path

import httpx


WorkerPlan = namedtuple("WorkerPlan", "target_eps concurrency")


def worker_plans(
    *, target_eps: float, concurrency: int, processes: int
) -> list[WorkerPlan]:
    """Split one offered-load contract across independent generator processes."""
    if processes < 1:
        raise ValueError("processes must be at least 1")
    if concurrency < processes:
        raise ValueError("concurrency must be at least processes")
    target_base = target_eps / processes
    concurrency_base, concurrency_remainder = divmod(concurrency, processes)
    plans = []
    assigned_target = 0.0
    for index in range(processes):
        worker_target = (
            target_eps - assigned_target
            if index == processes - 1
            else target_base
        )
        assigned_target += worker_target
        plans.append(
            WorkerPlan(
                target_eps=worker_target,
                concurrency=concurrency_base
                + (1 if index < concurrency_remainder else 0),
            )
        )
    return plans


def _percentile_from_buckets(
    buckets: dict[int, int], fraction: float
) -> float | None:
    count = sum(buckets.values())
    if not count:
        return None
    rank = max(1, math.ceil(count * fraction))
    seen = 0
    for latency_ms, bucket_count in sorted(buckets.items()):
        seen += bucket_count
        if seen >= rank:
            return float(latency_ms)
    return float(max(buckets))


def aggregate_worker_results(workers: list[dict], *, duration: float) -> dict:
    """Merge process-local counters without retaining every request latency."""
    accepted = sum(int(worker["accepted"]) for worker in workers)
    failed = sum(int(worker["failed"]) for worker in workers)
    completed = sum(int(worker["completed_requests"]) for worker in workers)
    elapsed = max(0.001, float(duration))
    latency_count = sum(sum(worker["latency_buckets"].values()) for worker in workers)
    latency_sum = sum(float(worker["latency_sum_ms"]) for worker in workers)
    latency_buckets: Counter[int] = Counter()
    per_second_events: Counter[int] = Counter()
    status_counts: Counter[str] = Counter()
    error_samples: list[dict] = []
    for worker in workers:
        latency_buckets.update(worker["latency_buckets"])
        per_second_events.update(worker["per_second_events"])
        status_counts.update(worker["status_counts"])
        error_samples.extend(worker["error_samples"])
    windows = []
    for start in range(0, max(per_second_events.keys(), default=-1) + 1, 60):
        seconds = min(60, max(0, int(elapsed) - start))
        if seconds:
            windows.append(
                sum(per_second_events.get(i, 0) for i in range(start, start + seconds))
                / seconds
            )
    return {
        "duration_seconds": round(elapsed, 3),
        "completed_requests": completed,
        "accepted": accepted,
        "failed": failed,
        "events_per_second": round(accepted / elapsed, 2),
        "requests_per_second": round(completed / elapsed, 2),
        "minimum_one_minute_eps": round(min(windows), 2) if windows else None,
        "latency_ms": {
            "mean": round(latency_sum / latency_count, 2) if latency_count else None,
            "p50": _percentile_from_buckets(latency_buckets, 0.50),
            "p95": _percentile_from_buckets(latency_buckets, 0.95),
            "p99": _percentile_from_buckets(latency_buckets, 0.99),
        },
        "status_counts": dict(status_counts),
        "error_samples": error_samples[:5],
    }


def event(profile: str, customer_id: str) -> dict:
    payload = {
        "eventId": str(uuid.uuid4()), "customerId": customer_id,
        "provider": "openai", "modelId": "gpt-4o-mini",
        "inputTokens": 640, "outputTokens": 180,
        "timestamp": int(time.time() * 1000),
    }
    if profile in ("typical", "heavy"):
        payload.update({
            "requestId": f"req-{uuid.uuid4()}",
            "sessionId": f"session-{random.randrange(10_000)}",
            "environment": "production", "latencyMs": 820,
            "metadata": {"feature": "chat", "room_id": f"room-{random.randrange(1000)}"},
        })
    if profile == "heavy":
        payload["metadata"] = {"feature": "x" * 1024, "room_id": "y" * 1024}
    return payload


def customer_for(sequence: int, tenant_mode: str) -> str:
    if tenant_mode == "noisy":
        return "noisy-tenant" if sequence % 10 else f"normal-{sequence % 10}"
    if tenant_mode == "zipf":
        rank = min(99, int(random.paretovariate(1.3)) - 1)
        return f"zipf-{rank}"
    return f"load-{sequence % 1000}"


async def run_worker(args: argparse.Namespace) -> dict:
    headers = {"X-API-Key": args.api_key}
    accepted = failed = completed_requests = sequence = 0
    latency_buckets: Counter[int] = Counter()
    latency_sum_ms = 0.0
    status_counts: dict[str, int] = {}
    error_samples: list[dict] = []
    per_second_events: dict[int, int] = {}
    run_started = time.monotonic()
    measure_started = run_started + args.warmup
    stop_at = measure_started + args.duration
    events_per_request = 1 if args.mode == "single" else args.batch_size
    target_rps = args.target_eps / events_per_request if args.target_eps else 0
    timeout = httpx.Timeout(args.timeout)
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    lock = asyncio.Lock()

    async with httpx.AsyncClient(
        base_url=args.api_url,
        headers=headers,
        timeout=timeout,
        limits=limits,
        trust_env=False,
    ) as client:
        async def send_one() -> None:
            nonlocal accepted, failed, completed_requests, sequence, latency_sum_ms
            async with lock:
                start_sequence = sequence
                sequence += events_per_request
            payloads = [event(args.profile, customer_for(start_sequence + offset, args.tenant_mode)) for offset in range(events_per_request)]
            payload = payloads[0] if args.mode == "single" else payloads
            path = "/ingest" if args.mode == "single" else "/ingest/batch"
            request_started = time.monotonic()
            measured = request_started >= measure_started
            status_key = ""
            successful = 0
            try:
                response = await client.post(path, json=payload)
                status_key = str(response.status_code)
                if response.status_code in (200, 202, 207):
                    if args.mode == "single":
                        successful = 1
                    else:
                        successful = sum(1 for item in response.json().get("results", []) if item.get("status") in ("accepted", "quarantined"))
                elif len(error_samples) < 5:
                    try:
                        detail = response.json()
                        if isinstance(detail, dict) and isinstance(detail.get("results"), list):
                            rows = detail["results"]
                            counts: dict[str, int] = {}
                            for row in rows:
                                status = str(row.get("status", "unknown"))
                                counts[status] = counts.get(status, 0) + 1
                            detail = {
                                "status": detail.get("status"),
                                "result_counts": counts,
                                "sample": rows[:5],
                            }
                    except ValueError:
                        detail = response.text[:500]
                    error_samples.append({"status": response.status_code, "detail": detail})
            except httpx.HTTPError as exc:
                status_key = type(exc).__name__
            latency = (time.monotonic() - request_started) * 1000
            if not measured:
                return
            async with lock:
                latency_buckets[max(0, int(round(latency)))] += 1
                latency_sum_ms += latency
                completed_requests += 1
                accepted += successful
                failed += events_per_request - successful
                status_counts[status_key] = status_counts.get(status_key, 0) + 1
                second = int(request_started - measure_started)
                per_second_events[second] = per_second_events.get(second, 0) + successful

        async def worker(worker_index: int) -> None:
            interval = args.concurrency / target_rps if target_rps else 0
            next_send = run_started + (worker_index * interval / max(args.concurrency, 1))
            while time.monotonic() < stop_at:
                if args.traffic_model == "open":
                    delay = next_send - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    next_send += interval
                await send_one()

        await asyncio.gather(*(worker(index) for index in range(args.concurrency)))

    elapsed = max(0.001, time.monotonic() - measure_started)
    return {
        "elapsed": elapsed,
        "completed_requests": completed_requests,
        "accepted": accepted,
        "failed": failed,
        "latency_sum_ms": latency_sum_ms,
        "latency_buckets": dict(latency_buckets),
        "per_second_events": per_second_events,
        "status_counts": status_counts,
        "error_samples": error_samples,
    }


def _run_worker_process(args_dict: dict) -> dict:
    return asyncio.run(run_worker(argparse.Namespace(**args_dict)))


def run(args: argparse.Namespace) -> dict:
    plans = worker_plans(
        target_eps=args.target_eps,
        concurrency=args.concurrency,
        processes=args.processes,
    )
    worker_args = []
    for plan in plans:
        values = vars(args).copy()
        values["target_eps"] = plan.target_eps
        values["concurrency"] = plan.concurrency
        worker_args.append(values)
    if args.processes == 1:
        workers = [_run_worker_process(worker_args[0])]
    else:
        with ProcessPoolExecutor(max_workers=args.processes) as executor:
            workers = list(executor.map(_run_worker_process, worker_args))
    measured_duration = max(float(worker["elapsed"]) for worker in workers)
    result = aggregate_worker_results(workers, duration=measured_duration)
    result.update(
        {
            "mode": args.mode,
            "traffic_model": args.traffic_model,
            "profile": args.profile,
            "tenant_mode": args.tenant_mode,
            "batch_size": 1 if args.mode == "single" else args.batch_size,
            "concurrency": args.concurrency,
            "processes": args.processes,
            "warmup_seconds": args.warmup,
            "target_events_per_second": args.target_eps or None,
            "host": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "logical_cpu_count": os.cpu_count(),
                "python": platform.python_version(),
            },
        }
    )
    return result


def passes(result: dict, args: argparse.Namespace) -> bool:
    latency = result["latency_ms"]
    if result["failed"] or result["events_per_second"] < args.min_eps:
        return False
    if args.max_p50_ms and (latency["p50"] is None or latency["p50"] > args.max_p50_ms):
        return False
    if args.max_p99_ms and (latency["p99"] is None or latency["p99"] > args.max_p99_ms):
        return False
    window = result["minimum_one_minute_eps"]
    if args.target_eps and window is not None and window < args.target_eps * 0.95:
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default=os.getenv("FLUXMETER_API_URL", "http://localhost:8000"))
    parser.add_argument("--api-key", default=os.getenv("FLUXMETER_API_KEY", ""))
    parser.add_argument("--mode", choices=("single", "batch"), required=True)
    parser.add_argument("--traffic-model", choices=("open", "closed"), default="open")
    parser.add_argument("--profile", choices=("minimal", "typical", "heavy"), default="typical")
    parser.add_argument("--tenant-mode", choices=("normal", "noisy", "zipf"), default="normal")
    parser.add_argument("--warmup", type=int, default=300)
    parser.add_argument("--duration", type=int, default=1800)
    parser.add_argument("--target-eps", type=float, default=0)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument(
        "--processes", type=int, default=int(os.getenv("HTTP_LOAD_PROCESSES", "1"))
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output-dir", default="load-test-results")
    parser.add_argument("--min-eps", type=float, default=0)
    parser.add_argument("--max-p50-ms", type=float, default=0)
    parser.add_argument("--max-p99-ms", type=float, default=0)
    args = parser.parse_args()
    if args.traffic_model == "open" and args.target_eps <= 0:
        parser.error("--target-eps is required for open-loop traffic")
    if args.processes < 1:
        parser.error("--processes must be at least 1")
    if args.concurrency < args.processes:
        parser.error("--concurrency must be at least --processes")
    result = run(args)
    result["passed"] = passes(result, args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"http-{args.mode}-{args.profile}-{timestamp}.json"
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    result["output"] = str(output_path)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
