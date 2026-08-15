#!/usr/bin/env python3
"""Benchmark the supported HTTP custody boundary in single or batch mode."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
import uuid
from pathlib import Path

import httpx


def event() -> dict:
    return {
        "eventId": str(uuid.uuid4()),
        "customerId": "load-test",
        "provider": "openai",
        "modelId": "gpt-4o-mini",
        "inputTokens": 10,
        "outputTokens": 5,
        "timestamp": int(time.time() * 1000),
    }


async def run(args: argparse.Namespace) -> dict:
    headers = {"X-API-Key": args.api_key}
    semaphore = asyncio.Semaphore(args.concurrency)
    accepted = 0
    failed = 0
    completed_requests = 0
    request_latencies_ms: list[float] = []
    status_counts: dict[str, int] = {}
    started_at = time.monotonic()
    stop_at = started_at + args.duration

    timeout = httpx.Timeout(args.timeout)
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=args.api_url, headers=headers, timeout=timeout, limits=limits) as client:
        async def worker() -> None:
            nonlocal accepted, failed, completed_requests
            while time.monotonic() < stop_at:
                payload = event() if args.mode == "single" else [event() for _ in range(args.batch_size)]
                path = "/ingest" if args.mode == "single" else "/ingest/batch"
                request_started_at = time.monotonic()
                try:
                    async with semaphore:
                        response = await client.post(path, json=payload)
                except httpx.HTTPError as exc:
                    failed += 1 if args.mode == "single" else args.batch_size
                    status_counts[type(exc).__name__] = status_counts.get(type(exc).__name__, 0) + 1
                    continue
                request_latencies_ms.append((time.monotonic() - request_started_at) * 1000)
                completed_requests += 1
                status_key = str(response.status_code)
                status_counts[status_key] = status_counts.get(status_key, 0) + 1
                if response.status_code in (200, 202, 207):
                    if args.mode == "single":
                        accepted += 1
                    else:
                        successful = sum(
                            1
                            for item in response.json().get("results", [])
                            if item.get("status") in ("accepted", "quarantined")
                        )
                        accepted += successful
                        failed += args.batch_size - successful
                else:
                    failed += 1 if args.mode == "single" else args.batch_size

        await asyncio.gather(*(worker() for _ in range(args.concurrency)))

    elapsed = time.monotonic() - started_at
    eps = accepted / elapsed
    sorted_latencies = sorted(request_latencies_ms)

    def percentile(fraction: float) -> float | None:
        if not sorted_latencies:
            return None
        index = min(len(sorted_latencies) - 1, int((len(sorted_latencies) - 1) * fraction))
        return round(sorted_latencies[index], 2)

    return {
        "mode": args.mode,
        "batch_size": args.batch_size if args.mode == "batch" else 1,
        "concurrency": args.concurrency,
        "duration_seconds": round(elapsed, 3),
        "completed_requests": completed_requests,
        "accepted": accepted,
        "failed": failed,
        "events_per_second": round(eps, 2),
        "requests_per_second": round(completed_requests / elapsed, 2),
        "latency_ms": {
            "mean": round(statistics.fmean(request_latencies_ms), 2) if request_latencies_ms else None,
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
        },
        "status_counts": status_counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default=os.getenv("FLUXMETER_API_URL", "http://localhost:8000"))
    parser.add_argument("--api-key", default=os.getenv("FLUXMETER_API_KEY", ""))
    parser.add_argument("--mode", choices=("single", "batch"), required=True)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output-dir", default="load-test-results")
    parser.add_argument("--min-eps", type=float, default=0)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"http-{args.mode}-{timestamp}.json"
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    result["output"] = str(output_path)
    print(json.dumps(result, sort_keys=True))
    return 1 if result["events_per_second"] < args.min_eps or result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
