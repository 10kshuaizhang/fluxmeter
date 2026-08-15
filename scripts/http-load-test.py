#!/usr/bin/env python3
"""Benchmark the supported HTTP custody boundary in single or batch mode."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid

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
    started_at = time.monotonic()
    stop_at = started_at + args.duration

    async with httpx.AsyncClient(base_url=args.api_url, headers=headers, timeout=15) as client:
        async def worker() -> None:
            nonlocal accepted, failed
            while time.monotonic() < stop_at:
                payload = event() if args.mode == "single" else [event() for _ in range(args.batch_size)]
                path = "/ingest" if args.mode == "single" else "/ingest/batch"
                async with semaphore:
                    response = await client.post(path, json=payload)
                if response.status_code in (200, 202, 207):
                    accepted += 1 if args.mode == "single" else sum(
                        1 for item in response.json().get("results", []) if item.get("status") in ("accepted", "quarantined")
                    )
                else:
                    failed += 1 if args.mode == "single" else args.batch_size

        await asyncio.gather(*(worker() for _ in range(args.concurrency)))

    elapsed = time.monotonic() - started_at
    eps = accepted / elapsed
    return {"mode": args.mode, "duration_seconds": elapsed, "accepted": accepted, "failed": failed, "events_per_second": round(eps, 2)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default=os.getenv("FLUXMETER_API_URL", "http://localhost:8000"))
    parser.add_argument("--api-key", default=os.getenv("FLUXMETER_API_KEY", ""))
    parser.add_argument("--mode", choices=("single", "batch"), required=True)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--min-eps", type=float, default=0)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    print(json.dumps(result, sort_keys=True))
    return 1 if result["events_per_second"] < args.min_eps or result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
