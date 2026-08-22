#!/usr/bin/env python3
"""Project Redis custody capacity without conflating test and production TTLs."""

from __future__ import annotations

import argparse
import json


def project_capacity(
    *,
    before_bytes: int,
    after_bytes: int,
    accepted: int,
    target_eps: float,
    benchmark_ttl_seconds: int,
    production_ttl_seconds: int,
    redis_maxmemory_bytes: int,
) -> dict:
    accepted = max(1, int(accepted))
    delta = max(0, int(after_bytes) - int(before_bytes))
    bytes_per_event = delta / accepted
    benchmark_bytes = int(bytes_per_event * target_eps * benchmark_ttl_seconds)
    production_bytes = int(bytes_per_event * target_eps * production_ttl_seconds)
    safe_limit = int(redis_maxmemory_bytes * 0.8)
    return {
        "accepted": accepted,
        "redis_delta_bytes": delta,
        "estimated_bytes_per_event": round(bytes_per_event, 1),
        "benchmark_ttl_seconds": benchmark_ttl_seconds,
        "benchmark_steady_state_bytes": benchmark_bytes,
        "production_ttl_seconds": production_ttl_seconds,
        "production_retention_bytes": production_bytes,
        "redis_maxmemory_bytes": redis_maxmemory_bytes,
        "benchmark_safe_at_80_percent": benchmark_bytes <= safe_limit,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", required=True)
    parser.add_argument("--before-bytes", required=True, type=int)
    parser.add_argument("--after-bytes", required=True, type=int)
    parser.add_argument("--target-eps", required=True, type=float)
    parser.add_argument("--benchmark-ttl-seconds", required=True, type=int)
    parser.add_argument(
        "--production-ttl-seconds", type=int, default=30 * 24 * 60 * 60
    )
    parser.add_argument("--redis-maxmemory-bytes", required=True, type=int)
    args = parser.parse_args()
    accepted = int(json.load(open(args.result))["accepted"])
    result = project_capacity(
        before_bytes=args.before_bytes,
        after_bytes=args.after_bytes,
        accepted=accepted,
        target_eps=args.target_eps,
        benchmark_ttl_seconds=args.benchmark_ttl_seconds,
        production_ttl_seconds=args.production_ttl_seconds,
        redis_maxmemory_bytes=args.redis_maxmemory_bytes,
    )
    gib = 1024**3
    tib = 1024**4
    print(json.dumps(result, sort_keys=True))
    print(f"accepted={result['accepted']:,}")
    print(f"redis_delta={result['redis_delta_bytes'] / 1024**2:.1f} MiB")
    print(f"estimated_bytes_per_event={result['estimated_bytes_per_event']:.1f}")
    print(
        "benchmark_steady_state="
        f"{result['benchmark_steady_state_bytes'] / gib:.1f} GiB "
        f"at {result['benchmark_ttl_seconds']}s TTL"
    )
    print(
        "production_30d_retention="
        f"{result['production_retention_bytes'] / tib:.1f} TiB"
    )
    print(
        "BENCHMARK_SAFE_AT_80_PERCENT="
        + ("yes" if result["benchmark_safe_at_80_percent"] else "no")
    )
    return 0 if result["benchmark_safe_at_80_percent"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
