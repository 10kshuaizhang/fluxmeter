"""Periodic balance reconciliation: balance == initial + topups - total_deducted."""

from __future__ import annotations

import json
import logging
import os
import time

import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reconcile_balances")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
DRIFT_THRESHOLD = float(os.getenv("RECONCILE_DRIFT_THRESHOLD_USD", "0.01"))
INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "900"))


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )


def reconcile_all(r: redis.Redis) -> dict:
    drifts = []
    scanned = 0
    for key in r.scan_iter(match="budget:*:balance_usd", count=500):
        parts = key.split(":")
        if len(parts) < 3:
            continue
        customer_id = parts[1]
        budget_key = f"budget:{customer_id}"
        balance = float(r.get(key) or 0)
        initial = float(r.get(f"{budget_key}:initial_balance_usd") or 0)
        topups = float(r.get(f"{budget_key}:total_topup_usd") or 0)
        deducted = float(r.get(f"{budget_key}:total_deducted_usd") or 0)
        debt = float(r.get(f"{budget_key}:debt_usd") or 0)
        expected = initial + topups - deducted
        drift = balance - expected
        scanned += 1
        if abs(drift) > DRIFT_THRESHOLD:
            drifts.append({
                "customer_id": customer_id,
                "balance_usd": balance,
                "expected_usd": expected,
                "drift_usd": drift,
                "debt_usd": debt,
            })
            logger.warning(
                "Drift %s: balance=%.4f expected=%.4f drift=%.4f",
                customer_id, balance, expected, drift,
            )

    result = {
        "timestamp": int(time.time() * 1000),
        "customers_scanned": scanned,
        "drift_count": len(drifts),
        "drifts": drifts[:100],
    }
    r.set("reconciliation:last", json.dumps(result))
    if drifts:
        r.incr("metrics:reconciliation_drift")
    return result


def main() -> None:
    r = get_redis()
    logger.info("Reconciliation job started (interval=%ds)", INTERVAL_SEC)
    while True:
        try:
            result = reconcile_all(r)
            logger.info(
                "Reconciled %d customers, %d drifts",
                result["customers_scanned"],
                result["drift_count"],
            )
        except Exception as e:
            logger.exception("Reconciliation failed: %s", e)
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
