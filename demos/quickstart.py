"""Prove HTTP custody → Flink → customer usage using only the standard library."""

import argparse
import json
import os
import sys
import time
import uuid
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


def request(base_url, path, *, api_key="", payload=None, timeout=15):
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(base_url.rstrip("/") + path, data=data, headers=headers)
    # macOS system proxies may route localhost through a remote proxy. Only
    # bypass for loopback; explicitly configured remote endpoints keep proxies.
    local = urlsplit(base_url).hostname in ("localhost", "127.0.0.1", "::1")
    opener = build_opener(ProxyHandler({})) if local else build_opener()
    try:
        with opener.open(req, timeout=timeout) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(body)
        except ValueError:
            return exc.code, {"detail": body[:500] or exc.reason}


def poll(label, operation, is_done, *, timeout, interval=1):
    deadline = time.monotonic() + timeout
    last = None
    while True:
        try:
            last = operation()
            if is_done(*last):
                return last[1]
        except (URLError, TimeoutError, ConnectionError) as exc:
            last = str(exc)
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{label} timed out after {timeout}s; last response: {last}")
        time.sleep(interval)


def require_status(status, body, *, success, pending=()):
    if status in success:
        return True
    if status in pending:
        return False
    raise RuntimeError(f"Unexpected HTTP {status}: {body}")


def run(base_url, *, api_key="", timeout=180, expected_cost="0.00042"):
    customer_id = "demo-" + uuid.uuid4().hex
    event_id = "evt-" + uuid.uuid4().hex
    call = lambda path, **kw: request(base_url, path, api_key=api_key, **kw)
    print("Waiting for /ready (Redis, Kafka and a consumed Flink probe)...", flush=True)
    poll("Readiness", lambda: call("/ready"),
         lambda s, b: require_status(s, b, success=(200,), pending=(503,)), timeout=timeout)
    # Omit timestamp so the server assigns current time. Retries reuse this ID
    # and identical payload even if a previous ACK/finalization was uncertain.
    event = {"eventId": event_id, "customerId": customer_id,
             "modelId": "gpt-4o-mini", "inputTokens": 1200, "outputTokens": 400}
    receipt = poll("Custody", lambda: call("/ingest", payload=event),
                   lambda s, b: require_status(s, b, success=(202,), pending=(429, 503)),
                   timeout=timeout)
    if receipt.get("status") != "accepted" or receipt.get("eventId") != event_id:
        raise RuntimeError(f"Expected accepted custody (not quarantine): {receipt}")
    print(f"202 custody accepted: {event_id}; waiting for customer usage...", flush=True)
    expected = {"customer_id": customer_id, "event_count": 1,
                "input_tokens": 1200, "output_tokens": 400, "total_tokens": 1600}

    def projected(status, body):
        if not require_status(status, body, success=(200,), pending=(404, 503)):
            return False
        return (all(body.get(k) == v for k, v in expected.items())
                and Decimal(str(body.get("cost_usd", -1))) == Decimal(expected_cost))

    usage = poll("Customer usage", lambda: call(f"/usage/customer/{customer_id}"),
                 projected, timeout=timeout)
    print(json.dumps(usage, indent=2))
    print("PASS: 1 event, 1600 tokens, $" + expected_cost + "; no provider key required.")
    return usage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv("FLUXMETER_API_URL", "http://localhost:8000"))
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--expected-cost", default="0.00042",
                        help="Expected USD for this fixture; default catalog: 0.00042")
    args = parser.parse_args()
    try:
        run(args.url, api_key=os.getenv("FLUXMETER_API_KEY", ""),
            timeout=args.timeout, expected_cost=args.expected_cost)
    except (RuntimeError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
