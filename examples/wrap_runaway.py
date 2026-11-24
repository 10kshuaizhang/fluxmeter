#!/usr/bin/env python3
"""Runaway agent loop through FluxMeter wrap(). Local mock upstream, no provider key.

Sets a $0.30 prepaid budget, then calls a local OpenAI-compatible mock that
reports gpt-4o 4000 input + 2000 output tokens (catalog $0.03 per call).
wrap() reserves before each call so later loops see the held balance.

This is a reproduction helper, not a zero-overspend guarantee. Concurrent
requests, stale fallback data, and estimated tokens can still admit more
than the remaining balance.

Usage, from the repository root against a running stack:

    make demo
    pip install "fluxmeter>=2.0.2" openai
    python examples/wrap_runaway.py

Optional interval in seconds between calls (default 0 = tight loop):

    python examples/wrap_runaway.py 1

Requires Python 3.9+, the PyPI package fluxmeter>=2.0.2, and openai.
Everything else is the standard library.
"""

from __future__ import annotations

import json
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from openai import OpenAI

from fluxmeter import BudgetExceededError, FluxMeter, wrap

API = "http://localhost:8000"
MOCK_HOST = "127.0.0.1"
MOCK_PORT = 9911
BUDGET_USD = 0.30
ESTIMATED_COST_USD = 0.03
MODEL = "gpt-4o"
INPUT_TOKENS = 4000
OUTPUT_TOKENS = 2000


class Mock(BaseHTTPRequestHandler):
    """Every completion reports 4000 in + 2000 out on gpt-4o = $0.03."""

    def log_message(self, *_args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            self.rfile.read(length)
        body = json.dumps(
            {
                "id": "cmpl-" + uuid.uuid4().hex,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": MODEL,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "loop"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": INPUT_TOKENS,
                    "completion_tokens": OUTPUT_TOKENS,
                    "total_tokens": INPUT_TOKENS + OUTPUT_TOKENS,
                },
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def http_json(url, method="GET", payload=None, timeout=10):
    headers = {"Content-Type": "application/json"}
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(url, data=data, headers=headers, method=method)
    local = urlsplit(url).hostname in ("localhost", "127.0.0.1", "::1")
    opener = build_opener(ProxyHandler({})) if local else build_opener()
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body) if body else {}
        except ValueError:
            parsed = {"detail": body[:500]}
        raise SystemExit(f"HTTP {exc.code} {url}: {parsed}") from exc
    except (URLError, TimeoutError, ConnectionError, OSError) as exc:
        raise SystemExit(
            f"Cannot reach {url}: {exc}\nStart the stack first: make demo"
        ) from exc


def main() -> int:
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0
    customer_id = "runaway-" + uuid.uuid4().hex[:8]

    server = ThreadingHTTPServer((MOCK_HOST, MOCK_PORT), Mock)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    http_json(f"{API}/budget/{customer_id}", method="POST", payload={"balance_usd": BUDGET_USD})

    client = wrap(
        OpenAI(base_url=f"http://{MOCK_HOST}:{MOCK_PORT}/v1", api_key="sk-mock"),
        FluxMeter(api_url=API),
        customer_id=customer_id,
        fail_open=False,
        estimated_cost_usd=ESTIMATED_COST_USD,
    )

    calls, t0 = 0, time.time()
    while time.time() - t0 < 60:
        try:
            client.chat.completions.create(
                model=MODEL, messages=[{"role": "user", "content": "loop"}]
            )
            calls += 1
        except BudgetExceededError as exc:
            print(
                f"blocked after {calls} calls at {time.time() - t0:.1f}s: "
                f"{exc.gate.get('reason')}"
            )
            break
        time.sleep(interval)
    else:
        print(f"loop ended after {calls} calls without a budget block")

    time.sleep(25)  # let Flink settle the window
    print("budget:", http_json(f"{API}/budget/{customer_id}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
