#!/usr/bin/env python3
"""Runaway agent A/B: unguarded mock vs FluxMeter Gateway (no provider key).

Run with ``make demo-runaway`` against a warm stack (``make demo`` / ``make start``).
The with-FluxMeter path goes through the Gateway, not Python ``wrap()``.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MODEL = "gpt-4o"
INPUT_TOKENS = 4000
OUTPUT_TOKENS = 2000
BUDGET_USD = 0.30
DEFAULT_MOCK_PORT = 19090
UNGUARDED_SECONDS = 3.0
UNGUARDED_MAX_CALLS = 2500
UNGUARDED_WORKERS = 12
SETTLE_TIMEOUT = 25.0


def catalog_cost_usd(
    model: str = MODEL,
    input_tokens: int = INPUT_TOKENS,
    output_tokens: int = OUTPUT_TOKENS,
) -> float:
    """gpt-4o catalog: 4000 in × $2.50/M + 2000 out × $10/M = $0.03."""
    api_dir = os.path.join(ROOT, "api")
    if api_dir not in sys.path:
        sys.path.insert(0, api_dir)
    from pricing_loader import PricingCatalog  # noqa: WPS433 — demo-local catalog quote

    return PricingCatalog.load_from_file().quote_usd(
        model, input_tokens=input_tokens, output_tokens=output_tokens
    )


def usd(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.2f}"


class MockOpenAIHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 — stdlib callback
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_POST(self) -> None:  # noqa: N802 — stdlib callback
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

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class MockUpstream:
    def __init__(self, port: int) -> None:
        self.port = port
        self._server = ThreadingHTTPServer(("0.0.0.0", port), MockOpenAIHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> MockUpstream:
        self._thread.start()
        self.port = int(self._server.server_address[1])
        return self

    def __exit__(self, *_args: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


class HttpJson:
    """Keep-alive JSON client. Avoids urllib so macOS system proxies cannot intercept."""

    def __init__(self, url: str, timeout: float = 10.0) -> None:
        parts = urlsplit(url)
        if parts.hostname is None:
            raise ValueError(f"invalid url: {url}")
        self._host = parts.hostname
        self._port = parts.port or (443 if parts.scheme == "https" else 80)
        self._timeout = timeout
        self._conn = http.client.HTTPConnection(self._host, self._port, timeout=timeout)

    def close(self) -> None:
        self._conn.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        hdrs = dict(headers or {})
        payload = None
        if body is not None:
            payload = json.dumps(body).encode()
            hdrs.setdefault("Content-Type", "application/json")
            hdrs["Content-Length"] = str(len(payload))
        try:
            self._conn.request(method, path, body=payload, headers=hdrs)
            resp = self._conn.getresponse()
            raw = resp.read()
            return resp.status, raw, {k.lower(): v for k, v in resp.headers.items()}
        except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError, OSError):
            self._conn.close()
            self._conn = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
            self._conn.request(method, path, body=payload, headers=hdrs)
            resp = self._conn.getresponse()
            raw = resp.read()
            return resp.status, raw, {k.lower(): v for k, v in resp.headers.items()}

    def json(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, Any, dict[str, str]]:
        status, raw, hdrs = self.request(method, path, body=body, headers=headers)
        if not raw:
            return status, None, hdrs
        try:
            return status, json.loads(raw.decode()), hdrs
        except ValueError:
            return status, raw.decode("utf-8", errors="replace")[:500], hdrs


def wait_for(
    description: str,
    probe: Callable[[], Any],
    accept: Callable[[Any], bool],
    *,
    timeout: float,
    interval: float = 0.25,
) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = probe()
            if accept(last):
                return last
        except (OSError, http.client.HTTPException, ConnectionError, TimeoutError, ValueError):
            last = None
        time.sleep(interval)
    raise RuntimeError(f"{description} timed out after {timeout:.0f}s; last={last!r}")


def chat_body() -> dict[str, Any]:
    return {
        "model": MODEL,
        "messages": [{"role": "user", "content": "loop"}],
        "max_tokens": OUTPUT_TOKENS,
    }


def parse_budget_error(payload: Any) -> str:
    if not isinstance(payload, dict):
        return "non-json"
    err = payload.get("error") if isinstance(payload.get("error"), dict) else payload
    code = str(err.get("code") or err.get("reason") or "error")
    return code


@dataclass
class LoopResult:
    admitted: int = 0
    elapsed_s: float = 0.0
    stopped: bool = False
    stop_reason: str = "did not stop"
    spend_usd: float = 0.0
    reserved_usd: float | None = None
    first_error_status: int | None = None
    first_error_body: Any = None
    codes: dict[int, int] = field(default_factory=dict)


def run_chat_loop(
    client: HttpJson,
    path: str,
    *,
    headers: dict[str, str],
    cost_per_call: float,
    max_seconds: float,
    max_calls: int,
    stop_on_non_200: bool,
) -> LoopResult:
    result = LoopResult()
    body = chat_body()
    t0 = time.perf_counter()
    while True:
        elapsed = time.perf_counter() - t0
        if elapsed >= max_seconds or result.admitted >= max_calls:
            result.elapsed_s = elapsed
            result.spend_usd = result.admitted * cost_per_call
            result.stop_reason = f"still burning ({max_seconds:.1f}s cap)"
            return result
        status, payload, resp_headers = client.json(
            "POST", path, body=body, headers=headers
        )
        result.codes[status] = result.codes.get(status, 0) + 1
        if status == 200:
            result.admitted += 1
            if result.reserved_usd is None:
                raw_hold = resp_headers.get("x-fluxmeter-reserved-usd")
                if raw_hold:
                    try:
                        result.reserved_usd = float(raw_hold)
                    except ValueError:
                        pass
            continue
        result.elapsed_s = time.perf_counter() - t0
        result.spend_usd = result.admitted * cost_per_call
        result.first_error_status = status
        result.first_error_body = payload
        result.stop_reason = f"{status} {parse_budget_error(payload)}"
        result.stopped = stop_on_non_200
        return result


def run_unguarded_parallel(
    url: str,
    *,
    headers: dict[str, str],
    cost_per_call: float,
    max_seconds: float,
    max_calls: int,
    workers: int,
) -> LoopResult:
    """Burst the mock so the unguarded side clearly overshoots the budget."""
    stop = threading.Event()
    lock = threading.Lock()
    result = LoopResult()

    def worker() -> None:
        client = HttpJson(url)
        try:
            while not stop.is_set():
                status, _payload, _hdrs = client.json(
                    "POST", "/v1/chat/completions", body=chat_body(), headers=headers
                )
                with lock:
                    result.codes[status] = result.codes.get(status, 0) + 1
                    if status == 200:
                        result.admitted += 1
                        if result.admitted >= max_calls:
                            stop.set()
                    else:
                        result.first_error_status = status
                        result.first_error_body = _payload
                        stop.set()
        except (OSError, http.client.HTTPException):
            stop.set()
        finally:
            client.close()

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, workers))]
    for thread in threads:
        thread.start()
    stop.wait(timeout=max_seconds)
    stop.set()
    for thread in threads:
        thread.join(timeout=2)
    result.elapsed_s = time.perf_counter() - t0
    result.spend_usd = result.admitted * cost_per_call
    if result.first_error_status:
        result.stop_reason = f"{result.first_error_status} {parse_budget_error(result.first_error_body)}"
    else:
        result.stop_reason = f"still burning ({max_seconds:.1f}s cap)"
    return result


@dataclass
class Ledger:
    customer_id: str
    event_count: int | None = None
    cost_usd: float | None = None
    debt_usd: float | None = None
    held_usd: float | None = None
    balance_usd: float | None = None
    settled: bool = False


def poll_ledger(api: HttpJson, customer_id: str, admitted: int, timeout: float) -> Ledger:
    ledger = Ledger(customer_id=customer_id)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _status, usage, _ = api.json("GET", f"/usage/customer/{customer_id}")
        _bstatus, budget, _ = api.json("GET", f"/budget/{customer_id}")
        if isinstance(usage, dict):
            ledger.event_count = usage.get("event_count")
            try:
                ledger.cost_usd = float(usage["cost_usd"]) if usage.get("cost_usd") is not None else None
            except (TypeError, ValueError, KeyError):
                ledger.cost_usd = None
        if isinstance(budget, dict):
            ledger.debt_usd = float(budget.get("debt_usd") or 0)
            ledger.held_usd = float(budget.get("held_usd") or 0)
            ledger.balance_usd = float(budget.get("balance_usd") or 0)
        events_ready = ledger.event_count == admitted
        holds_released = ledger.held_usd == 0
        if events_ready and holds_released:
            ledger.settled = True
            return ledger
        time.sleep(0.5)
    return ledger


def format_comparison(
    *,
    budget_usd: float,
    cost_per_call: float,
    unguarded: LoopResult,
    guarded: LoopResult,
    ledger: Ledger | None,
    customer_id: str,
    wall_s: float,
) -> str:
    a_stop = unguarded.stop_reason
    b_stop = guarded.stop_reason if guarded.stopped else unguarded.stop_reason
    b_debt = None
    b_spend = guarded.spend_usd
    if ledger and ledger.cost_usd is not None:
        b_spend = ledger.cost_usd
    if ledger and ledger.debt_usd is not None:
        b_debt = ledger.debt_usd
    a_over = max(0.0, unguarded.spend_usd - budget_usd)
    b_over = max(0.0, b_spend - budget_usd)
    if b_debt is not None:
        b_over_label = usd(b_debt)
    else:
        b_over_label = usd(b_over)

    lines = [
        "",
        "=" * 72,
        " FluxMeter runaway demo — no provider key",
        f" Budget {usd(budget_usd)}   ·  {MODEL}  ·  {usd(cost_per_call)}/call"
        f"  ({INPUT_TOKENS} in + {OUTPUT_TOKENS} out)",
        "=" * 72,
        "",
        f" [A] no FluxMeter           {unguarded.admitted:>5} calls in {unguarded.elapsed_s:5.2f}s"
        f"  — {a_stop}",
        f" [B] FluxMeter Gateway      {guarded.admitted:>5} admitted, stop in {guarded.elapsed_s:5.2f}s"
        f"  — {b_stop}",
        "",
        f" {'':<22}{'A  no FluxMeter':<24}{'B  FluxMeter Gateway':<24}",
        f" {'Calls admitted':<22}{unguarded.admitted:<24}{guarded.admitted:<24}",
        f" {'Spend USD':<22}{usd(unguarded.spend_usd):<24}{usd(b_spend):<24}",
        f" {'Budget':<22}{usd(budget_usd):<24}{usd(budget_usd):<24}",
        f" {'Overspend / debt':<22}{usd(a_over):<24}{b_over_label:<24}",
        f" {'Time to stop':<22}{'did not stop':<24}{f'{guarded.elapsed_s:.2f}s':<24}",
        "",
        f" Your agent loop just tried to spend {usd(unguarded.spend_usd)}.",
        f" FluxMeter stopped it at {usd(b_spend)}.",
    ]
    if ledger:
        settle = "settled" if ledger.settled else "projection pending"
        lines.append(
            f" Ledger customer={customer_id}  events={ledger.event_count}"
            f"  cost_usd={ledger.cost_usd}  debt_usd={ledger.debt_usd}"
            f"  held_usd={ledger.held_usd}  ({settle})"
        )
    lines.append(f" Wall-clock {wall_s:.1f}s (warm path: mock overlay + both loops + ledger poll)")
    lines.append("=" * 72)
    return "\n".join(lines)


def require_stack(api: HttpJson, gateway: HttpJson, ready_timeout: float) -> None:
    try:
        wait_for(
            "API /ready",
            lambda: api.json("GET", "/ready")[0],
            lambda status: status == 200,
            timeout=ready_timeout,
        )
    except RuntimeError as exc:
        raise RuntimeError(
            f"{exc}\nStart the stack first: make demo   (or make start)"
        ) from exc
    try:
        wait_for(
            "Gateway /health",
            lambda: gateway.json("GET", "/health")[0],
            lambda status: status == 200,
            timeout=ready_timeout,
        )
    except RuntimeError as exc:
        raise RuntimeError(
            f"{exc}\nRecreate Gateway with the mock overlay: make demo-runaway"
        ) from exc


def run(
    *,
    api_url: str,
    gateway_url: str,
    mock_port: int,
    budget_usd: float,
    unguarded_seconds: float,
    unguarded_max_calls: int,
    unguarded_workers: int,
    settle_timeout: float,
    ready_timeout: float,
) -> int:
    wall0 = time.perf_counter()
    cost_per_call = catalog_cost_usd()
    customer_id = "runaway-" + uuid.uuid4().hex[:8]
    mock_url = f"http://127.0.0.1:{mock_port}"

    print(
        f"Budget {usd(budget_usd)}   runaway agent: {usd(cost_per_call)}/call, infinite loop",
        flush=True,
    )
    print("No provider key. [A] hits the mock directly. [B] goes through FluxMeter Gateway.", flush=True)

    with MockUpstream(mock_port):
        mock = HttpJson(mock_url)
        api = HttpJson(api_url)
        gateway = HttpJson(gateway_url)
        try:
            wait_for(
                "mock upstream",
                lambda: mock.json("GET", "/health")[0],
                lambda status: status == 200,
                timeout=5,
            )
            require_stack(api, gateway, ready_timeout)

            status, body, _ = api.json(
                "POST",
                f"/budget/{customer_id}",
                body={"balance_usd": budget_usd, "alert_threshold_usd": budget_usd * 0.2},
            )
            if status not in (200, 201):
                raise RuntimeError(f"POST /budget/{customer_id} failed: {status} {body}")

            print(
                f"\n[A] no FluxMeter  — mock {mock_url}  "
                f"({unguarded_seconds:.1f}s cap, {unguarded_workers} workers)",
                flush=True,
            )
            unguarded = run_unguarded_parallel(
                mock_url,
                headers={"Authorization": "Bearer sk-runaway", "Content-Type": "application/json"},
                cost_per_call=cost_per_call,
                max_seconds=unguarded_seconds,
                max_calls=unguarded_max_calls,
                workers=unguarded_workers,
            )
            print(
                f"[A] {unguarded.admitted} calls  {usd(unguarded.spend_usd)}  "
                f"{unguarded.elapsed_s:.2f}s  {unguarded.stop_reason}",
                flush=True,
            )

            print(f"\n[B] FluxMeter Gateway  — {gateway_url}  customer={customer_id}", flush=True)
            guarded = run_chat_loop(
                gateway,
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer sk-runaway",
                    "Content-Type": "application/json",
                    "X-FluxMeter-Customer-Id": customer_id,
                },
                cost_per_call=cost_per_call,
                max_seconds=40.0,
                max_calls=unguarded_max_calls,
                stop_on_non_200=True,
            )
            hold = ""
            if guarded.reserved_usd is not None:
                hold = f"  reserved {usd(guarded.reserved_usd)}/call"
                if abs(guarded.reserved_usd - cost_per_call) > 0.005:
                    print(
                        f"NOTE: Gateway reserved {usd(guarded.reserved_usd)}/call vs catalog "
                        f"{usd(cost_per_call)}/call. make demo-runaway sets "
                        "GATEWAY_DEFAULT_INPUT_TOKENS=4000 so the hold matches mock usage.",
                        flush=True,
                    )
            print(
                f"[B] {guarded.admitted} admitted  {usd(guarded.spend_usd)}  "
                f"{guarded.elapsed_s:.2f}s  {guarded.stop_reason}{hold}",
                flush=True,
            )

            if not guarded.stopped or guarded.first_error_status != 402:
                hint = ""
                if guarded.first_error_status == 401:
                    hint = (
                        "\nGateway has no upstream key. Use `make demo-runaway` "
                        "so docker-compose.runaway.yml sets GATEWAY_UPSTREAM_API_KEY."
                    )
                elif guarded.first_error_status in (502, 503, None) or guarded.admitted == 0:
                    hint = (
                        "\nGateway could not reach the mock. `make demo-runaway` points "
                        "GATEWAY_UPSTREAM_BASE at host.docker.internal:19090."
                    )
                raise RuntimeError(
                    f"Gateway did not return 402 budget_exceeded "
                    f"(status={guarded.first_error_status} body={guarded.first_error_body!r})."
                    + hint
                )

            print("\nWaiting for Flink ledger (hold release + usage projection)...", flush=True)
            ledger = poll_ledger(api, customer_id, guarded.admitted, settle_timeout)
            wall_s = time.perf_counter() - wall0
            print(
                format_comparison(
                    budget_usd=budget_usd,
                    cost_per_call=cost_per_call,
                    unguarded=unguarded,
                    guarded=guarded,
                    ledger=ledger,
                    customer_id=customer_id,
                    wall_s=wall_s,
                ),
                flush=True,
            )
            spend = ledger.cost_usd if ledger.cost_usd is not None else guarded.spend_usd
            if wall_s > 60:
                print(f"WARN: wall-clock {wall_s:.1f}s exceeded the 60s wow-demo budget.", flush=True)
            print(
                f"PASS: Gateway 402 after {guarded.admitted} admitted calls in "
                f"{guarded.elapsed_s:.2f}s; unguarded run kept spending "
                f"{usd(unguarded.spend_usd)} vs budget {usd(budget_usd)}; "
                f"guarded spend {usd(spend)}.",
                flush=True,
            )
            return 0
        finally:
            mock.close()
            api.close()
            gateway.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=os.getenv("FLUXMETER_API_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--gateway", default=os.getenv("FLUXMETER_GATEWAY", "http://127.0.0.1:8080"))
    parser.add_argument("--mock-port", type=int, default=int(os.getenv("FLUXMETER_MOCK_PORT", str(DEFAULT_MOCK_PORT))))
    parser.add_argument("--budget", type=float, default=BUDGET_USD)
    parser.add_argument("--unguarded-seconds", type=float, default=UNGUARDED_SECONDS)
    parser.add_argument("--unguarded-calls", type=int, default=UNGUARDED_MAX_CALLS)
    parser.add_argument("--unguarded-workers", type=int, default=UNGUARDED_WORKERS)
    parser.add_argument("--settle-timeout", type=float, default=SETTLE_TIMEOUT)
    parser.add_argument("--ready-timeout", type=float, default=30.0)
    args = parser.parse_args()
    try:
        return run(
            api_url=args.api.rstrip("/"),
            gateway_url=args.gateway.rstrip("/"),
            mock_port=args.mock_port,
            budget_usd=args.budget,
            unguarded_seconds=args.unguarded_seconds,
            unguarded_max_calls=args.unguarded_calls,
            unguarded_workers=args.unguarded_workers,
            settle_timeout=args.settle_timeout,
            ready_timeout=args.ready_timeout,
        )
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
