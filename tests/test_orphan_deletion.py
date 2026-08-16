"""Guard: deleted Lite writers / sinks must stay gone."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_no_increment_writers_in_api():
    text = (ROOT / "api" / "usage_buckets.py").read_text()
    assert "def increment_session" not in text
    assert "def increment_span" not in text


def test_optimized_redis_sink_deleted():
    assert not (ROOT / "src/main/java/io/fluxmeter/sink/OptimizedRedisSink.java").exists()


def test_no_production_import_of_increment():
    banned = ("increment_session", "increment_span")
    for path in (ROOT / "api").rglob("*.py"):
        if path.name.startswith("test_") or "__pycache__" in str(path):
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    assert alias.name not in banned, f"{path} imports {alias.name}"
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                # allow strings in comments only — skip Name checks for call sites
                pass
