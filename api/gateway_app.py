"""FluxMeter Gateway — OpenAI-compatible proxy with meter + guardrail."""

from __future__ import annotations

from fastapi import FastAPI

from gateway.deps import init_gateway
from gateway.routes import router

app = FastAPI(
    title="FluxMeter Gateway",
    description="OpenAI-compatible proxy with budget check, reserve, mid-stream kill, and auto-ingest",
    version="3.2.0",
)

app.include_router(router)


@app.on_event("startup")
async def startup():
    init_gateway()
