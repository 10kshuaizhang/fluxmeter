"""Redis key prefixes — mirrors io.fluxmeter.util.TenantKeys (Java)."""

from __future__ import annotations


def has_tenant(tenant_id: str | None) -> bool:
    return bool(tenant_id and tenant_id.strip())


def customer_prefix(tenant_id: str | None, customer_id: str) -> str:
    if has_tenant(tenant_id):
        return f"tenant:{tenant_id}:customer:{customer_id}"
    return f"customer:{customer_id}"


def budget_prefix(tenant_id: str | None, customer_id: str) -> str:
    if has_tenant(tenant_id):
        return f"tenant:{tenant_id}:budget:{customer_id}"
    return f"budget:{customer_id}"


def global_key(tenant_id: str | None, suffix: str) -> str:
    if has_tenant(tenant_id):
        return f"tenant:{tenant_id}:global:{suffix}"
    return f"global:{suffix}"
