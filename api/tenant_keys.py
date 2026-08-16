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


def budget_prefix_for_write(tenant_id: str | None, customer_id: str) -> str:
    """Writes always use the canonical (possibly tenant-scoped) key."""
    return budget_prefix(tenant_id, customer_id)


def customer_prefix_for_write(tenant_id: str | None, customer_id: str) -> str:
    return customer_prefix(tenant_id, customer_id)


def budget_prefix_for_read(redis_client, tenant_id: str | None, customer_id: str) -> str:
    """Prefer tenant key; fall back to legacy budget:{cid} when migrating.

    ponytail: dual-read during tenant cutover; ceiling = forever dual schema.
    Upgrade: one-shot migrate script + drop legacy branch.
    """
    preferred = budget_prefix(tenant_id, customer_id)
    if not has_tenant(tenant_id):
        return preferred
    if redis_client.exists(f"{preferred}:balance_usd") or redis_client.exists(f"{preferred}:held_usd"):
        return preferred
    legacy = f"budget:{customer_id}"
    if redis_client.exists(f"{legacy}:balance_usd") or redis_client.exists(f"{legacy}:held_usd"):
        return legacy
    return preferred


def customer_prefix_for_read(redis_client, tenant_id: str | None, customer_id: str) -> str:
    """Prefer tenant customer prefix; fall back to bare customer:{cid}.

    ponytail: dual-read during tenant cutover; ceiling = forever dual schema.
    Upgrade: one-shot migrate script + drop legacy branch.
    """
    preferred = customer_prefix(tenant_id, customer_id)
    if not has_tenant(tenant_id):
        return preferred
    if (
        redis_client.exists(f"{preferred}:total_tokens")
        or redis_client.exists(f"{preferred}:cost_usd")
        or redis_client.exists(f"{preferred}:spans")
    ):
        return preferred
    legacy = f"customer:{customer_id}"
    if (
        redis_client.exists(f"{legacy}:total_tokens")
        or redis_client.exists(f"{legacy}:cost_usd")
        or redis_client.exists(f"{legacy}:spans")
    ):
        return legacy
    return preferred


def global_ns_for_read(redis_client, tenant_id: str | None) -> str:
    """Return 'global' or 'tenant:{tid}:global' with legacy fallback."""
    if not has_tenant(tenant_id):
        return "global"
    preferred = f"tenant:{tenant_id}:global"
    if redis_client.exists(f"{preferred}:total_events") or redis_client.exists(
        f"{preferred}:total_cost_usd"
    ):
        return preferred
    if redis_client.exists("global:total_events") or redis_client.exists("global:total_cost_usd"):
        return "global"
    return preferred


def global_key_for_write(tenant_id: str | None, suffix: str) -> str:
    return global_key(tenant_id, suffix)
