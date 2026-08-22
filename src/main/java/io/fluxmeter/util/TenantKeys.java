package io.fluxmeter.util;

/**
 * Redis key prefixes for single-tenant (default) and multi-tenant (SaaS) modes.
 *
 * Single-tenant (no tenantId): {@code customer:{id}:...}, {@code budget:{id}:...}
 * Multi-tenant: {@code tenant:{tid}:customer:{id}:...}, {@code tenant:{tid}:budget:{id}:...}
 */
public final class TenantKeys {

    private TenantKeys() {}

    public static boolean hasTenant(String tenantId) {
        return tenantId != null && !tenantId.isBlank();
    }

    public static String customerPrefix(String tenantId, String customerId) {
        if (hasTenant(tenantId)) {
            return "tenant:" + tenantId + ":customer:" + customerId;
        }
        return "customer:" + customerId;
    }

    public static String budgetPrefix(String tenantId, String customerId) {
        if (hasTenant(tenantId)) {
            return "tenant:" + tenantId + ":budget:" + customerId;
        }
        return "budget:" + customerId;
    }

    public static String globalKey(String tenantId, String suffix) {
        if (hasTenant(tenantId)) {
            return "tenant:" + tenantId + ":global:" + suffix;
        }
        return "global:" + suffix;
    }

    public static String scopePrefix(String tenantId, String kind, String scopeId) {
        if (!"span".equals(kind) && !"session".equals(kind)) {
            throw new IllegalArgumentException("scope kind must be span or session");
        }
        if (hasTenant(tenantId)) {
            return "tenant:" + tenantId + ":" + kind + ":" + scopeId;
        }
        return kind + ":" + scopeId;
    }

    public static String packageKey(String tenantId, String customerId) {
        if (hasTenant(tenantId)) {
            return "tenant:" + tenantId + ":package:" + customerId + ":tokens_remaining";
        }
        return "package:" + customerId + ":tokens_remaining";
    }

    public static String windowId(String tenantId, String customerId, String modelId, long windowStart) {
        if (hasTenant(tenantId)) {
            return tenantId + "|" + customerId + "|" + modelId + "|" + windowStart;
        }
        return customerId + "|" + modelId + "|" + windowStart;
    }

    /** Reservation attach set for a tumbling window (shared with BudgetEnforcerSink). */
    public static String windowReservationsKey(String windowId) {
        return "window:reservations:" + windowId;
    }
}
