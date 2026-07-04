package io.fluxmeter.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** UTC calendar-month billing period helpers (mirrors api/pricing_loader.py). */
public final class BillingPeriod {

    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ISO_LOCAL_DATE;

    private BillingPeriod() {}

    public static String monthUtc(long timestampMs) {
        return Instant.ofEpochMilli(timestampMs).atZone(ZoneOffset.UTC).format(MONTH);
    }

    public static String dayUtc(long timestampMs) {
        return Instant.ofEpochMilli(timestampMs).atZone(ZoneOffset.UTC).format(DAY);
    }

    /** customer_model scope monthly volume counter key. */
    public static String periodVolumeKey(
            String tenantId, String customerId, String modelId, long timestampMs) {
        String base = TenantKeys.customerPrefix(tenantId, customerId);
        return base + ":model:" + modelId + ":period:" + monthUtc(timestampMs) + ":volume_tokens";
    }
}
