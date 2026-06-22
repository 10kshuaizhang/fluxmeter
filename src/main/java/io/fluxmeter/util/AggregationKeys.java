package io.fluxmeter.util;

import io.fluxmeter.model.UsageAggregate;

/**
 * Parses Flink window keys into tenant / customer / model fields.
 * Key format: {@code tenant|customer|model} (SaaS) or {@code customer|model} (lite).
 */
public final class AggregationKeys {

    private AggregationKeys() {}

    public static void applyTo(UsageAggregate agg, String key) {
        String[] parts = key.split("\\|", -1);
        if (parts.length >= 3) {
            agg.setTenantId(parts[0]);
            agg.setCustomerId(parts[1]);
            agg.setModelId(parts[2]);
        } else if (parts.length == 2) {
            agg.setCustomerId(parts[0]);
            agg.setModelId(parts[1]);
        } else {
            agg.setCustomerId(key);
            agg.setModelId("unknown");
        }
    }
}
