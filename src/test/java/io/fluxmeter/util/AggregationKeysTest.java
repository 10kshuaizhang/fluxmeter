package io.fluxmeter.util;

import io.fluxmeter.model.UsageAggregate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AggregationKeysTest {

    @Test
    void applyTo_singleTenant_twoParts() {
        UsageAggregate agg = new UsageAggregate();
        AggregationKeys.applyTo(agg, "cust_1|gpt-4o");
        assertNull(agg.getTenantId());
        assertEquals("cust_1", agg.getCustomerId());
        assertEquals("gpt-4o", agg.getModelId());
    }

    @Test
    void applyTo_multiTenant_threeParts() {
        UsageAggregate agg = new UsageAggregate();
        AggregationKeys.applyTo(agg, "tenant_xyz|cust_1|gpt-4o-mini");
        assertEquals("tenant_xyz", agg.getTenantId());
        assertEquals("cust_1", agg.getCustomerId());
        assertEquals("gpt-4o-mini", agg.getModelId());
    }

    @Test
    void applyTo_malformed_singlePart() {
        UsageAggregate agg = new UsageAggregate();
        AggregationKeys.applyTo(agg, "orphan_key");
        assertEquals("orphan_key", agg.getCustomerId());
        assertEquals("unknown", agg.getModelId());
    }
}
