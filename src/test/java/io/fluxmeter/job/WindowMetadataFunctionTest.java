package io.fluxmeter.job;

import io.fluxmeter.model.UsageAggregate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WindowMetadataFunctionTest {

    @Test
    void parsesTenantCustomerModel() {
        UsageAggregate agg = new UsageAggregate();
        TokenUsageAggregator.WindowMetadataFunction.applyKeyAndWindow(
                "t1|cust_a|gpt-4o", agg, 1000L, 11000L);
        assertEquals("t1", agg.getTenantId());
        assertEquals("cust_a", agg.getCustomerId());
        assertEquals("gpt-4o", agg.getModelId());
        assertEquals(1000L, agg.getWindowStart());
        assertEquals(11000L, agg.getWindowEnd());
    }

    @Test
    void parsesCustomerModel() {
        UsageAggregate agg = new UsageAggregate();
        TokenUsageAggregator.WindowMetadataFunction.applyKeyAndWindow(
                "cust_b|claude-opus-4", agg, 0L, 10_000L);
        assertNull(agg.getTenantId());
        assertEquals("cust_b", agg.getCustomerId());
        assertEquals("claude-opus-4", agg.getModelId());
    }

    @Test
    void fallsBackForDegenerateKey() {
        UsageAggregate agg = new UsageAggregate();
        TokenUsageAggregator.WindowMetadataFunction.applyKeyAndWindow(
                "lonely", agg, 5L, 15L);
        assertEquals("lonely", agg.getCustomerId());
        assertEquals("unknown", agg.getModelId());
    }
}
