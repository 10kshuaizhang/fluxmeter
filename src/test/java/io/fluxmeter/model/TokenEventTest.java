package io.fluxmeter.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TokenEventTest {

    @Test
    void aggregationKey_singleTenant() {
        TokenEvent event = new TokenEvent();
        event.setCustomerId("cust_1");
        event.setModelId("gpt-4o");
        assertEquals("cust_1|gpt-4o", event.getAggregationKey());
    }

    @Test
    void aggregationKey_multiTenant() {
        TokenEvent event = new TokenEvent();
        event.setTenantId("tenant_xyz");
        event.setCustomerId("cust_1");
        event.setModelId("gpt-4o");
        assertEquals("tenant_xyz|cust_1|gpt-4o", event.getAggregationKey());
    }
}
