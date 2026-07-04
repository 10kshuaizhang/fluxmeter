package io.fluxmeter.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BillingPeriodTest {

    @Test
    void monthUtc_usesUtcCalendarMonth() {
        long ts = Instant.parse("2026-07-01T00:00:00Z").toEpochMilli();
        assertEquals("2026-07", BillingPeriod.monthUtc(ts));
    }

    @Test
    void periodVolumeKey_customerModelScope() {
        long ts = Instant.parse("2026-07-04T12:00:00Z").toEpochMilli();
        String key = BillingPeriod.periodVolumeKey(null, "cust1", "gpt-4o", ts);
        assertEquals("customer:cust1:model:gpt-4o:period:2026-07:volume_tokens", key);
    }

    @Test
    void periodVolumeKey_tenantScoped() {
        long ts = Instant.parse("2026-07-04T12:00:00Z").toEpochMilli();
        String key = BillingPeriod.periodVolumeKey("t1", "cust1", "gpt-4o", ts);
        assertEquals(
                "tenant:t1:customer:cust1:model:gpt-4o:period:2026-07:volume_tokens",
                key);
    }
}
