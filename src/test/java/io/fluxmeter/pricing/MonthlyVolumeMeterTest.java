package io.fluxmeter.pricing;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MonthlyVolumeMeterTest {

    private static long ts(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    @Test
    void startsAtZero() {
        MonthlyVolumeMeter meter = new MonthlyVolumeMeter();
        assertEquals(0L, meter.tokensBefore(ts("2026-07-04T12:00:00Z")));
    }

    @Test
    void advancesAfterAdvance() {
        MonthlyVolumeMeter meter = new MonthlyVolumeMeter();
        long july = ts("2026-07-04T12:00:00Z");
        assertEquals(0L, meter.tokensBefore(july));
        meter.advance(july, 1_000_000);
        assertEquals(1_000_000L, meter.tokensBefore(july));
    }

    @Test
    void resetsOnUtcMonthBoundary() {
        MonthlyVolumeMeter meter = new MonthlyVolumeMeter();
        long endJuly = ts("2026-07-31T23:59:00Z");
        meter.advance(endJuly, 5_000_000);
        assertEquals(5_000_000L, meter.tokensBefore(endJuly));

        long august = ts("2026-08-01T00:01:00Z");
        assertEquals(0L, meter.tokensBefore(august));
        meter.advance(august, 100);
        assertEquals(100L, meter.tokensBefore(august));
        assertEquals("2026-08", meter.getBillingPeriod());
    }

    @Test
    void hydratesFromState() {
        MonthlyVolumeMeter meter = MonthlyVolumeMeter.fromState(9_000_000L, "2026-07");
        assertEquals(9_000_000L, meter.tokensBefore(ts("2026-07-15T00:00:00Z")));
    }
}
