package io.fluxmeter.job;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LateEventRouterTest {

    @Test
    void routesOnlyWhenTheEventsWindowHasClosed() {
        long windowMillis = 10_000;

        assertFalse(TokenUsageAggregator.LateEventRouter.isLate(15_000, 19_999, windowMillis));
        assertTrue(TokenUsageAggregator.LateEventRouter.isLate(15_000, 20_000, windowMillis));
        assertFalse(TokenUsageAggregator.LateEventRouter.isLate(
                15_000, Long.MIN_VALUE, windowMillis));
    }
}
