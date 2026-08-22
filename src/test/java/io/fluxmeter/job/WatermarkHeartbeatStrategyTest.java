package io.fluxmeter.job;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WatermarkHeartbeatStrategyTest {

    @Test
    void sourceIdlenessLeavesRoomForHeartbeatToCloseTenSecondWindow() {
        assertEquals(Duration.ofSeconds(15), TokenUsageAggregator.sourceIdleness());
    }
}
