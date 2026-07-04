package io.fluxmeter.model;

import io.fluxmeter.pricing.MonthlyVolumeMeter;
import io.fluxmeter.pricing.PricingCatalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Simulates Flink UsageAggregateFunction tier metering without a mini cluster. */
class UsageAggregateTierTest {

    @BeforeEach
    void loadTieredCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    private static long ts(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    private static void addEvent(UsageAggregate agg, MonthlyVolumeMeter meter, TokenEvent event) {
        long before = meter.tokensBefore(event.getTimestamp());
        agg.addEvent(event, before);
        meter.advance(event.getTimestamp(), event.getTotalTokens());
    }

    @Test
    void volumePricing_entireEventUsesTierFromMeter() {
        MonthlyVolumeMeter meter = MonthlyVolumeMeter.fromState(9_000_000L, "2026-07");
        UsageAggregate agg = new UsageAggregate();

        TokenEvent event = new TokenEvent();
        event.setModelId("gpt-4o-mini");
        event.setInputTokens(1_000_000);
        event.setTimestamp(ts("2026-07-04T12:00:00Z"));

        addEvent(agg, meter, event);
        assertEquals(150_000L, agg.getCostMicro());
        assertEquals(10_000_000L, meter.getMonthlyTokens());
    }

    @Test
    void volumePricing_secondEventUsesHigherTier() {
        MonthlyVolumeMeter meter = MonthlyVolumeMeter.fromState(10_000_000L, "2026-07");
        UsageAggregate agg = new UsageAggregate();

        TokenEvent event = new TokenEvent();
        event.setModelId("gpt-4o-mini");
        event.setInputTokens(1_000_000);
        event.setTimestamp(ts("2026-07-04T12:00:00Z"));

        addEvent(agg, meter, event);
        assertEquals(120_000L, agg.getCostMicro());
    }

    @Test
    void graduatedPricing_splitsAtBoundary() {
        MonthlyVolumeMeter meter = MonthlyVolumeMeter.fromState(900_000L, "2026-07");
        UsageAggregate agg = new UsageAggregate();

        TokenEvent event = new TokenEvent();
        event.setModelId("claude-sonnet-4");
        event.setInputTokens(100_000);
        event.setOutputTokens(100_000);
        event.setTimestamp(ts("2026-07-04T12:00:00Z"));

        addEvent(agg, meter, event);
        assertEquals(400_000L, agg.getCostMicro());
    }
}
