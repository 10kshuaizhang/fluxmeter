package io.fluxmeter.pricing;

import io.fluxmeter.model.TokenEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PricingCatalogTest {

    @BeforeEach
    void loadCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("config/pricing.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void normalizeModelId_stripsVersionSuffix() {
        assertEquals("gpt-4o", PricingCatalog.get().normalizeModelId("gpt-4o-2024-08-06"));
    }

    @Test
    void calculateGpt4oInputCost_oneMillionTokens() {
        TokenEvent event = new TokenEvent();
        event.setModelId("gpt-4o");
        event.setInputTokens(1_000_000);
        event.setOutputTokens(0);
        // input_per_m = 2.50 → 1M tokens × 2.50 = 2_500_000 microdollars ($2.50)
        assertEquals(2_500_000L, PricingCatalog.get().calculateEventCostMicro(event));
    }

    @Test
    void calculateClaudeOpus_mixedTokens() {
        TokenEvent event = new TokenEvent();
        event.setModelId("claude-opus-4");
        event.setInputTokens(10_000);
        event.setOutputTokens(5_000);
        long micro = PricingCatalog.get().calculateEventCostMicro(event);
        // 10000*15 + 5000*75 = 150000 + 375000 = 525000 micro
        assertEquals(525_000L, micro);
        assertTrue(PricingCatalog.get().calculateEventCost(event) > 0.5);
    }

    @Test
    void cacheReadTokens_discounted() {
        TokenEvent event = new TokenEvent();
        event.setModelId("gpt-4o");
        event.setCacheReadTokens(1_000_000);
        long withCache = PricingCatalog.get().calculateEventCostMicro(event);

        TokenEvent fullPrice = new TokenEvent();
        fullPrice.setModelId("gpt-4o");
        fullPrice.setInputTokens(1_000_000);
        long withoutCache = PricingCatalog.get().calculateEventCostMicro(fullPrice);

        assertTrue(withCache < withoutCache);
        assertEquals(withoutCache / 2, withCache);
    }
}
