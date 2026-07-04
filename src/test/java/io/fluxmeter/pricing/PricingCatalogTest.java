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
        assertEquals(2_500_000L, PricingCatalog.get().calculateEventCostMicro(event));
    }

    @Test
    void calculateClaudeOpus_mixedTokens() {
        TokenEvent event = new TokenEvent();
        event.setModelId("claude-opus-4");
        event.setInputTokens(10_000);
        event.setOutputTokens(5_000);
        long micro = PricingCatalog.get().calculateEventCostMicro(event);
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

    @Test
    void volumePricing_entireEventUsesTierFromMonthlyBefore() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));

        TokenEvent event = new TokenEvent();
        event.setModelId("gpt-4o-mini");
        event.setInputTokens(1_000_000);
        event.setOutputTokens(0);

        // 9M before → tier-1 (up_to 10M): 1M × 0.15 = 150_000 micro
        assertEquals(150_000L, PricingCatalog.get().calculateEventCostMicro(event, 9_000_000L));

        // 10M before → tier-2: 1M × 0.12 = 120_000 micro
        assertEquals(120_000L, PricingCatalog.get().calculateEventCostMicro(event, 10_000_000L));
    }

    @Test
    void volumePricing_tierBoundaryUsesMillionsFloor() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));

        TokenEvent event = new TokenEvent();
        event.setModelId("gpt-4o-mini");
        event.setInputTokens(100_000);

        // 9_999_999 tokens → tokensM=9, still tier-1
        assertEquals(15_000L, PricingCatalog.get().calculateEventCostMicro(event, 9_999_999L));
    }

    @Test
    void graduatedPricing_splitsAcrossTierBoundary() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));

        TokenEvent event = new TokenEvent();
        event.setModelId("claude-sonnet-4");
        event.setInputTokens(100_000);
        event.setOutputTokens(100_000);

        // monthlyBefore=900K: input 100K @ tier1 (2.0), output 100K @ tier2 (2.0)
        long cost = PricingCatalog.get().calculateEventCostMicro(event, 900_000L);
        assertEquals(400_000L, cost);
    }

    @Test
    void graduatedPricing_allInFirstTier() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));

        TokenEvent event = new TokenEvent();
        event.setModelId("claude-sonnet-4");
        event.setInputTokens(50_000);
        event.setOutputTokens(50_000);

        // input 50K×2.0 + output 50K×4.0 = 100K + 200K = 300K micro
        assertEquals(300_000L, PricingCatalog.get().calculateEventCostMicro(event, 0L));
    }

    @Test
    void catalogDefaults_volumeScopeAndBillingPeriod() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog catalog = PricingCatalog.loadFromBytes(json);
        assertEquals(PricingCatalog.VolumeScope.CUSTOMER_MODEL, catalog.getVolumeScope());
        assertEquals(PricingCatalog.BillingPeriod.CALENDAR_MONTH, catalog.getBillingPeriod());
    }
}
