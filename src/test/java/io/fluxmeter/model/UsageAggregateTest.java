package io.fluxmeter.model;

import io.fluxmeter.pricing.PricingCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UsageAggregateTest {

    @BeforeEach
    void loadCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("config/pricing.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void deduplicatesSameEventId() {
        UsageAggregate agg = new UsageAggregate();
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-1");
        event.setModelId("gpt-4o");
        event.setInputTokens(100);
        event.setOutputTokens(50);

        agg.addEvent(event);
        agg.addEvent(event);

        assertEquals(1, agg.getEventCount());
        assertEquals(150, agg.getTotalTokens());
    }

    @Test
    void propagatesTenantIdFromEvent() {
        UsageAggregate agg = new UsageAggregate();
        TokenEvent event = new TokenEvent();
        event.setTenantId("tenant_abc");
        event.setCustomerId("cust_1");
        event.setModelId("gpt-4o");
        event.setInputTokens(10);

        agg.addEvent(event);
        assertEquals("tenant_abc", agg.getTenantId());
    }

    @Test
    void mergeCombinesCounters() {
        UsageAggregate a = new UsageAggregate();
        UsageAggregate b = new UsageAggregate();

        TokenEvent e1 = new TokenEvent();
        e1.setEventId("e1");
        e1.setModelId("gpt-4o");
        e1.setInputTokens(100);
        a.addEvent(e1);

        TokenEvent e2 = new TokenEvent();
        e2.setEventId("e2");
        e2.setModelId("gpt-4o");
        e2.setInputTokens(200);
        b.addEvent(e2);

        a.merge(b);
        assertEquals(2, a.getEventCount());
        assertEquals(300, a.getInputTokens());
    }
}
