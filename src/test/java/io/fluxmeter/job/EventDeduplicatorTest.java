package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventDeduplicatorTest {

    @Test
    void dropsDuplicateInsideSafetyWindow() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, TokenEvent, TokenEvent> harness =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new EventDeduplicator(600), TokenEvent::getEventId, Types.STRING);
        harness.open();
        try {
            TokenEvent first = event("evt-1");
            TokenEvent duplicate = event("evt-1");
            harness.processElement(first, 1L);
            harness.processElement(duplicate, 2L);

            List<TokenEvent> output = harness.extractOutputValues();
            assertEquals(1, output.size());
            assertEquals("evt-1", output.get(0).getEventId());
        } finally {
            harness.close();
        }
    }

    private static TokenEvent event(String eventId) {
        TokenEvent event = new TokenEvent();
        event.setEventId(eventId);
        event.setCustomerId("customer");
        event.setModelId("gpt-4o-mini");
        return event;
    }
}
