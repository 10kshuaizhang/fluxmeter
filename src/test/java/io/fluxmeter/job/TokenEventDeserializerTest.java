package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenEventDeserializerTest {

    @Test
    void trustedEnvelopeOverridesClientIdentity() throws Exception {
        String json = """
                {
                  "envelopeVersion": 1,
                  "source": "http",
                  "payload": {
                    "eventId": "evt-1",
                    "tenantId": "tenant_forged",
                    "customerId": "cust_1",
                    "modelId": "gpt-4o-mini",
                    "inputTokens": 10,
                    "timestamp": 1770000000000
                  },
                  "auth": {
                    "tenantId": "tenant_trusted",
                    "customerId": "cust_1",
                    "apiKeyId": "key_7"
                  },
                  "receipt": {
                    "receivedAt": 1770000000123,
                    "traceId": "trace-1"
                  }
                }
                """;

        TokenEvent event = new TokenUsageAggregator.TokenEventDeserializer()
                .deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertEquals("tenant_trusted", event.getTenantId());
        assertEquals("cust_1", event.getCustomerId());
        assertEquals("key_7", event.getApiKeyId());
        assertEquals("http", event.getIngestSource());
        assertEquals(1770000000123L, event.getReceivedAt());
        assertEquals("trace-1", event.getIngestTraceId());
    }

    @Test
    void legacyUnenvelopedEventIsMarkedForDlq() throws Exception {
        String json = "{\"eventId\":\"evt-old\",\"customerId\":\"cust\",\"modelId\":\"m\"}";

        TokenEvent event = new TokenUsageAggregator.TokenEventDeserializer()
                .deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertTrue(event.isMalformedEnvelope());
        assertEquals(json, event.getRawEnvelope());
    }

    @Test
    void authorizedOperatorReplayUsesFreshReceiptTime() throws Exception {
        String json = """
                {"envelopeVersion":1,"source":"operator",
                 "payload":{"eventId":"evt-r","customerId":"cust","modelId":"m","timestamp":1},
                 "auth":{"customerId":"cust"},
                 "receipt":{"receivedAt":1770000000123},
                 "replay":{"authorized":true}}
                """;

        TokenEvent event = new TokenUsageAggregator.TokenEventDeserializer()
                .deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertTrue(event.isAuthorizedReplay());
        assertEquals(1770000000123L, event.getTimestamp());
    }
}
