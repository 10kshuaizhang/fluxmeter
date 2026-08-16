package io.fluxmeter.pricing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fluxmeter.model.TokenEvent;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Consumes docs/contracts/pricing-vectors.json — must stay in sync with Python.
 */
class PricingVectorsTest {

    @Test
    void sharedGoldenVectors() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(Files.readAllBytes(Path.of("docs/contracts/pricing-vectors.json")));
        for (JsonNode caseNode : root) {
            PricingCatalog.reload(PricingCatalog.loadFromBytes(
                    Files.readAllBytes(Path.of(caseNode.get("catalog").asText()))));
            TokenEvent event = new TokenEvent();
            JsonNode ev = caseNode.get("event");
            event.setModelId(ev.get("modelId").asText());
            event.setInputTokens(ev.path("inputTokens").asInt(0));
            event.setOutputTokens(ev.path("outputTokens").asInt(0));
            long before = caseNode.path("monthly_tokens_before").asLong(0L);
            long micro = PricingCatalog.get().calculateEventCostMicro(event, before);
            assertEquals(
                    caseNode.get("expected_micro").asLong(),
                    micro,
                    caseNode.get("name").asText());
        }
    }
}
