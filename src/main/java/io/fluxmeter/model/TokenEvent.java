package io.fluxmeter.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Unified token usage event supporting multiple AI providers.
 *
 * Supports OpenAI, Anthropic, Google, and custom providers.
 * Each event represents one LLM API call's token usage.
 */
public class TokenEvent implements Serializable {
    private static final long serialVersionUID = 2L;

    // Identity
    private String eventId;
    private String tenantId;       // SaaS tenant scope (optional; omit for single-tenant)
    private String customerId;
    private String requestId;      // Provider request ID (e.g. chatcmpl-xxx)
    private String spanId;         // Agent/trace span ID for observability
    private String parentSpanId;   // Parent span (links child LLM calls to agent run)

    // Trusted ingestion envelope (server/operator supplied, never client payload)
    private String apiKeyId;
    private String ingestSource;
    private String ingestTraceId;
    private String reservationId;
    private double reservedUsd;
    private long receivedAt;
    private boolean malformedEnvelope;
    private String rawEnvelope;
    private boolean authorizedReplay;

    // Provider & Model
    private String provider;       // "openai", "anthropic", "google", "azure"
    private String modelId;        // "gpt-4o", "claude-sonnet-4-6", etc.

    // Token counts (all optional — set what's available from provider response)
    private int inputTokens;       // Prompt/input tokens
    private int outputTokens;      // Completion/output tokens
    private int cacheReadTokens;   // Cached input tokens (OpenAI)
    private int cacheWriteTokens;  // Tokens written to cache
    private int reasoningTokens;   // Reasoning tokens (o1, o3)
    private int embeddingTokens;   // Embedding tokens

    // Timing
    private long timestamp;        // Epoch millis (event time, not ingestion time)
    private int latencyMs;         // Provider response latency (optional)

    // Context
    private String sessionId;      // User session / conversation ID
    private String environment;    // "production", "staging", "development"
    private Map<String, String> metadata;  // Arbitrary key-value pairs

    public TokenEvent() {}

    /**
     * Returns total billable tokens for this event.
     */
    public int getTotalTokens() {
        return inputTokens + outputTokens + cacheReadTokens
                + cacheWriteTokens + reasoningTokens + embeddingTokens;
    }

    /**
     * Composite key for Flink keyed stream aggregation.
     */
    public String getAggregationKey() {
        if (tenantId != null && !tenantId.isBlank()) {
            return tenantId + "|" + customerId + "|" + modelId;
        }
        return customerId + "|" + modelId;
    }

    // Getters and setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getParentSpanId() { return parentSpanId; }
    public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

    public String getApiKeyId() { return apiKeyId; }
    public void setApiKeyId(String apiKeyId) { this.apiKeyId = apiKeyId; }

    public String getIngestSource() { return ingestSource; }
    public void setIngestSource(String ingestSource) { this.ingestSource = ingestSource; }

    public String getIngestTraceId() { return ingestTraceId; }
    public void setIngestTraceId(String ingestTraceId) { this.ingestTraceId = ingestTraceId; }

    public String getReservationId() { return reservationId; }
    public void setReservationId(String reservationId) { this.reservationId = reservationId; }

    public double getReservedUsd() { return reservedUsd; }
    public void setReservedUsd(double reservedUsd) { this.reservedUsd = reservedUsd; }

    public long getReceivedAt() { return receivedAt; }
    public void setReceivedAt(long receivedAt) { this.receivedAt = receivedAt; }

    public boolean isMalformedEnvelope() { return malformedEnvelope; }
    public void setMalformedEnvelope(boolean malformedEnvelope) { this.malformedEnvelope = malformedEnvelope; }

    public String getRawEnvelope() { return rawEnvelope; }
    public void setRawEnvelope(String rawEnvelope) { this.rawEnvelope = rawEnvelope; }

    public boolean isAuthorizedReplay() { return authorizedReplay; }
    public void setAuthorizedReplay(boolean authorizedReplay) { this.authorizedReplay = authorizedReplay; }

    public String getProvider() { return provider; }
    public void setProvider(String provider) { this.provider = provider; }

    public String getModelId() { return modelId; }
    public void setModelId(String modelId) { this.modelId = modelId; }

    public int getInputTokens() { return inputTokens; }
    public void setInputTokens(int inputTokens) { this.inputTokens = inputTokens; }

    public int getOutputTokens() { return outputTokens; }
    public void setOutputTokens(int outputTokens) { this.outputTokens = outputTokens; }

    public int getCacheReadTokens() { return cacheReadTokens; }
    public void setCacheReadTokens(int cacheReadTokens) { this.cacheReadTokens = cacheReadTokens; }

    public int getCacheWriteTokens() { return cacheWriteTokens; }
    public void setCacheWriteTokens(int cacheWriteTokens) { this.cacheWriteTokens = cacheWriteTokens; }

    public int getReasoningTokens() { return reasoningTokens; }
    public void setReasoningTokens(int reasoningTokens) { this.reasoningTokens = reasoningTokens; }

    public int getEmbeddingTokens() { return embeddingTokens; }
    public void setEmbeddingTokens(int embeddingTokens) { this.embeddingTokens = embeddingTokens; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public int getLatencyMs() { return latencyMs; }
    public void setLatencyMs(int latencyMs) { this.latencyMs = latencyMs; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenEvent that = (TokenEvent) o;
        return Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }
}
