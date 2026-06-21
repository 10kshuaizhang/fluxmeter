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
    private String customerId;
    private String requestId;      // Provider request ID (e.g. chatcmpl-xxx)
    private String spanId;         // Agent/trace span ID for observability
    private String parentSpanId;   // Parent span (links child LLM calls to agent run)

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
        return customerId + "|" + modelId;
    }

    // Getters and setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getParentSpanId() { return parentSpanId; }
    public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

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
