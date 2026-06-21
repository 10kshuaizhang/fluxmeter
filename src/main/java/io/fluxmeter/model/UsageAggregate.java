package io.fluxmeter.model;

import java.io.Serializable;

/**
 * Aggregated token usage for a (customer, model) key within a time window.
 * Accumulates all token categories and computes cost.
 */
public class UsageAggregate implements Serializable {
    private static final long serialVersionUID = 2L;

    private String customerId;
    private String modelId;
    private long windowStart;
    private long windowEnd;

    // Token counts by category
    private long inputTokens;
    private long outputTokens;
    private long cacheReadTokens;
    private long cacheWriteTokens;
    private long reasoningTokens;
    private long embeddingTokens;
    private long totalTokens;

    // Computed — cost stored as microdollars (long) to avoid float accumulation errors.
    // 1 microdollar = $0.000001. Convert to USD: costMicro / 1_000_000.0
    private long costMicro;
    private long eventCount;
    private long totalLatencyMs;   // Sum of per-request latencies (for avg calc)
    private long deduplicatedCount; // Events skipped due to duplicate eventId

    public UsageAggregate() {}

    public UsageAggregate(String customerId, String modelId, long windowStart, long windowEnd) {
        this.customerId = customerId;
        this.modelId = modelId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public void addEvent(TokenEvent event) {
        this.inputTokens += event.getInputTokens();
        this.outputTokens += event.getOutputTokens();
        this.cacheReadTokens += event.getCacheReadTokens();
        this.cacheWriteTokens += event.getCacheWriteTokens();
        this.reasoningTokens += event.getReasoningTokens();
        this.embeddingTokens += event.getEmbeddingTokens();
        this.totalTokens += event.getTotalTokens();
        this.eventCount++;

        if (event.getLatencyMs() > 0) {
            this.totalLatencyMs += event.getLatencyMs();
        }

        this.costMicro += calculateEventCostMicro(event);
    }

    public UsageAggregate merge(UsageAggregate other) {
        this.inputTokens += other.inputTokens;
        this.outputTokens += other.outputTokens;
        this.cacheReadTokens += other.cacheReadTokens;
        this.cacheWriteTokens += other.cacheWriteTokens;
        this.reasoningTokens += other.reasoningTokens;
        this.embeddingTokens += other.embeddingTokens;
        this.totalTokens += other.totalTokens;
        this.costMicro += other.costMicro;
        this.eventCount += other.eventCount;
        this.totalLatencyMs += other.totalLatencyMs;
        return this;
    }

    /**
     * Cost in microdollars (long). 1 microdollar = $0.000001.
     * Using long eliminates float accumulation errors over millions of events.
     * Public for use by SpanAggregateFunction.
     */
    public static long calculateEventCostMicro(TokenEvent event) {
        String model = event.getModelId();
        long cost = 0;

        // Formula: tokens * price_per_million / 1M = tokens * price / 1M
        // In microdollars: tokens * price_micros_per_token
        // price_micros_per_token = price_per_million_dollars * 1_000_000 / 1_000_000 = price_per_million
        // So: cost_micros = tokens * price_per_million (since price is $/M and we want micros)
        // Wait: $2.50/M tokens = 2.5 microdollars per token? No.
        // $2.50/M = $0.0000025/token = 2.5 microdollars/token. Yes.
        // cost_micros = tokens * (price_dollars_per_M * 1_000_000 / 1_000_000)
        // Simpler: cost_micros = (long)(tokens * price_per_M)
        // Because: tokens/1M * $price * 1M_micros/$ = tokens * price microdollars

        cost += (long) event.getInputTokens() * (long) (getInputPrice(model) * 1.0);
        cost += (long) event.getOutputTokens() * (long) (getOutputPrice(model) * 1.0);
        cost += (long) event.getCacheReadTokens() * (long) (getCacheReadPrice(model) * 1.0);
        cost += (long) event.getReasoningTokens() * (long) (getOutputPrice(model) * 1.0);
        cost += (long) event.getCacheWriteTokens() * (long) (getInputPrice(model) * 1.0);
        cost += (long) event.getEmbeddingTokens() * (long) (getEmbeddingPrice(model) * 1.0);

        return cost;
    }

    /**
     * Cost in USD (double). Convenience wrapper for backward compatibility.
     * Internally uses microdollars then converts.
     */
    public static double calculateEventCost(TokenEvent event) {
        return calculateEventCostMicro(event) / 1_000_000.0;
    }

    private static double getInputPrice(String model) {
        return switch (model) {
            case "gpt-4o" -> 2.50;
            case "gpt-4o-mini" -> 0.15;
            case "o1" -> 15.00;
            case "o3-mini" -> 1.10;
            case "claude-opus-4" -> 15.00;
            case "claude-sonnet-4" -> 3.00;
            case "claude-haiku-4" -> 0.80;
            case "gemini-1.5-pro" -> 3.50;
            case "gemini-1.5-flash" -> 0.075;
            default -> 1.00;
        };
    }

    private static double getOutputPrice(String model) {
        return switch (model) {
            case "gpt-4o" -> 10.00;
            case "gpt-4o-mini" -> 0.60;
            case "o1" -> 60.00;
            case "o3-mini" -> 4.40;
            case "claude-opus-4" -> 75.00;
            case "claude-sonnet-4" -> 15.00;
            case "claude-haiku-4" -> 4.00;
            case "gemini-1.5-pro" -> 10.50;
            case "gemini-1.5-flash" -> 0.30;
            default -> 3.00;
        };
    }

    private static double getCacheReadPrice(String model) {
        // Cached tokens are typically 50% of input price
        return getInputPrice(model) * 0.5;
    }

    private static double getEmbeddingPrice(String model) {
        return switch (model) {
            case "text-embedding-3-small" -> 0.02;
            case "text-embedding-3-large" -> 0.13;
            default -> 0.10;
        };
    }

    // Getters
    public String getCustomerId() { return customerId; }
    public String getModelId() { return modelId; }
    public long getWindowStart() { return windowStart; }
    public long getWindowEnd() { return windowEnd; }
    public long getInputTokens() { return inputTokens; }
    public long getOutputTokens() { return outputTokens; }
    public long getCacheReadTokens() { return cacheReadTokens; }
    public long getCacheWriteTokens() { return cacheWriteTokens; }
    public long getReasoningTokens() { return reasoningTokens; }
    public long getEmbeddingTokens() { return embeddingTokens; }
    public long getTotalTokens() { return totalTokens; }
    public double getCostUsd() { return costMicro / 1_000_000.0; }
    public long getCostMicro() { return costMicro; }
    public long getEventCount() { return eventCount; }
    public long getTotalLatencyMs() { return totalLatencyMs; }

    public double getAvgLatencyMs() {
        return eventCount > 0 ? (double) totalLatencyMs / eventCount : 0;
    }

    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setModelId(String modelId) { this.modelId = modelId; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
}
