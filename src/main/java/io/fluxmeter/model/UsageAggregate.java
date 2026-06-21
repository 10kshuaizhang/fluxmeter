package io.fluxmeter.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

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
    private Set<String> seenEventIds; // Per-window dedup (bounded by window event count)

    public UsageAggregate() {}

    public UsageAggregate(String customerId, String modelId, long windowStart, long windowEnd) {
        this.customerId = customerId;
        this.modelId = modelId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public void addEvent(TokenEvent event) {
        if (event.getEventId() != null) {
            if (seenEventIds == null) {
                seenEventIds = new HashSet<>();
            }
            if (!seenEventIds.add(event.getEventId())) {
                this.deduplicatedCount++;
                return;
            }
        }

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
        if (other.seenEventIds != null) {
            if (seenEventIds == null) {
                seenEventIds = new HashSet<>(other.seenEventIds);
            } else {
                seenEventIds.addAll(other.seenEventIds);
            }
        }
        this.deduplicatedCount += other.deduplicatedCount;

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
        String model = normalizeModelId(event.getModelId());
        long cost = 0;

        // price_per_M is $/million tokens; cost_micros = tokens * price_per_M
        // e.g. 1000 tokens at $0.15/M → 1000 * 0.15 = 150 microdollars ($0.00015)
        cost += Math.round(event.getInputTokens() * getInputPrice(model));
        cost += Math.round(event.getOutputTokens() * getOutputPrice(model));
        cost += Math.round(event.getCacheReadTokens() * getCacheReadPrice(model));
        cost += Math.round(event.getReasoningTokens() * getOutputPrice(model));
        cost += Math.round(event.getCacheWriteTokens() * getInputPrice(model));
        cost += Math.round(event.getEmbeddingTokens() * getEmbeddingPrice(model));

        return cost;
    }

    /** Map versioned provider model IDs (e.g. gpt-4o-2024-08-06) to canonical pricing keys. */
    static String normalizeModelId(String model) {
        if (model == null || model.isEmpty()) {
            return "unknown";
        }
        if (KNOWN_MODELS.contains(model)) {
            return model;
        }
        for (String known : KNOWN_MODELS_BY_PREFIX) {
            if (model.startsWith(known)) {
                return known;
            }
        }
        return model;
    }

    private static final Set<String> KNOWN_MODELS = Set.of(
            "gpt-4o", "gpt-4o-mini", "o1", "o3-mini",
            "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
            "gemini-1.5-pro", "gemini-1.5-flash",
            "text-embedding-3-small", "text-embedding-3-large"
    );

    // Longest prefix first so "gpt-4o-mini" matches before "gpt-4o"
    private static final List<String> KNOWN_MODELS_BY_PREFIX = List.of(
            "gpt-4o-mini", "gpt-4o", "o3-mini", "o1",
            "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
            "gemini-1.5-pro", "gemini-1.5-flash",
            "text-embedding-3-large", "text-embedding-3-small"
    );

    /**
     * Cost in USD (double). Convenience wrapper for backward compatibility.
     * Internally uses microdollars then converts.
     */
    public static double calculateEventCost(TokenEvent event) {
        return calculateEventCostMicro(event) / 1_000_000.0;
    }

    private static double getInputPrice(String model) {
        model = normalizeModelId(model);
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
        model = normalizeModelId(model);
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
        model = normalizeModelId(model);
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
