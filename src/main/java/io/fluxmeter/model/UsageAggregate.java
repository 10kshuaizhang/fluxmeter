package io.fluxmeter.model;

import io.fluxmeter.pricing.PricingCatalog;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Aggregated token usage for a (customer, model) key within a time window.
 * Accumulates all token categories and computes cost via PricingCatalog.
 */
public class UsageAggregate implements Serializable {
    private static final long serialVersionUID = 3L;

    private String customerId;
    private String modelId;
    private long windowStart;
    private long windowEnd;

    private long inputTokens;
    private long outputTokens;
    private long cacheReadTokens;
    private long cacheWriteTokens;
    private long reasoningTokens;
    private long embeddingTokens;
    private long totalTokens;

    private long costMicro;
    private long eventCount;
    private long totalLatencyMs;
    private long deduplicatedCount;
    private Set<String> seenEventIds;

    public UsageAggregate() {}

    public UsageAggregate(String customerId, String modelId, long windowStart, long windowEnd) {
        this.customerId = customerId;
        this.modelId = modelId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public void addEvent(TokenEvent event) {
        addEvent(event, 0L);
    }

    public void addEvent(TokenEvent event, long monthlyTokensBefore) {
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

        this.costMicro += PricingCatalog.get().calculateEventCostMicro(event, monthlyTokensBefore);
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

    public static long calculateEventCostMicro(TokenEvent event) {
        return PricingCatalog.get().calculateEventCostMicro(event);
    }

    public static long calculateEventCostMicro(TokenEvent event, long monthlyTokensBefore) {
        return PricingCatalog.get().calculateEventCostMicro(event, monthlyTokensBefore);
    }

    public static double calculateEventCost(TokenEvent event) {
        return PricingCatalog.get().calculateEventCost(event);
    }

    public static String normalizeModelId(String model) {
        return PricingCatalog.get().normalizeModelId(model);
    }

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
