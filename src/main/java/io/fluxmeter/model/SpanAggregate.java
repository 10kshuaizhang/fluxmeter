package io.fluxmeter.model;

import java.io.Serializable;

/**
 * Aggregated cost and usage for an agent span (a group of related LLM calls).
 *
 * An agent run (e.g. a tool-use loop) generates multiple LLM calls, each with its
 * own spanId. By setting parentSpanId on child calls, FluxMeter can attribute the
 * total cost of the entire agent run to the root span.
 */
public class SpanAggregate implements Serializable {
    private static final long serialVersionUID = 1L;

    private String spanId;          // The span being aggregated (parentSpanId from events)
    private String customerId;
    private long totalTokens;
    private long inputTokens;
    private long outputTokens;
    private long reasoningTokens;
    private double costUsd;
    private int callCount;          // Number of LLM calls in this span
    private long firstEventTime;    // Earliest event timestamp
    private long lastEventTime;     // Latest event timestamp

    public SpanAggregate() {
        this.firstEventTime = Long.MAX_VALUE;
        this.lastEventTime = 0;
    }

    public void addEvent(TokenEvent event, double eventCost) {
        this.customerId = event.getCustomerId();
        this.spanId = event.getParentSpanId();
        this.totalTokens += event.getTotalTokens();
        this.inputTokens += event.getInputTokens();
        this.outputTokens += event.getOutputTokens();
        this.reasoningTokens += event.getReasoningTokens();
        this.costUsd += eventCost;
        this.callCount++;

        if (event.getTimestamp() < this.firstEventTime) {
            this.firstEventTime = event.getTimestamp();
        }
        if (event.getTimestamp() > this.lastEventTime) {
            this.lastEventTime = event.getTimestamp();
        }
    }

    public SpanAggregate merge(SpanAggregate other) {
        this.totalTokens += other.totalTokens;
        this.inputTokens += other.inputTokens;
        this.outputTokens += other.outputTokens;
        this.reasoningTokens += other.reasoningTokens;
        this.costUsd += other.costUsd;
        this.callCount += other.callCount;
        if (other.firstEventTime < this.firstEventTime) {
            this.firstEventTime = other.firstEventTime;
        }
        if (other.lastEventTime > this.lastEventTime) {
            this.lastEventTime = other.lastEventTime;
        }
        return this;
    }

    public long getDurationMs() {
        return lastEventTime > firstEventTime ? lastEventTime - firstEventTime : 0;
    }

    // Getters
    public String getSpanId() { return spanId; }
    public String getCustomerId() { return customerId; }
    public long getTotalTokens() { return totalTokens; }
    public long getInputTokens() { return inputTokens; }
    public long getOutputTokens() { return outputTokens; }
    public long getReasoningTokens() { return reasoningTokens; }
    public double getCostUsd() { return costUsd; }
    public int getCallCount() { return callCount; }
    public long getFirstEventTime() { return firstEventTime; }
    public long getLastEventTime() { return lastEventTime; }

    public void setSpanId(String spanId) { this.spanId = spanId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
}
