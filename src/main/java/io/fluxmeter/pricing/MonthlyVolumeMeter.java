package io.fluxmeter.pricing;

import io.fluxmeter.util.BillingPeriod;

/**
 * Keyed monthly volume meter for tier pricing (Flink ValueState backing logic).
 * Volume scope: customer_model; billing period: UTC calendar month.
 */
public final class MonthlyVolumeMeter implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private long monthlyTokens;
    private String billingPeriod;

    public MonthlyVolumeMeter() {}

    public static MonthlyVolumeMeter fromState(long monthlyTokens, String billingPeriod) {
        MonthlyVolumeMeter meter = new MonthlyVolumeMeter();
        meter.monthlyTokens = monthlyTokens;
        meter.billingPeriod = billingPeriod;
        return meter;
    }

    public long getMonthlyTokens() {
        return monthlyTokens;
    }

    public String getBillingPeriod() {
        return billingPeriod;
    }

    /** Tokens already billed this period before processing {@code eventTimestampMs}. */
    public long tokensBefore(long eventTimestampMs) {
        rollPeriodIfNeeded(eventTimestampMs);
        return monthlyTokens;
    }

    /** Record billable tokens after cost calculation. */
    public void advance(long eventTimestampMs, int totalTokens) {
        rollPeriodIfNeeded(eventTimestampMs);
        monthlyTokens += totalTokens;
    }

    private void rollPeriodIfNeeded(long eventTimestampMs) {
        String period = BillingPeriod.monthUtc(eventTimestampMs);
        if (billingPeriod == null || !billingPeriod.equals(period)) {
            billingPeriod = period;
            monthlyTokens = 0L;
        }
    }
}
