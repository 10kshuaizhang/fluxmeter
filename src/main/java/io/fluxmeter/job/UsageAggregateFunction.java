package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.pricing.MonthlyVolumeMeter;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * Window aggregator with keyed {@link MonthlyVolumeMeter} state for tier pricing.
 * State persists across windows per (tenant|customer|model) key.
 */
public class UsageAggregateFunction extends AbstractRichFunction
        implements AggregateFunction<TokenEvent, UsageAggregate, UsageAggregate> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Long> monthlyVolumeState;
    private transient ValueState<String> billingPeriodState;

    @Override
    public void open(Configuration parameters) {
        monthlyVolumeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("monthlyVolumeTokens", Long.class));
        billingPeriodState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("billingPeriodMonth", String.class));
    }

    @Override
    public UsageAggregate createAccumulator() {
        return new UsageAggregate();
    }

    @Override
    public UsageAggregate add(TokenEvent event, UsageAggregate acc) {
        try {
            MonthlyVolumeMeter meter = loadMeter();
            long monthlyBefore = meter.tokensBefore(event.getTimestamp());
            acc.addEvent(event, monthlyBefore);
            meter.advance(event.getTimestamp(), event.getTotalTokens());
            saveMeter(meter);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update monthly volume state", e);
        }
        return acc;
    }

    @Override
    public UsageAggregate getResult(UsageAggregate acc) {
        return acc;
    }

    @Override
    public UsageAggregate merge(UsageAggregate a, UsageAggregate b) {
        // ponytail: volume state lives in Flink keyed state, not in the window acc;
        // merge only combines partial window aggregates (cost already tier-aware per add).
        return a.merge(b);
    }

    private MonthlyVolumeMeter loadMeter() throws Exception {
        Long vol = monthlyVolumeState.value();
        String period = billingPeriodState.value();
        if (period == null && vol == null) {
            return new MonthlyVolumeMeter();
        }
        return MonthlyVolumeMeter.fromState(vol != null ? vol : 0L, period);
    }

    private void saveMeter(MonthlyVolumeMeter meter) throws Exception {
        monthlyVolumeState.update(meter.getMonthlyTokens());
        billingPeriodState.update(meter.getBillingPeriod());
    }
}
