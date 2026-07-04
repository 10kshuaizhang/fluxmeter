package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.pricing.MonthlyVolumeMeter;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Keyed pre-window step: maintain monthly volume meter and stamp each event
 * with {@link UsageAggregateFunction#MONTHLY_VOLUME_BEFORE_KEY} for tier pricing.
 */
public class MonthlyVolumeStampFunction extends KeyedProcessFunction<String, TokenEvent, TokenEvent> {

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
    public void processElement(TokenEvent event, Context ctx, Collector<TokenEvent> out) throws Exception {
        MonthlyVolumeMeter meter = loadMeter();
        long monthlyBefore = meter.tokensBefore(event.getTimestamp());
        meter.advance(event.getTimestamp(), event.getTotalTokens());
        saveMeter(meter);

        Map<String, String> md = event.getMetadata();
        if (md == null) {
            md = new HashMap<>();
            event.setMetadata(md);
        }
        md.put(UsageAggregateFunction.MONTHLY_VOLUME_BEFORE_KEY, String.valueOf(monthlyBefore));
        out.collect(event);
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
