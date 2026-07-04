package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;

/**
 * Window aggregator — tier volume is stamped on each event by {@link MonthlyVolumeStampFunction}
 * before windowing (Flink disallows RichFunction on aggregate+ProcessWindowFunction).
 */
public class UsageAggregateFunction
        implements AggregateFunction<TokenEvent, UsageAggregate, UsageAggregate> {

    private static final long serialVersionUID = 1L;
    static final String MONTHLY_VOLUME_BEFORE_KEY = "_monthlyVolumeBefore";

    @Override
    public UsageAggregate createAccumulator() {
        return new UsageAggregate();
    }

    @Override
    public UsageAggregate add(TokenEvent event, UsageAggregate acc) {
        acc.addEvent(event, readMonthlyBefore(event));
        return acc;
    }

    @Override
    public UsageAggregate getResult(UsageAggregate acc) {
        return acc;
    }

    @Override
    public UsageAggregate merge(UsageAggregate a, UsageAggregate b) {
        return a.merge(b);
    }

    static long readMonthlyBefore(TokenEvent event) {
        Map<String, String> md = event.getMetadata();
        if (md == null || !md.containsKey(MONTHLY_VOLUME_BEFORE_KEY)) {
            return 0L;
        }
        return Long.parseLong(md.get(MONTHLY_VOLUME_BEFORE_KEY));
    }
}
