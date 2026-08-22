package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Short-lived safety deduplication for the Redis→Kafka custody uncertainty window.
 *
 * If the same eventId arrives twice (SDK retry, Kafka redelivery), the second
 * one is dropped. Long-term (30 day) client retry identity belongs to HTTP
 * Custody; this state only covers acknowledgement/finalize crash windows.
 *
 * Key: eventId
 * State: boolean (seen or not)
 * TTL: EVENT_SAFETY_DEDUP_TTL_SECONDS (10 minutes by default)
 */
public class EventDeduplicator extends KeyedProcessFunction<String, TokenEvent, TokenEvent> {

    private transient ValueState<Boolean> seenState;
    private final long ttlSeconds;

    public EventDeduplicator() {
        this(Long.parseLong(System.getenv().getOrDefault(
                "EVENT_SAFETY_DEDUP_TTL_SECONDS", "600")));
    }

    EventDeduplicator(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(ttlSeconds))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TokenEvent event, Context ctx, Collector<TokenEvent> out) throws Exception {
        if (event.getEventId() == null) {
            out.collect(event); // No eventId — can't dedup, pass through
            return;
        }

        Boolean seen = seenState.value();
        if (seen != null) {
            return; // Duplicate — drop silently
        }

        seenState.update(true);
        out.collect(event);
    }
}
