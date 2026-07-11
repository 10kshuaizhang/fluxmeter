package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies tumbling windows with sideOutputLateData and no allowedLateness:
 * in-window events fire the main output; events after the watermark passed the
 * window end go to the late side output.
 */
class LateDataSideOutputTest {

    private static final OutputTag<TokenEvent> LATE =
            new OutputTag<TokenEvent>("late-events-test") {};

    @BeforeEach
    void clearSinks() {
        MainCollectSink.values.clear();
        LateCollectSink.values.clear();
    }

    @Test
    void lateEventsGoToSideOutputNotMainWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);

        DataStream<TokenEvent> events = env
                .addSource(new OrderedEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TokenEvent>forGenerator(ctx -> new CloserWatermarkGenerator())
                                .withTimestampAssigner((e, ts) -> e.getTimestamp()));

        SingleOutputStreamOperator<UsageAggregate> aggregates = events
                .keyBy(TokenEvent::getAggregationKey)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(LATE)
                .aggregate(new UsageAggregateFunction(), new TokenUsageAggregator.WindowMetadataFunction());

        aggregates.addSink(new MainCollectSink());
        aggregates.getSideOutput(LATE).addSink(new LateCollectSink());

        env.execute("late-data-side-output-test");

        long mainInput = MainCollectSink.values.stream().mapToLong(UsageAggregate::getInputTokens).sum();
        assertTrue(MainCollectSink.values.size() >= 1,
                "expected window fire, lateSize=" + LateCollectSink.values.size());
        assertTrue(mainInput >= 100 && mainInput < 100 + 999,
                "on-time only in main, got mainInput=" + mainInput
                        + " lateSize=" + LateCollectSink.values.size());
        assertEquals(1, LateCollectSink.values.size());
        assertEquals("e3", LateCollectSink.values.get(0).getEventId());
        assertEquals(999, LateCollectSink.values.get(0).getInputTokens());
    }

    /** Advances watermark past window end only when the closer event (e2) arrives. */
    private static final class CloserWatermarkGenerator
            implements WatermarkGenerator<TokenEvent>, Serializable {
        @Override
        public void onEvent(TokenEvent event, long eventTimestamp, WatermarkOutput output) {
            if ("e2".equals(event.getEventId())) {
                output.emitWatermark(new Watermark(10_000));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // event-driven only
        }
    }

    @SuppressWarnings("deprecation")
    private static class OrderedEventSource implements SourceFunction<TokenEvent> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<TokenEvent> ctx) throws Exception {
            emit(ctx, event("e1", 1_000L, 100));
            Thread.sleep(20);
            emit(ctx, event("e2", 10_001L, 1));
            Thread.sleep(20);
            emit(ctx, event("e3", 5_000L, 999));
        }

        private void emit(SourceContext<TokenEvent> ctx, TokenEvent e) {
            if (!running) {
                return;
            }
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(e);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static TokenEvent event(String id, long ts, int input) {
        TokenEvent e = new TokenEvent();
        e.setEventId(id);
        e.setCustomerId("cust_late");
        e.setModelId("gpt-4o-mini");
        e.setProvider("openai");
        e.setTimestamp(ts);
        e.setInputTokens(input);
        e.setOutputTokens(0);
        return e;
    }

    private static class MainCollectSink implements SinkFunction<UsageAggregate> {
        static final List<UsageAggregate> values =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(UsageAggregate value, Context context) {
            values.add(value);
        }
    }

    private static class LateCollectSink implements SinkFunction<TokenEvent> {
        static final List<TokenEvent> values = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(TokenEvent value, Context context) {
            values.add(value);
        }
    }
}
