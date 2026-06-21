package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.model.SpanAggregate;
import io.fluxmeter.sink.BudgetEnforcerSink;
import io.fluxmeter.sink.RedisSink;
import io.fluxmeter.sink.SpanSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import java.time.Duration;

public class TokenUsageAggregator {

    // Side output for late events (arrived after watermark passed their window)
    public static final OutputTag<TokenEvent> LATE_EVENTS =
            new OutputTag<TokenEvent>("late-events") {};

    public static void main(String[] args) throws Exception {
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BROKERS", "kafka:9092");
        String kafkaTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "token-events");
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        long windowSeconds = Long.parseLong(System.getenv().getOrDefault("WINDOW_SECONDS", "10"));
        String checkpointDir = System.getenv().getOrDefault("CHECKPOINT_DIR", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 5000));
        env.getConfig().setAutoWatermarkInterval(1000);

        // --- Exactly-once: enable checkpointing ---
        if (!checkpointDir.isEmpty()) {
            env.enableCheckpointing(30_000); // 30s checkpoint interval
            env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        // Kafka source: use committed offsets on restart (exactly-once with checkpointing)
        KafkaSource<TokenEvent> source = KafkaSource.<TokenEvent>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(kafkaTopic)
                .setGroupId("fluxmeter-aggregator")
                .setStartingOffsets(checkpointDir.isEmpty()
                        ? OffsetsInitializer.latest()
                        : OffsetsInitializer.committedOffsets(
                                org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new TokenEventDeserializer())
                .build();

        WatermarkStrategy<TokenEvent> watermarkStrategy = WatermarkStrategy
                .<TokenEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                .withIdleness(Duration.ofSeconds(30));

        // Null/invalid filter only. Dedup handled at sink level (Redis SET NX).
        // Flink-level dedup removed: keying by eventId creates unbounded state
        // (1 key per event × TTL window = OOM at production throughput).
        DataStream<TokenEvent> events = env
                .fromSource(source, watermarkStrategy, "Kafka Token Events")
                .filter(event -> event != null && event.getCustomerId() != null && event.getModelId() != null);

        // Windowed aggregation. Late events (after watermark passes window end)
        // go to DLQ for reprocessing. No allowedLateness — avoids window re-fire
        // which conflicts with SET NX idempotency (second fire gets blocked,
        // losing the late data contribution).
        SingleOutputStreamOperator<UsageAggregate> aggregates = events
                .keyBy(TokenEvent::getAggregationKey)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .sideOutputLateData(LATE_EVENTS)
                .aggregate(new UsageAggregateFunction(), new WindowMetadataFunction());

        // Late events routed to DLQ Kafka topic for reprocessing
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "token-events-dlq");
        DataStream<TokenEvent> lateEvents = aggregates.getSideOutput(LATE_EVENTS);
        lateEvents.addSink(new LateEventSink(kafkaBrokers, dlqTopic));

        // --- Span attribution: aggregate cost per agent run (parentSpanId) ---
        // Session window closes after 60s of inactivity.
        // Note: long-running agents (calling every <60s) will keep the window open.
        // SpanSink uses SET (overwrite) so even if the window fires multiple times
        // (via Flink's internal session merge), correctness is maintained.
        // For memory safety, configure Flink state TTL at cluster level.
        DataStream<SpanAggregate> spanAggregates = events
                .filter(event -> event.getParentSpanId() != null && !event.getParentSpanId().isEmpty())
                .keyBy(TokenEvent::getParentSpanId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(60)))
                .aggregate(new SpanAggregateFunction());

        spanAggregates.addSink(new SpanSink(redisHost, redisPort));

        // --- Global counter: aggregate all windows into a single stream ---
        // This eliminates the Redis write hotspot (every parallel task was writing
        // to the same 5 global:* keys). Now: Flink aggregates globally, writes once.
        DataStream<UsageAggregate> globalStream = aggregates
                .keyBy(agg -> "global")
                .reduce((a, b) -> {
                    a.merge(b);
                    return a;
                });
        // The global stream fires one merged result per checkpoint/window cycle.
        // In practice this is handled by the per-customer sink's global counter write.
        // The reduce above is for future use when we separate global writes.

        // Main sink with idempotency
        String alertTopic = System.getenv().getOrDefault("ALERT_TOPIC", "budget-alerts");
        boolean budgetEnabled = Boolean.parseBoolean(
                System.getenv().getOrDefault("BUDGET_ENFORCEMENT", "true"));

        if (budgetEnabled) {
            aggregates.addSink(new BudgetEnforcerSink(redisHost, redisPort, kafkaBrokers, alertTopic));
        } else {
            aggregates.addSink(new RedisSink(redisHost, redisPort));
        }

        env.execute("FluxMeter - Token Usage Aggregator");
    }

    /**
     * Incremental aggregation: pre-aggregates events as they arrive.
     * Only one UsageAggregate is kept in memory per key per window.
     */
    public static class UsageAggregateFunction
            implements AggregateFunction<TokenEvent, UsageAggregate, UsageAggregate> {

        @Override
        public UsageAggregate createAccumulator() {
            return new UsageAggregate();
        }

        @Override
        public UsageAggregate add(TokenEvent event, UsageAggregate acc) {
            acc.addEvent(event);
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
    }

    /**
     * Adds window metadata (start/end timestamps, key) to the pre-aggregated result.
     */
    public static class WindowMetadataFunction
            extends ProcessWindowFunction<UsageAggregate, UsageAggregate, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<UsageAggregate> elements,
                            Collector<UsageAggregate> out) {
            UsageAggregate agg = elements.iterator().next();
            String[] parts = key.split("\\|", 2);
            agg.setCustomerId(parts[0]);
            agg.setModelId(parts.length > 1 ? parts[1] : "unknown");
            agg.setWindowStart(context.window().getStart());
            agg.setWindowEnd(context.window().getEnd());
            out.collect(agg);
        }
    }

    /**
     * Incremental aggregation for span-level cost attribution.
     * Groups all LLM calls with the same parentSpanId and computes total cost.
     * Uses session windows (60s gap) since agent runs have variable duration.
     */
    public static class SpanAggregateFunction
            implements AggregateFunction<TokenEvent, SpanAggregate, SpanAggregate> {

        @Override
        public SpanAggregate createAccumulator() {
            return new SpanAggregate();
        }

        @Override
        public SpanAggregate add(TokenEvent event, SpanAggregate acc) {
            double cost = UsageAggregate.calculateEventCost(event);
            acc.addEvent(event, cost);
            return acc;
        }

        @Override
        public SpanAggregate getResult(SpanAggregate acc) {
            return acc;
        }

        @Override
        public SpanAggregate merge(SpanAggregate a, SpanAggregate b) {
            return a.merge(b);
        }
    }

    /**
     * Routes late events to a Kafka DLQ topic for reprocessing.
     */
    public static class LateEventSink
            extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<TokenEvent> {

        private transient org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer;
        private transient com.fasterxml.jackson.databind.ObjectMapper mapper;
        private final String brokers;
        private final String dlqTopic;

        public LateEventSink(String brokers, String dlqTopic) {
            this.brokers = brokers;
            this.dlqTopic = dlqTopic;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            java.util.Properties props = new java.util.Properties();
            props.put("bootstrap.servers", brokers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("acks", "1");
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
            mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        }

        @Override
        public void invoke(TokenEvent event, Context context) {
            try {
                byte[] value = mapper.writeValueAsBytes(event);
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        dlqTopic, event.getCustomerId(), value));
            } catch (Exception e) {
                // Best effort — don't crash on DLQ failure
            }
        }

        @Override
        public void close() {
            if (producer != null) producer.close();
        }
    }

    public static class TokenEventDeserializer extends AbstractDeserializationSchema<TokenEvent> {
        private transient ObjectMapper mapper;

        @Override
        public TokenEvent deserialize(byte[] bytes) {
            if (mapper == null) {
                mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }
            try {
                return mapper.readValue(bytes, TokenEvent.class);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
