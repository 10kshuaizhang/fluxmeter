package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.model.SpanAggregate;
import io.fluxmeter.sink.BudgetEnforcerSink;
import io.fluxmeter.sink.RedisSink;
import io.fluxmeter.sink.SpanSink;
import io.fluxmeter.sink.EventProjectionSink;

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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
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
        String watermarkTopic = System.getenv().getOrDefault("WATERMARK_TOPIC", "metering-watermarks");
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        long windowSeconds = Long.parseLong(System.getenv().getOrDefault("WINDOW_SECONDS", "10"));
        String checkpointDir = System.getenv().getOrDefault("CHECKPOINT_DIR", "");
        String pricingFile = System.getenv().getOrDefault("PRICING_FILE", "config/pricing.json");
        if (!pricingFile.isEmpty()) {
            System.setProperty("PRICING_FILE", pricingFile);
            io.fluxmeter.pricing.PricingCatalog.reload(
                    io.fluxmeter.pricing.PricingCatalog.loadDefault());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 5000));
        env.getConfig().setAutoWatermarkInterval(1000);

        // --- Exactly-once: enable checkpointing ---
        if (!checkpointDir.isEmpty()) {
            env.enableCheckpointing(30_000); // 30s checkpoint interval
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
            env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000L);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        // Kafka source: use committed offsets on restart (exactly-once with checkpointing)
        KafkaSource<TokenEvent> source = KafkaSource.<TokenEvent>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(kafkaTopic, watermarkTopic)
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
                .withIdleness(sourceIdleness());

        DataStream<TokenEvent> sourceEvents = env
                .fromSource(source, watermarkStrategy, "Kafka Token Events");

        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "token-events-dlq");
        sourceEvents
                .filter(TokenEvent::isMalformedEnvelope)
                .addSink(new MalformedEnvelopeSink(kafkaBrokers, dlqTopic));

        // Valid trusted envelopes only. Readiness probes traverse Kafka and this source,
        // then write a causal acknowledgement without entering billing.
        DataStream<TokenEvent> trustedEvents = sourceEvents
                .filter(event -> !event.isMalformedEnvelope()
                        && event.getCustomerId() != null && event.getModelId() != null);
        trustedEvents
                .filter(TokenUsageAggregator::isHeartbeat)
                .addSink(new ConsumerHeartbeatSink(redisHost, redisPort));
        DataStream<TokenEvent> validatedEvents = trustedEvents
                .filter(event -> !isHeartbeat(event));

        DataStream<TokenEvent> events = validatedEvents
                .keyBy(TokenEvent::getEventId)
                .process(new EventDeduplicator());

        SingleOutputStreamOperator<TokenEvent> onTimeEvents = events
                .process(new LateEventRouter(windowSeconds));

        // Windowed aggregation. Late events (after watermark passes window end)
        // go to DLQ for reprocessing. No allowedLateness — avoids window re-fire
        // which conflicts with SET NX idempotency (second fire gets blocked,
        // losing the late data contribution).
        DataStream<TokenEvent> stampedEvents = onTimeEvents
                .keyBy(TokenEvent::getAggregationKey)
                .process(new MonthlyVolumeStampFunction());

        stampedEvents.addSink(new EventProjectionSink(redisHost, redisPort));

        SingleOutputStreamOperator<UsageAggregate> aggregates = stampedEvents
                .keyBy(TokenEvent::getAggregationKey)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .sideOutputLateData(LATE_EVENTS)
                .aggregate(new UsageAggregateFunction(), new WindowMetadataFunction());

        // Late events routed to DLQ Kafka topic for reprocessing
        DataStream<TokenEvent> lateEvents = onTimeEvents.getSideOutput(LATE_EVENTS)
                .union(aggregates.getSideOutput(LATE_EVENTS));
        lateEvents.addSink(new LateEventSink(kafkaBrokers, dlqTopic));

        // --- Span attribution: aggregate cost per agent run (parentSpanId) ---
        // Session window closes after 60s of inactivity.
        // Note: long-running agents (calling every <60s) will keep the window open.
        // SpanSink uses SET (overwrite) so even if the window fires multiple times
        // (via Flink's internal session merge), correctness is maintained.
        // For memory safety, configure Flink state TTL at cluster level.
        DataStream<SpanAggregate> spanAggregates = onTimeEvents
                .filter(event -> event.getParentSpanId() != null && !event.getParentSpanId().isEmpty())
                .keyBy(TokenEvent::getParentSpanId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(60)))
                .aggregate(new SpanAggregateFunction());

        spanAggregates.addSink(new SpanSink(redisHost, redisPort));

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

    /** Streaming heartbeats carry cumulative tokens for dashboards; exclude from billing. */
    static boolean isHeartbeat(TokenEvent event) {
        if (event.getMetadata() == null) {
            return false;
        }
        return "true".equals(event.getMetadata().get("_heartbeat"));
    }

    static Duration sourceIdleness() {
        return Duration.ofSeconds(Long.parseLong(
                System.getenv().getOrDefault("SOURCE_IDLE_SECONDS", "15")));
    }

    /** Acknowledges a readiness probe only after it traverses the Kafka consumer. */
    public static class ConsumerHeartbeatSink
            extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<TokenEvent> {
        private final String host;
        private final int port;
        private transient redis.clients.jedis.JedisPool pool;

        public ConsumerHeartbeatSink(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            pool = io.fluxmeter.sink.RedisConnections.createPool(host, port, 2);
        }

        @Override
        public void invoke(TokenEvent event, Context context) {
            try (redis.clients.jedis.Jedis jedis = pool.getResource()) {
                jedis.setex("flink:probe:" + event.getEventId(), 60, String.valueOf(System.currentTimeMillis()));
                jedis.set("flink:heartbeat:last_processed_at", String.valueOf(System.currentTimeMillis()));
            }
        }

        @Override
        public void close() {
            if (pool != null) pool.close();
        }
    }

    /** Routes already-late events before any irreversible Redis projection. */
    public static class LateEventRouter extends ProcessFunction<TokenEvent, TokenEvent> {
        private final long windowMillis;

        public LateEventRouter(long windowSeconds) {
            this.windowMillis = windowSeconds * 1000;
        }

        static boolean isLate(long eventTimestamp, long watermark, long windowMillis) {
            if (watermark == Long.MIN_VALUE) return false;
            long windowEnd = (Math.floorDiv(eventTimestamp, windowMillis) + 1) * windowMillis;
            return windowEnd <= watermark;
        }

        @Override
        public void processElement(TokenEvent event, Context context, Collector<TokenEvent> out) {
            if (!event.isAuthorizedReplay()
                    && isLate(event.getTimestamp(), context.timerService().currentWatermark(), windowMillis)) {
                context.output(LATE_EVENTS, event);
            } else {
                out.collect(event);
            }
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
            applyKeyAndWindow(key, agg, context.window().getStart(), context.window().getEnd());
            out.collect(agg);
        }

        /** Package-visible for unit tests — key shape: tenant|customer|model or customer|model. */
        static void applyKeyAndWindow(String key, UsageAggregate agg, long windowStart, long windowEnd) {
            String[] parts = key.split("\\|", -1);
            if (parts.length >= 3) {
                agg.setTenantId(parts[0]);
                agg.setCustomerId(parts[1]);
                agg.setModelId(parts[2]);
            } else if (parts.length == 2) {
                agg.setCustomerId(parts[0]);
                agg.setModelId(parts[1]);
            } else {
                agg.setCustomerId(key);
                agg.setModelId("unknown");
            }
            agg.setWindowStart(windowStart);
            agg.setWindowEnd(windowEnd);
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
            // ponytail: span key is parentSpanId; tier volume is customer|model scoped — flat for spans
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
            props.put("acks", "all");
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
            mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        }

        @Override
        public void invoke(TokenEvent event, Context context) {
            try {
                java.util.Map<String, Object> auth = new java.util.HashMap<>();
                auth.put("tenantId", event.getTenantId());
                auth.put("customerId", event.getCustomerId());
                auth.put("apiKeyId", event.getApiKeyId());
                java.util.Map<String, Object> receipt = new java.util.HashMap<>();
                receipt.put("receivedAt", System.currentTimeMillis());
                receipt.put("traceId", event.getIngestTraceId());
                java.util.Map<String, Object> envelope = new java.util.HashMap<>();
                envelope.put("envelopeVersion", 1);
                envelope.put("source", "operator");
                envelope.put("payload", event);
                envelope.put("auth", auth);
                envelope.put("receipt", receipt);
                envelope.put("replay", java.util.Map.of("authorized", true, "reason", "late-event-dlq"));
                byte[] value = mapper.writeValueAsBytes(envelope);
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
                JsonNode root = mapper.readTree(bytes);
                if (root.path("envelopeVersion").asInt(-1) != 1
                        || !root.path("payload").isObject()
                        || !root.path("auth").isObject()
                        || !root.path("receipt").isObject()) {
                    return malformed(bytes);
                }

                JsonNode auth = root.path("auth");
                String trustedCustomer = textOrNull(auth, "customerId");
                if (trustedCustomer == null || trustedCustomer.isBlank()) {
                    return malformed(bytes);
                }

                TokenEvent event = mapper.treeToValue(root.path("payload"), TokenEvent.class);
                if (event.getEventId() == null || event.getEventId().isBlank()
                        || event.getModelId() == null || event.getModelId().isBlank()) {
                    return malformed(bytes);
                }
                event.setCustomerId(trustedCustomer);
                event.setTenantId(textOrNull(auth, "tenantId"));
                event.setApiKeyId(textOrNull(auth, "apiKeyId"));
                event.setIngestSource(textOrNull(root, "source"));
                event.setReceivedAt(root.path("receipt").path("receivedAt").asLong(0));
                event.setIngestTraceId(textOrNull(root.path("receipt"), "traceId"));
                event.setReservationId(textOrNull(root.path("reservation"), "reservationId"));
                event.setReservedUsd(root.path("reservation").path("reservedUsd").asDouble(0));
                boolean authorizedReplay = "operator".equals(event.getIngestSource())
                        && root.path("replay").path("authorized").asBoolean(false);
                event.setAuthorizedReplay(authorizedReplay);
                if (authorizedReplay && event.getReceivedAt() > 0) {
                    // Replayed historical events enter a fresh processing window. The original
                    // event timestamp remains present in the raw DLQ envelope for audit.
                    event.setTimestamp(event.getReceivedAt());
                }
                return event;
            } catch (Exception e) {
                return malformed(bytes);
            }
        }

        private static TokenEvent malformed(byte[] bytes) {
            TokenEvent event = new TokenEvent();
            event.setMalformedEnvelope(true);
            event.setRawEnvelope(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            event.setTimestamp(System.currentTimeMillis());
            return event;
        }

        private static String textOrNull(JsonNode node, String field) {
            JsonNode value = node.path(field);
            return value.isTextual() && !value.asText().isBlank() ? value.asText() : null;
        }
    }

    /** Persists unsupported or malformed envelopes for operator inspection. */
    public static class MalformedEnvelopeSink
            extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<TokenEvent> {
        private final String brokers;
        private final String topic;
        private transient org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer;

        public MalformedEnvelopeSink(String brokers, String topic) {
            this.brokers = brokers;
            this.topic = topic;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            java.util.Properties props = new java.util.Properties();
            props.put("bootstrap.servers", brokers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("acks", "all");
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        }

        @Override
        public void invoke(TokenEvent event, Context context) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                    topic,
                    "malformed",
                    event.getRawEnvelope().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @Override
        public void close() {
            if (producer != null) producer.close();
        }
    }

}
