package io.fluxmeter.generator;

import io.fluxmeter.model.TokenEvent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LoadGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong eventCounter = new AtomicLong(0);
    private static final AtomicLong byteCounter = new AtomicLong(0);

    // Realistic model distribution (weighted toward popular models)
    private static final String[][] PROVIDER_MODELS = {
            {"openai", "gpt-4o"},
            {"openai", "gpt-4o-mini"},
            {"openai", "o1"},
            {"openai", "o3-mini"},
            {"anthropic", "claude-opus-4"},
            {"anthropic", "claude-sonnet-4"},
            {"anthropic", "claude-haiku-4"},
            {"google", "gemini-1.5-pro"},
            {"google", "gemini-1.5-flash"},
    };

    // Weights: gpt-4o-mini and claude-haiku are most popular (cheap models)
    private static final int[] MODEL_WEIGHTS = {15, 30, 3, 5, 2, 12, 20, 5, 8};

    private static final String[] ENVIRONMENTS = {"production", "staging"};

    public static void main(String[] args) throws Exception {
        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "token-events");
        int numCustomers = Integer.parseInt(System.getenv().getOrDefault("NUM_CUSTOMERS", "10000"));
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "4"));
        int batchSize = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "16384"));
        int targetEps = Integer.parseInt(System.getenv().getOrDefault("TARGET_EPS", "1000000"));

        System.out.printf("FluxMeter Load Generator%n");
        System.out.printf("  Brokers: %s%n", brokers);
        System.out.printf("  Topic: %s%n", topic);
        System.out.printf("  Customers: %d%n", numCustomers);
        System.out.printf("  Threads: %d%n", numThreads);
        System.out.printf("  Target EPS: %d%n", targetEps);
        System.out.println("  Starting...");

        // Stats reporter thread
        Thread statsThread = new Thread(() -> {
            long lastCount = 0;
            long lastBytes = 0;
            long lastTime = System.currentTimeMillis();
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
                long now = System.currentTimeMillis();
                long count = eventCounter.get();
                long bytes = byteCounter.get();
                double elapsed = (now - lastTime) / 1000.0;
                double eps = (count - lastCount) / elapsed;
                double mbps = ((bytes - lastBytes) / elapsed) / (1024 * 1024);
                System.out.printf("  [STATS] %.0f events/sec | %.1f MB/sec | %,d total%n", eps, mbps, count);
                lastCount = count;
                lastBytes = bytes;
                lastTime = now;
            }
        }, "stats-reporter");
        statsThread.setDaemon(true);
        statsThread.start();

        // Producer threads
        int epsPerThread = targetEps / numThreads;
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    runProducer(brokers, topic, numCustomers, batchSize, epsPerThread, threadId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "producer-" + t);
            threads[t].start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private static void runProducer(String brokers, String topic, int numCustomers,
                                     int batchSize, int targetEps, int threadId) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 128 * 1024 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        Random random = new Random(threadId);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        // Precompute cumulative weights for model selection
        int totalWeight = 0;
        int[] cumWeights = new int[MODEL_WEIGHTS.length];
        for (int i = 0; i < MODEL_WEIGHTS.length; i++) {
            totalWeight += MODEL_WEIGHTS[i];
            cumWeights[i] = totalWeight;
        }

        long intervalNanos = 1_000_000_000L / targetEps;
        long nextSendTime = System.nanoTime();

        while (true) {
            TokenEvent event = generateEvent(random, numCustomers, cumWeights, totalWeight);
            byte[] value = MAPPER.writeValueAsBytes(event);

            producer.send(new ProducerRecord<>(topic, event.getCustomerId(), value));
            eventCounter.incrementAndGet();
            byteCounter.addAndGet(value.length);

            nextSendTime += intervalNanos;
            long sleepNanos = nextSendTime - System.nanoTime();
            if (sleepNanos > 1_000_000) {
                Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
            }
        }
    }

    private static TokenEvent generateEvent(Random random, int numCustomers,
                                             int[] cumWeights, int totalWeight) {
        // Select model based on weights
        int roll = random.nextInt(totalWeight);
        int modelIdx = 0;
        for (int i = 0; i < cumWeights.length; i++) {
            if (roll < cumWeights[i]) {
                modelIdx = i;
                break;
            }
        }

        String provider = PROVIDER_MODELS[modelIdx][0];
        String modelId = PROVIDER_MODELS[modelIdx][1];
        String customerId = "cust_" + random.nextInt(numCustomers);

        TokenEvent event = new TokenEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setCustomerId(customerId);
        event.setProvider(provider);
        event.setModelId(modelId);
        event.setTimestamp(System.currentTimeMillis());
        event.setEnvironment(ENVIRONMENTS[random.nextInt(100) < 95 ? 0 : 1]);

        // Generate realistic token counts based on model type
        switch (modelId) {
            case "o1", "o3-mini" -> {
                // Reasoning models: moderate input, reasoning + output
                event.setInputTokens(200 + random.nextInt(3000));
                event.setOutputTokens(100 + random.nextInt(2000));
                event.setReasoningTokens(500 + random.nextInt(10000));
            }
            case "gpt-4o", "claude-opus-4", "claude-sonnet-4", "gemini-1.5-pro" -> {
                // Large models: typical chat completion
                event.setInputTokens(100 + random.nextInt(4000));
                event.setOutputTokens(50 + random.nextInt(2000));
                // 20% chance of cache hit
                if (random.nextInt(100) < 20) {
                    event.setCacheReadTokens(50 + random.nextInt(500));
                }
            }
            case "gpt-4o-mini", "claude-haiku-4", "gemini-1.5-flash" -> {
                // Fast/cheap models: shorter exchanges, higher volume
                event.setInputTokens(50 + random.nextInt(1000));
                event.setOutputTokens(20 + random.nextInt(500));
            }
            default -> {
                event.setInputTokens(100 + random.nextInt(2000));
                event.setOutputTokens(50 + random.nextInt(1000));
            }
        }

        // Simulate provider response latency
        event.setLatencyMs(50 + random.nextInt(2000));

        // 30% of events have a session ID (multi-turn conversations)
        if (random.nextInt(100) < 30) {
            event.setSessionId("sess_" + customerId + "_" + random.nextInt(100));
        }

        // 10% have a span ID (agent/tool calls)
        if (random.nextInt(100) < 10) {
            event.setSpanId("span_" + UUID.randomUUID().toString().substring(0, 8));
        }

        return event;
    }
}
