package io.fluxmeter.generator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

/** Open-loop HTTP custody benchmark that does not share Python's generator ceiling. */
public final class HttpLoadGenerator {
    private static final int MAX_RECORDED_LATENCY_MS = 120_000;

    private HttpLoadGenerator() {}

    public record Config(
            String url,
            int targetEps,
            Duration duration,
            Duration warmup,
            int concurrency,
            String apiKey,
            double minEps,
            long maxP50Millis,
            long maxP99Millis) {
        public Config {
            if (url == null || url.isBlank()) throw new IllegalArgumentException("url is required");
            if (targetEps < 1) throw new IllegalArgumentException("targetEps must be positive");
            if (duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException("duration must be positive");
            }
            if (warmup.isNegative()) throw new IllegalArgumentException("warmup must not be negative");
            if (concurrency < 1) throw new IllegalArgumentException("concurrency must be positive");
        }
    }

    public record Result(
            long offered,
            long completed,
            long accepted,
            long rejected,
            long transportErrors,
            long dropped,
            double achievedEps,
            long p50Millis,
            long p99Millis,
            long maxMillis) {
        public boolean meetsGates(Config config) {
            return achievedEps >= config.minEps()
                    && p50Millis <= config.maxP50Millis()
                    && p99Millis <= config.maxP99Millis()
                    && accepted == completed
                    && transportErrors == 0
                    && dropped == 0;
        }
    }

    public static Result run(Config config) throws InterruptedException {
        int executorThreads = Math.min(
                config.concurrency(), Math.max(8, Runtime.getRuntime().availableProcessors() * 4));
        ExecutorService executor = Executors.newFixedThreadPool(executorThreads);
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .executor(executor)
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
            String runId = UUID.randomUUID().toString();
            if (!config.warmup().isZero()) {
                runPhase(client, config, config.warmup(), runId + "-warmup", false);
            }
            return runPhase(client, config, config.duration(), runId, true).result();
        } finally {
            executor.shutdownNow();
        }
    }

    private static PhaseResult runPhase(
            HttpClient client,
            Config config,
            Duration duration,
            String runId,
            boolean record) throws InterruptedException {
        Semaphore slots = new Semaphore(config.concurrency());
        Stats stats = new Stats();
        long intervalNanos = Math.max(1, 1_000_000_000L / config.targetEps());
        long started = System.nanoTime();
        long deadline = started + duration.toNanos();
        long sequence = 0;

        for (long scheduled = started; scheduled < deadline; scheduled += intervalNanos) {
            waitUntil(scheduled);
            stats.offered.incrementAndGet();
            if (!slots.tryAcquire()) {
                stats.dropped.incrementAndGet();
                continue;
            }
            long requestStarted = System.nanoTime();
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(config.url()))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "fluxmeter-java-gate/1")
                    .POST(HttpRequest.BodyPublishers.ofString(payload(runId, sequence++)));
            if (config.apiKey() != null && !config.apiKey().isBlank()) {
                request.header("X-API-Key", config.apiKey());
            }
            client.sendAsync(request.build(), HttpResponse.BodyHandlers.discarding())
                    .whenComplete((response, error) -> {
                        try {
                            if (error != null) {
                                stats.transportErrors.incrementAndGet();
                            } else if (response.statusCode() == 202) {
                                stats.accepted.incrementAndGet();
                            } else {
                                stats.rejected.incrementAndGet();
                            }
                            stats.completed.incrementAndGet();
                            if (record) {
                                stats.recordLatency(System.nanoTime() - requestStarted);
                            }
                        } finally {
                            slots.release();
                        }
                    });
        }

        slots.acquire(config.concurrency());
        long finished = System.nanoTime();
        slots.release(config.concurrency());
        double elapsedSeconds = (finished - started) / 1_000_000_000.0;
        Result result = new Result(
                stats.offered.get(),
                stats.completed.get(),
                stats.accepted.get(),
                stats.rejected.get(),
                stats.transportErrors.get(),
                stats.dropped.get(),
                stats.accepted.get() / elapsedSeconds,
                stats.percentile(0.50),
                stats.percentile(0.99),
                stats.maxLatencyMillis.get());
        return new PhaseResult(result);
    }

    private static void waitUntil(long targetNanos) {
        while (true) {
            long remaining = targetNanos - System.nanoTime();
            if (remaining <= 0) return;
            if (remaining > 100_000) {
                LockSupport.parkNanos(remaining - 50_000);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private static String payload(String runId, long sequence) {
        return "{\"customerId\":\"gate-customer-" + (sequence % 10_000)
                + "\",\"modelId\":\"gpt-4o-mini\",\"inputTokens\":10,"
                + "\"outputTokens\":5,\"eventId\":\"" + runId + "-" + sequence
                + "\",\"timestamp\":" + System.currentTimeMillis() + "}";
    }

    private record PhaseResult(Result result) {}

    private static final class Stats {
        private final AtomicLong offered = new AtomicLong();
        private final AtomicLong completed = new AtomicLong();
        private final AtomicLong accepted = new AtomicLong();
        private final AtomicLong rejected = new AtomicLong();
        private final AtomicLong transportErrors = new AtomicLong();
        private final AtomicLong dropped = new AtomicLong();
        private final AtomicLongArray latencyMillis = new AtomicLongArray(MAX_RECORDED_LATENCY_MS + 1);
        private final AtomicLong maxLatencyMillis = new AtomicLong();

        private void recordLatency(long latencyNanos) {
            long millis = Math.max(0, latencyNanos / 1_000_000L);
            int bucket = (int) Math.min(millis, MAX_RECORDED_LATENCY_MS);
            latencyMillis.incrementAndGet(bucket);
            maxLatencyMillis.accumulateAndGet(millis, Math::max);
        }

        private long percentile(double percentile) {
            long samples = completed.get();
            if (samples == 0) return 0;
            long target = Math.max(1, (long) Math.ceil(samples * percentile));
            long seen = 0;
            for (int millis = 0; millis < latencyMillis.length(); millis++) {
                seen += latencyMillis.get(millis);
                if (seen >= target) return millis;
            }
            return maxLatencyMillis.get();
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseArgs(args);
        int targetEps = intOption(options, "target-eps", 10_000);
        Config config = new Config(
                options.getOrDefault("url", "http://127.0.0.1:8000/ingest"),
                targetEps,
                Duration.ofSeconds(intOption(options, "duration-seconds", 1_800)),
                Duration.ofSeconds(intOption(options, "warmup-seconds", 300)),
                intOption(options, "concurrency", 800),
                options.get("api-key"),
                doubleOption(options, "min-eps", targetEps),
                intOption(options, "max-p50-ms", 25),
                intOption(options, "max-p99-ms", 100));

        Result result = run(config);
        boolean passed = result.meetsGates(config);
        System.out.printf(
                "{\"passed\":%s,\"offered\":%d,\"completed\":%d,"
                        + "\"accepted\":%d,\"rejected\":%d,\"transportErrors\":%d,"
                        + "\"dropped\":%d,\"achievedEps\":%.2f,\"p50Ms\":%d,"
                        + "\"p99Ms\":%d,\"maxMs\":%d}%n",
                passed,
                result.offered(), result.completed(), result.accepted(), result.rejected(),
                result.transportErrors(), result.dropped(), result.achievedEps(),
                result.p50Millis(), result.p99Millis(), result.maxMillis());
        if (!passed) System.exit(1);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if (!arg.startsWith("--") || index + 1 >= args.length) {
                throw new IllegalArgumentException("expected --name value, got: " + arg);
            }
            options.put(arg.substring(2), args[++index]);
        }
        return options;
    }

    private static int intOption(Map<String, String> options, String name, int fallback) {
        return Integer.parseInt(options.getOrDefault(name, Integer.toString(fallback)));
    }

    private static double doubleOption(Map<String, String> options, String name, double fallback) {
        return Double.parseDouble(options.getOrDefault(name, Double.toString(fallback)));
    }
}
