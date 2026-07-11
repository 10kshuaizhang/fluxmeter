package io.fluxmeter.sink;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.pricing.PricingCatalog;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Requires Redis (REDIS_HOST, default localhost). Skips when unreachable.
 * Verifies SET NX + counters are atomic: second apply of the same window is SKIP.
 */
class RedisSinkIdempotencyTest {

    @BeforeEach
    void loadCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("config/pricing.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void secondApplyOfSameWindowIsSkipped() {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(2);
        cfg.setMaxWait(Duration.ofSeconds(2));
        JedisPool pool;
        try {
            pool = new JedisPool(cfg, host, port, 2000);
            try (Jedis ping = pool.getResource()) {
                ping.ping();
            }
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Redis not reachable at " + host + ":" + port);
            return;
        }

        String customerId = "idempotency_test_" + System.currentTimeMillis();
        long windowStart = 1_700_000_000_000L;
        long windowEnd = windowStart + 10_000L;

        TokenEvent event = new TokenEvent();
        event.setEventId("evt-idem-" + customerId);
        event.setCustomerId(customerId);
        event.setModelId("gpt-4o-mini");
        event.setProvider("openai");
        event.setInputTokens(1_000);
        event.setOutputTokens(0);
        event.setTimestamp(windowStart + 1);

        UsageAggregate agg = new UsageAggregate(customerId, "gpt-4o-mini", windowStart, windowEnd);
        agg.addEvent(event);

        try (Jedis jedis = pool.getResource()) {
            String customerKey = "customer:" + customerId;
            String first = RedisSink.apply(jedis, agg);
            assertEquals("OK", first);

            long inputAfterFirst = Long.parseLong(jedis.get(customerKey + ":input_tokens"));
            assertEquals(1000L, inputAfterFirst);

            String second = RedisSink.apply(jedis, agg);
            assertEquals("SKIP", second);

            long inputAfterSecond = Long.parseLong(jedis.get(customerKey + ":input_tokens"));
            assertEquals(1000L, inputAfterSecond, "replay must not double-count");

            jedis.del(
                    "applied:" + customerId + "|gpt-4o-mini|" + windowStart,
                    customerKey + ":input_tokens",
                    customerKey + ":output_tokens",
                    customerKey + ":total_tokens",
                    customerKey + ":cost_usd",
                    customerKey + ":event_count");
        } finally {
            pool.close();
        }
    }
}
