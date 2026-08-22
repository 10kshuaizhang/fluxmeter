package io.fluxmeter.sink;

import io.fluxmeter.model.TokenEvent;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventProjectionSinkTest {

    @Test
    void projectionIdempotencyRetentionMatchesCrashSafetyWindow() {
        assertEquals(600L, EventProjectionSink.projectionTtlSeconds(null));
        assertEquals(600L, EventProjectionSink.projectionTtlSeconds(""));
        assertEquals(900L, EventProjectionSink.projectionTtlSeconds("900"));
    }

    @BeforeEach
    void loadCatalog() throws Exception {
        PricingCatalog.reload(PricingCatalog.loadFromBytes(
                Files.readAllBytes(Path.of("config/pricing.json"))));
    }

    @Test
    void trustedEventUpdatesReplaySafeFeatureProjections() {
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
            Assumptions.assumeTrue(false, "Redis not reachable");
            return;
        }

        String suffix = String.valueOf(System.currentTimeMillis());
        TokenEvent event = new TokenEvent();
        event.setEventId("evt-projection-" + suffix);
        event.setCustomerId("cust_projection_" + suffix);
        event.setModelId("gpt-4o-mini");
        event.setInputTokens(10);
        event.setOutputTokens(5);
        event.setTimestamp(1_770_000_000_000L);
        event.setReceivedAt(1_770_000_000_123L);
        event.setSessionId("session_" + suffix);
        event.setApiKeyId("key_" + suffix);
        event.setMetadata(Map.of("feature", "chat"));
        event.setReservationId("reservation_" + suffix);
        event.setReservedUsd(0.2);

        try (Jedis jedis = pool.getResource()) {
            jedis.set("package:" + event.getCustomerId() + ":tokens_remaining", "100");
            jedis.set("budget:" + event.getCustomerId() + ":held_usd", "1.0");
            jedis.hset("reservation:" + event.getReservationId(), "customer_id", event.getCustomerId());
            // Keep the record outside the live Gateway expiry worker's due range.
            jedis.zadd("gateway:reservations:pending",
                    System.currentTimeMillis() / 1000.0 + 3600, event.getReservationId());

            assertEquals("OK", EventProjectionSink.apply(jedis, event));
            assertEquals("SKIP", EventProjectionSink.apply(jedis, event));

            long projectionTtl = jedis.ttl(EventProjectionSink.projectionKey(event.getEventId()));
            assertTrue(projectionTtl > 0 && projectionTtl <= 600);

            assertEquals("15", jedis.get("session:" + event.getSessionId() + ":total_tokens"));
            assertEquals("1", jedis.get("session:" + event.getSessionId() + ":event_count"));
            assertEquals("85", jedis.get("package:" + event.getCustomerId() + ":tokens_remaining"));
            assertEquals(1.0, Double.parseDouble(
                    jedis.get("budget:" + event.getCustomerId() + ":held_usd")), 0.000001);
            assertNotNull(jedis.get("dim:feature:chat:event_count"));
            assertNotNull(jedis.get("flink:heartbeat:last_processed_at"));
            assertTrue(jedis.exists("reservation:" + event.getReservationId()));
            assertNotNull(jedis.zscore("gateway:reservations:pending", event.getReservationId()));
        } finally {
            pool.close();
        }
    }
}
