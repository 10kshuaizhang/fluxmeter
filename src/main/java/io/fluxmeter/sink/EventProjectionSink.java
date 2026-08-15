package io.fluxmeter.sink;

import io.fluxmeter.job.UsageAggregateFunction;
import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.util.BillingPeriod;
import io.fluxmeter.util.TenantKeys;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Idempotent per-event projections that cannot be reconstructed from window aggregates.
 */
public class EventProjectionSink extends RichSinkFunction<TokenEvent> {

    private static final long PROJECTION_TTL_SECONDS = 30L * 24 * 60 * 60;
    private static final long SESSION_TTL_SECONDS = 90L * 24 * 60 * 60;
    private static final long DIM_TTL_SECONDS = 400L * 24 * 60 * 60;

    private static final String SCRIPT =
            "if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[11]) == false then return 'SKIP' end\n" +
            "if KEYS[2] ~= 'noop' then\n" +
            " redis.call('SET', KEYS[2]..':customer_id', ARGV[7], 'EX', ARGV[12])\n" +
            " redis.call('INCRBY', KEYS[2]..':input_tokens', ARGV[1])\n" +
            " redis.call('INCRBY', KEYS[2]..':output_tokens', ARGV[2])\n" +
            " redis.call('INCRBY', KEYS[2]..':total_tokens', ARGV[3])\n" +
            " redis.call('INCRBY', KEYS[2]..':event_count', 1)\n" +
            " redis.call('INCRBYFLOAT', KEYS[2]..':cost_usd', ARGV[6])\n" +
            " if tonumber(ARGV[4]) > 0 then redis.call('INCRBY', KEYS[2]..':cache_read_tokens', ARGV[4]) end\n" +
            " if tonumber(ARGV[5]) > 0 then redis.call('INCRBY', KEYS[2]..':reasoning_tokens', ARGV[5]) end\n" +
            " for _,s in ipairs({'input_tokens','output_tokens','total_tokens','event_count','cost_usd','cache_read_tokens','reasoning_tokens'}) do redis.call('EXPIRE', KEYS[2]..':'..s, ARGV[12]) end\n" +
            "end\n" +
            "redis.call('HINCRBYFLOAT', KEYS[3], 'cost_usd', ARGV[6])\n" +
            "redis.call('HINCRBY', KEYS[3], 'event_count', 1)\n" +
            "redis.call('HINCRBY', KEYS[3], 'total_tokens', ARGV[3])\n" +
            "redis.call('HINCRBY', KEYS[3], 'input_tokens', ARGV[1])\n" +
            "redis.call('HINCRBY', KEYS[3], 'output_tokens', ARGV[2])\n" +
            "redis.call('EXPIRE', KEYS[3], ARGV[13])\n" +
            "if redis.call('EXISTS', KEYS[4]) == 1 then\n" +
            " local rem=tonumber(redis.call('GET', KEYS[4]) or '0'); local used=tonumber(ARGV[3]); local next=rem-used\n" +
            " if next < 0 then redis.call('INCRBY', KEYS[4]..':debt', -next); next=0 end; redis.call('SET', KEYS[4], next)\n" +
            "end\n" +
            "if KEYS[5] ~= 'noop' then redis.call('INCRBYFLOAT', KEYS[5], ARGV[6]); redis.call('EXPIRE', KEYS[5], 172800) end\n" +
            "if KEYS[6] ~= 'noop' then redis.call('INCRBYFLOAT', KEYS[6], ARGV[6]); redis.call('EXPIRE', KEYS[6], 5356800) end\n" +
            "if KEYS[7] ~= 'noop' then redis.call('SADD', KEYS[7], ARGV[14]); redis.call('EXPIRE', KEYS[7], ARGV[11]) end\n" +
            "for i=11,#KEYS do redis.call('INCRBYFLOAT', KEYS[i]..':cost_usd', ARGV[6]); redis.call('INCRBY', KEYS[i]..':event_count', 1); redis.call('EXPIRE', KEYS[i]..':cost_usd', ARGV[13]); redis.call('EXPIRE', KEYS[i]..':event_count', ARGV[13]) end\n" +
            "redis.call('SET', 'flink:heartbeat:last_processed_at', ARGV[10])\n" +
            "return 'OK'";

    private final String host;
    private final int port;
    private final Set<String> allowedDimensions;
    private transient JedisPool pool;

    public EventProjectionSink(String host, int port) {
        this(host, port, System.getenv().getOrDefault("FLUXMETER_USAGE_DIMS", "room_id,feature"));
    }

    EventProjectionSink(String host, int port, String dimensions) {
        this.host = host;
        this.port = port;
        this.allowedDimensions = new HashSet<>(Arrays.asList(dimensions.split(",")));
    }

    @Override
    public void open(Configuration parameters) {
        pool = RedisConnections.createPool(host, port, 8);
    }

    @Override
    public void invoke(TokenEvent event, Context context) {
        try (Jedis jedis = pool.getResource()) {
            apply(jedis, event, allowedDimensions);
        }
    }

    static String apply(Jedis jedis, TokenEvent event) {
        return apply(jedis, event, Set.of("room_id", "feature"));
    }

    static String apply(Jedis jedis, TokenEvent event, Set<String> allowedDimensions) {
        long before = 0;
        if (event.getMetadata() != null) {
            String raw = event.getMetadata().get(UsageAggregateFunction.MONTHLY_VOLUME_BEFORE_KEY);
            if (raw != null) before = Long.parseLong(raw);
        }
        double costUsd = UsageAggregate.calculateEventCostMicro(event, before) / 1_000_000.0;
        long accountingTime = event.getReceivedAt() > 0 ? event.getReceivedAt() : event.getTimestamp();
        String period = BillingPeriod.monthUtc(accountingTime);
        String day = BillingPeriod.dayUtc(accountingTime);
        long windowMillis = Long.parseLong(
                System.getenv().getOrDefault("WINDOW_SECONDS", "10")) * 1000;
        long windowStart = Math.floorDiv(event.getTimestamp(), windowMillis) * windowMillis;
        String windowId = TenantKeys.windowId(
                event.getTenantId(), event.getCustomerId(), event.getModelId(), windowStart);

        List<String> keys = new ArrayList<>();
        keys.add("projection:" + sha256(event.getEventId()));
        keys.add(event.getSessionId() == null || event.getSessionId().isBlank()
                ? "noop" : "session:" + event.getSessionId());
        keys.add("rollup:" + event.getCustomerId() + ":model:"
                + UsageAggregate.normalizeModelId(event.getModelId()) + ":period:" + period);
        keys.add("package:" + event.getCustomerId() + ":tokens_remaining");
        keys.add(event.getApiKeyId() == null ? "noop" : "apikey:" + event.getApiKeyId() + ":spent:d:" + day);
        keys.add(event.getApiKeyId() == null ? "noop" : "apikey:" + event.getApiKeyId() + ":spent:m:" + period);
        keys.add(event.getReservationId() == null ? "noop" : TenantKeys.windowReservationsKey(windowId));
        keys.add("noop");
        keys.add("noop");
        keys.add("noop");

        Map<String, String> metadata = event.getMetadata();
        if (metadata != null) {
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                if (allowedDimensions.contains(entry.getKey()) && entry.getValue() != null && !entry.getValue().isBlank()) {
                    keys.add("dim:" + entry.getKey() + ":" + entry.getValue());
                    keys.add("dim:" + entry.getKey() + ":" + entry.getValue() + ":period:" + period);
                }
            }
        }

        Object result = jedis.eval(
                SCRIPT,
                keys,
                List.of(
                        String.valueOf(event.getInputTokens()),
                        String.valueOf(event.getOutputTokens()),
                        String.valueOf(event.getTotalTokens()),
                        String.valueOf(event.getCacheReadTokens()),
                        String.valueOf(event.getReasoningTokens()),
                        String.valueOf(costUsd),
                        event.getCustomerId(),
                        String.valueOf(accountingTime),
                        String.valueOf(event.getReservedUsd()),
                        String.valueOf(System.currentTimeMillis()),
                        String.valueOf(PROJECTION_TTL_SECONDS),
                        String.valueOf(SESSION_TTL_SECONDS),
                        String.valueOf(DIM_TTL_SECONDS)
                        , event.getReservationId() == null ? "" : event.getReservationId()
                )
        );
        return result == null ? "OK" : result.toString();
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder();
            for (byte b : digest) out.append(String.format("%02x", b));
            return out.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        if (pool != null) pool.close();
    }
}
