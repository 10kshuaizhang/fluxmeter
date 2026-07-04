package io.fluxmeter.sink;

import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.util.TenantKeys;
import io.fluxmeter.util.BillingPeriod;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisSink extends RichSinkFunction<UsageAggregate> {

    private final String host;
    private final int port;
    private transient JedisPool pool;

    public RedisSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
        pool = RedisConnections.createPool(host, port, 8);
    }

    @Override
    public void invoke(UsageAggregate agg, Context context) {
        try (Jedis jedis = pool.getResource()) {
            // Idempotency: skip if this window was already applied
            String windowId = TenantKeys.windowId(agg.getTenantId(), agg.getCustomerId(), agg.getModelId(), agg.getWindowStart());
            String idempotencyKey = "applied:" + windowId;
            String setResult = jedis.set(idempotencyKey, "1", new redis.clients.jedis.params.SetParams().nx().ex(3600));
            if (setResult == null) {
                return; // Already applied
            }

            Pipeline pipe = jedis.pipelined();
            String customerKey = TenantKeys.customerPrefix(agg.getTenantId(), agg.getCustomerId());
            String modelKey = customerKey + ":model:" + agg.getModelId();

            // Per-customer token breakdown
            pipe.incrBy(customerKey + ":input_tokens", agg.getInputTokens());
            pipe.incrBy(customerKey + ":output_tokens", agg.getOutputTokens());
            pipe.incrBy(customerKey + ":total_tokens", agg.getTotalTokens());
            pipe.incrByFloat(customerKey + ":cost_usd", agg.getCostUsd());
            pipe.incrBy(customerKey + ":event_count", agg.getEventCount());

            // Per-customer per-model breakdown
            pipe.incrBy(modelKey + ":input_tokens", agg.getInputTokens());
            pipe.incrBy(modelKey + ":output_tokens", agg.getOutputTokens());
            pipe.incrBy(modelKey + ":total_tokens", agg.getTotalTokens());
            pipe.incrByFloat(modelKey + ":cost_usd", agg.getCostUsd());

            // Cache and reasoning tokens (only if non-zero)
            if (agg.getCacheReadTokens() > 0) {
                pipe.incrBy(customerKey + ":cache_read_tokens", agg.getCacheReadTokens());
            }
            if (agg.getReasoningTokens() > 0) {
                pipe.incrBy(customerKey + ":reasoning_tokens", agg.getReasoningTokens());
            }

            // Global counters (for dashboard)
            pipe.incrBy(TenantKeys.globalKey(agg.getTenantId(), "total_tokens"), agg.getTotalTokens());
            pipe.incrBy(TenantKeys.globalKey(agg.getTenantId(), "input_tokens"), agg.getInputTokens());
            pipe.incrBy(TenantKeys.globalKey(agg.getTenantId(), "output_tokens"), agg.getOutputTokens());
            pipe.incrBy(TenantKeys.globalKey(agg.getTenantId(), "total_events"), agg.getEventCount());
            pipe.incrByFloat(TenantKeys.globalKey(agg.getTenantId(), "total_cost_usd"), agg.getCostUsd());

            pipe.set(TenantKeys.globalKey(agg.getTenantId(), "last_window_end"), String.valueOf(agg.getWindowEnd()));

            // ponytail: sync billing-period volume counter for Lite/Flink hybrid dashboards
            String periodKey = BillingPeriod.periodVolumeKey(
                    agg.getTenantId(), agg.getCustomerId(), agg.getModelId(), agg.getWindowEnd());
            pipe.incrBy(periodKey, agg.getTotalTokens());

            // Calendar month/day rollup buckets (query API period/day endpoints)
            writeRollupBucket(pipe, agg.getCustomerId(), agg.getWindowEnd(), agg);

            pipe.sync();
        }
    }

    /** Mirrors api/rollup_worker.py month/day hash keys for full-mode period queries. */
    private static void writeRollupBucket(Pipeline pipe, String customerId, long windowEndMs, UsageAggregate agg) {
        String rollupBase = "rollup:" + customerId;
        String monthKey = rollupBase + ":period:" + BillingPeriod.monthUtc(windowEndMs);
        String dayKey = rollupBase + ":d:" + BillingPeriod.dayUtc(windowEndMs);
        writeRollupHash(pipe, monthKey, agg, 34560000);
        writeRollupHash(pipe, dayKey, agg, 34560000);
    }

    private static void writeRollupHash(Pipeline pipe, String hashKey, UsageAggregate agg, int ttlSec) {
        pipe.hincrBy(hashKey, "input_tokens", agg.getInputTokens());
        pipe.hincrBy(hashKey, "output_tokens", agg.getOutputTokens());
        pipe.hincrBy(hashKey, "total_tokens", agg.getTotalTokens());
        pipe.hincrBy(hashKey, "event_count", agg.getEventCount());
        pipe.hincrByFloat(hashKey, "cost_usd", agg.getCostUsd());
        if (agg.getCacheReadTokens() > 0) {
            pipe.hincrBy(hashKey, "cache_read_tokens", agg.getCacheReadTokens());
        }
        if (agg.getReasoningTokens() > 0) {
            pipe.hincrBy(hashKey, "reasoning_tokens", agg.getReasoningTokens());
        }
        pipe.expire(hashKey, ttlSec);
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
