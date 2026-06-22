package io.fluxmeter.sink;

import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.util.TenantKeys;

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

            pipe.sync();
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
