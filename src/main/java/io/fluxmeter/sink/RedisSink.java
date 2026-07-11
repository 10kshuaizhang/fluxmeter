package io.fluxmeter.sink;

import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.util.TenantKeys;
import io.fluxmeter.util.BillingPeriod;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Non-budget Redis sink. SET NX + all counter/rollup writes run in one Lua EVAL
 * so a crash mid-flight cannot leave the idempotency key set without counters.
 */
public class RedisSink extends RichSinkFunction<UsageAggregate> {

    private final String host;
    private final int port;
    private transient JedisPool pool;

    // KEYS[1]=idempotency
    // [2..6]=customer input/output/total/cost/event
    // [7..10]=model input/output/total/cost
    // [11]=cache_read [12]=reasoning
    // [13..17]=global total/input/output/events/cost
    // [18]=global:last_window_end
    // [19]=period volume
    // [20]=month rollup hash [21]=day rollup hash
    // ARGV[1..5]=input/output/total/events/cost
    // ARGV[6..7]=cache_read/reasoning
    // ARGV[8]=window_end
    // ARGV[9]=rollup TTL sec
    static final String SINK_LUA_SCRIPT =
            "if redis.call('SET', KEYS[1], '1', 'NX', 'EX', '3600') == false then\n" +
            "  return 'SKIP'\n" +
            "end\n" +
            "redis.call('INCRBY', KEYS[2], ARGV[1])\n" +
            "redis.call('INCRBY', KEYS[3], ARGV[2])\n" +
            "redis.call('INCRBY', KEYS[4], ARGV[3])\n" +
            "redis.call('INCRBYFLOAT', KEYS[5], ARGV[5])\n" +
            "redis.call('INCRBY', KEYS[6], ARGV[4])\n" +
            "redis.call('INCRBY', KEYS[7], ARGV[1])\n" +
            "redis.call('INCRBY', KEYS[8], ARGV[2])\n" +
            "redis.call('INCRBY', KEYS[9], ARGV[3])\n" +
            "redis.call('INCRBYFLOAT', KEYS[10], ARGV[5])\n" +
            "if tonumber(ARGV[6]) > 0 then redis.call('INCRBY', KEYS[11], ARGV[6]) end\n" +
            "if tonumber(ARGV[7]) > 0 then redis.call('INCRBY', KEYS[12], ARGV[7]) end\n" +
            "redis.call('INCRBY', KEYS[13], ARGV[3])\n" +
            "redis.call('INCRBY', KEYS[14], ARGV[1])\n" +
            "redis.call('INCRBY', KEYS[15], ARGV[2])\n" +
            "redis.call('INCRBY', KEYS[16], ARGV[4])\n" +
            "redis.call('INCRBYFLOAT', KEYS[17], ARGV[5])\n" +
            "redis.call('SET', KEYS[18], ARGV[8])\n" +
            "redis.call('INCRBY', KEYS[19], ARGV[3])\n" +
            "local function rollup(hk)\n" +
            "  redis.call('HINCRBY', hk, 'input_tokens', ARGV[1])\n" +
            "  redis.call('HINCRBY', hk, 'output_tokens', ARGV[2])\n" +
            "  redis.call('HINCRBY', hk, 'total_tokens', ARGV[3])\n" +
            "  redis.call('HINCRBY', hk, 'event_count', ARGV[4])\n" +
            "  redis.call('HINCRBYFLOAT', hk, 'cost_usd', ARGV[5])\n" +
            "  if tonumber(ARGV[6]) > 0 then redis.call('HINCRBY', hk, 'cache_read_tokens', ARGV[6]) end\n" +
            "  if tonumber(ARGV[7]) > 0 then redis.call('HINCRBY', hk, 'reasoning_tokens', ARGV[7]) end\n" +
            "  redis.call('EXPIRE', hk, tonumber(ARGV[9]))\n" +
            "end\n" +
            "rollup(KEYS[20])\n" +
            "rollup(KEYS[21])\n" +
            "return 'OK'";

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
            apply(jedis, agg);
        }
    }

    /**
     * Package-visible for idempotency tests — same Lua path as the Flink sink.
     * @return "OK" or "SKIP"
     */
    static String apply(Jedis jedis, UsageAggregate agg) {
        String customerKey = TenantKeys.customerPrefix(agg.getTenantId(), agg.getCustomerId());
        String modelKey = customerKey + ":model:" + agg.getModelId();
        String windowId = TenantKeys.windowId(
                agg.getTenantId(), agg.getCustomerId(), agg.getModelId(), agg.getWindowStart());
        String idempotencyKey = "applied:" + windowId;
        String periodKey = BillingPeriod.periodVolumeKey(
                agg.getTenantId(), agg.getCustomerId(), agg.getModelId(), agg.getWindowEnd());
        String rollupBase = "rollup:" + agg.getCustomerId();
        String monthKey = rollupBase + ":period:" + BillingPeriod.monthUtc(agg.getWindowEnd());
        String dayKey = rollupBase + ":d:" + BillingPeriod.dayUtc(agg.getWindowEnd());

        Object result = jedis.eval(
                SINK_LUA_SCRIPT,
                21,
                idempotencyKey,
                customerKey + ":input_tokens",
                customerKey + ":output_tokens",
                customerKey + ":total_tokens",
                customerKey + ":cost_usd",
                customerKey + ":event_count",
                modelKey + ":input_tokens",
                modelKey + ":output_tokens",
                modelKey + ":total_tokens",
                modelKey + ":cost_usd",
                customerKey + ":cache_read_tokens",
                customerKey + ":reasoning_tokens",
                TenantKeys.globalKey(agg.getTenantId(), "total_tokens"),
                TenantKeys.globalKey(agg.getTenantId(), "input_tokens"),
                TenantKeys.globalKey(agg.getTenantId(), "output_tokens"),
                TenantKeys.globalKey(agg.getTenantId(), "total_events"),
                TenantKeys.globalKey(agg.getTenantId(), "total_cost_usd"),
                TenantKeys.globalKey(agg.getTenantId(), "last_window_end"),
                periodKey,
                monthKey,
                dayKey,
                String.valueOf(agg.getInputTokens()),
                String.valueOf(agg.getOutputTokens()),
                String.valueOf(agg.getTotalTokens()),
                String.valueOf(agg.getEventCount()),
                String.valueOf(agg.getCostUsd()),
                String.valueOf(agg.getCacheReadTokens()),
                String.valueOf(agg.getReasoningTokens()),
                String.valueOf(agg.getWindowEnd()),
                "34560000"
        );
        return result == null ? "OK" : result.toString();
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
