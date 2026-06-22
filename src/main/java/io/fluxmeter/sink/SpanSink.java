package io.fluxmeter.sink;

import io.fluxmeter.model.SpanAggregate;
import io.fluxmeter.util.TenantKeys;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

/**
 * Writes span-level aggregates to Redis.
 *
 * Keys written:
 *   span:{spanId}:cost_usd         — total cost of the agent run
 *   span:{spanId}:total_tokens     — total tokens across all calls
 *   span:{spanId}:call_count       — number of LLM calls
 *   span:{spanId}:duration_ms      — time from first to last call
 *   span:{spanId}:customer_id      — owning customer
 *   customer:{customerId}:spans    — ZADD by cost (sorted set of spans)
 *
 * TTL: 24 hours (spans are ephemeral; query API serves live data)
 */
public class SpanSink extends RichSinkFunction<SpanAggregate> {

    private final String host;
    private final int port;
    private transient JedisPool pool;

    private static final int SPAN_TTL_SECONDS = 86400; // 24 hours

    public SpanSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
        pool = RedisConnections.createPool(host, port, 4);
    }

    @Override
    public void invoke(SpanAggregate span, Context context) {
        if (span.getSpanId() == null || span.getSpanId().isEmpty()) {
            return;
        }

        try (Jedis jedis = pool.getResource()) {
            String key = "span:" + span.getSpanId();

            // Use SET (overwrite), not INCRBY. Session windows can fire multiple
            // times (session merge, late data). Each fire contains the FULL aggregate
            // for the span, not a delta. Overwriting is correct; incrementing double-counts.
            Pipeline pipe = jedis.pipelined();
            pipe.set(key + ":cost_usd", String.valueOf(span.getCostUsd()));
            pipe.set(key + ":total_tokens", String.valueOf(span.getTotalTokens()));
            pipe.set(key + ":call_count", String.valueOf(span.getCallCount()));
            pipe.set(key + ":duration_ms", String.valueOf(span.getDurationMs()));
            pipe.set(key + ":customer_id", span.getCustomerId());

            // Set TTL on all span keys
            pipe.expire(key + ":cost_usd", SPAN_TTL_SECONDS);
            pipe.expire(key + ":total_tokens", SPAN_TTL_SECONDS);
            pipe.expire(key + ":call_count", SPAN_TTL_SECONDS);
            pipe.expire(key + ":duration_ms", SPAN_TTL_SECONDS);
            pipe.expire(key + ":customer_id", SPAN_TTL_SECONDS);

            // Add to customer's sorted set of spans (sorted by cost for top-N queries)
            pipe.zadd(TenantKeys.customerPrefix(span.getTenantId(), span.getCustomerId()) + ":spans",
                    span.getCostUsd(), span.getSpanId());

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
