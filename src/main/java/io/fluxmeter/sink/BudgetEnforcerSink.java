package io.fluxmeter.sink;

import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.util.TenantKeys;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Combined sink: writes aggregated usage to Redis AND enforces budget limits.
 *
 * All counter writes, idempotency, and budget deduction run in one Lua EVAL
 * so a crash mid-flight cannot leave counters updated without budget deduction.
 */
public class BudgetEnforcerSink extends RichSinkFunction<UsageAggregate> {

    private final String redisHost;
    private final int redisPort;
    private final String kafkaBrokers;
    private final String alertTopic;

    private transient JedisPool pool;
    private transient KafkaProducer<String, String> alertProducer;
    private transient ObjectMapper mapper;

    private static final double DEFAULT_ALERT_THRESHOLD_PERCENT = 0.10;

    // KEYS[1]=idempotency ... [19]=global:last_window, [20]=cache_read, [21]=reasoning
    private static final String SINK_LUA_SCRIPT =
            "if redis.call('SET', KEYS[1], '1', 'NX', 'EX', '3600') == false then\n" +
            "  return {'SKIP', '0', '0'}\n" +
            "end\n" +
            "redis.call('INCRBY', KEYS[2], ARGV[1])\n" +
            "redis.call('INCRBY', KEYS[3], ARGV[2])\n" +
            "redis.call('INCRBY', KEYS[4], ARGV[3])\n" +
            "redis.call('INCRBY', KEYS[5], ARGV[4])\n" +
            "redis.call('INCRBY', KEYS[6], ARGV[1])\n" +
            "redis.call('INCRBY', KEYS[7], ARGV[2])\n" +
            "redis.call('INCRBY', KEYS[8], ARGV[3])\n" +
            "redis.call('INCRBYFLOAT', KEYS[9], ARGV[5])\n" +
            "redis.call('INCRBYFLOAT', KEYS[10], ARGV[5])\n" +
            "if tonumber(ARGV[6]) > 0 then redis.call('INCRBY', KEYS[20], ARGV[6]) end\n" +
            "if tonumber(ARGV[7]) > 0 then redis.call('INCRBY', KEYS[21], ARGV[7]) end\n" +
            "redis.call('INCRBY', KEYS[11], ARGV[3])\n" +
            "redis.call('INCRBY', KEYS[12], ARGV[1])\n" +
            "redis.call('INCRBY', KEYS[13], ARGV[2])\n" +
            "redis.call('INCRBY', KEYS[14], ARGV[4])\n" +
            "redis.call('INCRBYFLOAT', KEYS[15], ARGV[5])\n" +
            "redis.call('SET', KEYS[19], ARGV[9])\n" +
            "redis.call('INCRBYFLOAT', KEYS[22], ARGV[5])\n" +
            "local balance = tonumber(redis.call('GET', KEYS[16]))\n" +
            "if balance == nil then return {'NONE', '0', '0'} end\n" +
            "local cost = tonumber(ARGV[5])\n" +
            "local new_balance = balance - cost\n" +
            "if new_balance < 0 then\n" +
            "  redis.call('INCRBYFLOAT', KEYS[23], -new_balance)\n" +
            "  new_balance = 0\n" +
            "end\n" +
            "redis.call('SET', KEYS[16], tostring(new_balance))\n" +
            "local threshold_str = redis.call('GET', KEYS[17])\n" +
            "local threshold\n" +
            "if threshold_str then\n" +
            "  threshold = tonumber(threshold_str)\n" +
            "else\n" +
            "  local initial = tonumber(redis.call('GET', KEYS[18]) or '0')\n" +
            "  threshold = initial * tonumber(ARGV[8])\n" +
            "end\n" +
            "if new_balance <= 0 then\n" +
            "  return {'EXHAUSTED', tostring(new_balance), tostring(balance)}\n" +
            "elseif new_balance <= threshold and balance > threshold then\n" +
            "  return {'LOW', tostring(new_balance), tostring(balance)}\n" +
            "else\n" +
            "  return {'OK', tostring(new_balance), tostring(balance)}\n" +
            "end";

    public BudgetEnforcerSink(String redisHost, int redisPort, String kafkaBrokers, String alertTopic) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.kafkaBrokers = kafkaBrokers;
        this.alertTopic = alertTopic;
    }

    @Override
    public void open(Configuration parameters) {
        pool = RedisConnections.createPool(redisHost, redisPort, 8);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        alertProducer = new KafkaProducer<>(props);

        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(UsageAggregate agg, Context context) {
        try (Jedis jedis = pool.getResource()) {
            String customerId = agg.getCustomerId();
            String customerKey = TenantKeys.customerPrefix(agg.getTenantId(), customerId);
            String modelKey = customerKey + ":model:" + agg.getModelId();
            String budgetKey = TenantKeys.budgetPrefix(agg.getTenantId(), customerId);

            String windowId = TenantKeys.windowId(agg.getTenantId(), customerId, agg.getModelId(), agg.getWindowStart());
            String idempotencyKey = "applied:" + windowId;

            @SuppressWarnings("unchecked")
            java.util.List<String> result = (java.util.List<String>) jedis.eval(
                    SINK_LUA_SCRIPT,
                    23,
                    idempotencyKey,
                    customerKey + ":input_tokens",
                    customerKey + ":output_tokens",
                    customerKey + ":total_tokens",
                    customerKey + ":event_count",
                    modelKey + ":input_tokens",
                    modelKey + ":output_tokens",
                    modelKey + ":total_tokens",
                    modelKey + ":cost_usd",
                    customerKey + ":cost_usd",
                    TenantKeys.globalKey(agg.getTenantId(), "total_tokens"),
                    TenantKeys.globalKey(agg.getTenantId(), "input_tokens"),
                    TenantKeys.globalKey(agg.getTenantId(), "output_tokens"),
                    TenantKeys.globalKey(agg.getTenantId(), "total_events"),
                    TenantKeys.globalKey(agg.getTenantId(), "total_cost_usd"),
                    budgetKey + ":balance_usd",
                    budgetKey + ":alert_threshold_usd",
                    budgetKey + ":initial_balance_usd",
                    TenantKeys.globalKey(agg.getTenantId(), "last_window_end"),
                    customerKey + ":cache_read_tokens",
                    customerKey + ":reasoning_tokens",
                    budgetKey + ":total_deducted_usd",
                    budgetKey + ":debt_usd",
                    String.valueOf(agg.getInputTokens()),
                    String.valueOf(agg.getOutputTokens()),
                    String.valueOf(agg.getTotalTokens()),
                    String.valueOf(agg.getEventCount()),
                    String.valueOf(agg.getCostUsd()),
                    String.valueOf(agg.getCacheReadTokens()),
                    String.valueOf(agg.getReasoningTokens()),
                    String.valueOf(DEFAULT_ALERT_THRESHOLD_PERCENT),
                    String.valueOf(agg.getWindowEnd())
            );

            String status = result.get(0);
            if ("SKIP".equals(status) || "NONE".equals(status)) {
                return;
            }

            double newBalance = Double.parseDouble(result.get(1));
            if ("EXHAUSTED".equals(status)) {
                emitAlert(customerId, "BUDGET_EXHAUSTED", newBalance, agg);
            } else if ("LOW".equals(status)) {
                emitAlert(customerId, "BUDGET_LOW", newBalance, agg);
            }
        }
    }

    private void emitAlert(String customerId, String alertType, double remainingBalance,
                           UsageAggregate agg) {
        try {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", alertType);
            alert.put("customerId", customerId);
            alert.put("remainingBalanceUsd", remainingBalance);
            alert.put("windowCostUsd", agg.getCostUsd());
            alert.put("modelId", agg.getModelId());
            alert.put("windowStart", agg.getWindowStart());
            alert.put("windowEnd", agg.getWindowEnd());
            alert.put("timestamp", System.currentTimeMillis());

            String value = mapper.writeValueAsString(alert);
            alertProducer.send(new ProducerRecord<>(alertTopic, customerId, value));
        } catch (Exception e) {
            // Don't fail the pipeline on alert delivery failure
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
        if (alertProducer != null) {
            alertProducer.close();
        }
    }
}
