package io.fluxmeter.sink;

import io.fluxmeter.model.UsageAggregate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Combined sink: writes aggregated usage to Redis AND enforces budget limits.
 *
 * For each window aggregate:
 * 1. Increments usage counters in Redis (same as RedisSink)
 * 2. Deducts cost from customer's prepaid balance (INCRBYFLOAT negative)
 * 3. Checks remaining balance against thresholds
 * 4. Publishes alerts/kill signals to Kafka when thresholds crossed
 *
 * Budget setup: SET budget:cust_123:balance_usd "100.00"
 * Optional:     SET budget:cust_123:alert_threshold_usd "10.00"
 */
public class BudgetEnforcerSink extends RichSinkFunction<UsageAggregate> {

    private final String redisHost;
    private final int redisPort;
    private final String kafkaBrokers;
    private final String alertTopic;

    private transient JedisPool pool;
    private transient KafkaProducer<String, String> alertProducer;
    private transient ObjectMapper mapper;

    // Default alert threshold: warn when 10% of balance remaining
    private static final double DEFAULT_ALERT_THRESHOLD_PERCENT = 0.10;

    // Lua script for atomic: counter writes + budget deduction + threshold check.
    // All in one EVAL so crash between counter write and budget deduction is impossible.
    // KEYS[1]=balance_key, KEYS[2]=threshold_key, KEYS[3]=customer_cost_key, KEYS[4]=initial_balance_key
    // ARGV[1]=cost, ARGV[2]=default_threshold_pct, ARGV[3]=total_tokens, ARGV[4]=event_count
    private static final String BUDGET_LUA_SCRIPT =
            "local balance_key = KEYS[1]\n" +
            "local threshold_key = KEYS[2]\n" +
            "local cost_key = KEYS[3]\n" +
            "local initial_key = KEYS[4]\n" +
            "local cost = tonumber(ARGV[1])\n" +
            "local default_threshold_pct = tonumber(ARGV[2])\n" +
            // Increment customer cost counter (always, even without budget)
            "redis.call('INCRBYFLOAT', cost_key, ARGV[1])\n" +
            // Budget check
            "local balance = tonumber(redis.call('GET', balance_key))\n" +
            "if balance == nil then return {'NONE', '0', '0'} end\n" +
            "local new_balance = balance - cost\n" +
            "redis.call('SET', balance_key, tostring(new_balance))\n" +
            // Threshold: use explicit config, or 10% of INITIAL balance (not current)
            "local threshold_str = redis.call('GET', threshold_key)\n" +
            "local threshold\n" +
            "if threshold_str then\n" +
            "  threshold = tonumber(threshold_str)\n" +
            "else\n" +
            "  local initial = tonumber(redis.call('GET', initial_key) or '0')\n" +
            "  threshold = initial * default_threshold_pct\n" +
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
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(8);
        pool = new JedisPool(config, redisHost, redisPort);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // Send alerts immediately
        alertProducer = new KafkaProducer<>(props);

        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(UsageAggregate agg, Context context) {
        try (Jedis jedis = pool.getResource()) {
            String customerId = agg.getCustomerId();
            String customerKey = "customer:" + customerId;
            String modelKey = customerKey + ":model:" + agg.getModelId();
            String budgetKey = "budget:" + customerId;

            // --- Idempotency check: skip if this window was already applied ---
            String windowId = customerId + "|" + agg.getModelId() + "|" + agg.getWindowStart();
            String idempotencyKey = "applied:" + windowId;
            // SET NX with 1-hour TTL — returns OK only if key didn't exist
            String setResult = jedis.set(idempotencyKey, "1", new redis.clients.jedis.params.SetParams().nx().ex(3600));
            if (setResult == null) {
                return; // Already applied — skip (exactly-once guarantee)
            }

            // --- Phase 1: Write usage counters (pipeline for non-budget keys) ---
            Pipeline pipe = jedis.pipelined();

            pipe.incrBy(customerKey + ":input_tokens", agg.getInputTokens());
            pipe.incrBy(customerKey + ":output_tokens", agg.getOutputTokens());
            pipe.incrBy(customerKey + ":total_tokens", agg.getTotalTokens());
            pipe.incrBy(customerKey + ":event_count", agg.getEventCount());

            pipe.incrBy(modelKey + ":input_tokens", agg.getInputTokens());
            pipe.incrBy(modelKey + ":output_tokens", agg.getOutputTokens());
            pipe.incrBy(modelKey + ":total_tokens", agg.getTotalTokens());
            pipe.incrByFloat(modelKey + ":cost_usd", agg.getCostUsd());

            if (agg.getCacheReadTokens() > 0) {
                pipe.incrBy(customerKey + ":cache_read_tokens", agg.getCacheReadTokens());
            }
            if (agg.getReasoningTokens() > 0) {
                pipe.incrBy(customerKey + ":reasoning_tokens", agg.getReasoningTokens());
            }

            pipe.incrBy("global:total_tokens", agg.getTotalTokens());
            pipe.incrBy("global:input_tokens", agg.getInputTokens());
            pipe.incrBy("global:output_tokens", agg.getOutputTokens());
            pipe.incrBy("global:total_events", agg.getEventCount());
            pipe.incrByFloat("global:total_cost_usd", agg.getCostUsd());
            pipe.set("global:last_window_end", String.valueOf(agg.getWindowEnd()));

            pipe.sync();

            // --- Phase 2: Budget enforcement + customer cost (atomic via Lua) ---
            // The Lua script atomically: increments customer cost_usd AND deducts budget.
            // This prevents the crash-between-counter-and-deduct race condition.
            @SuppressWarnings("unchecked")
            java.util.List<String> result = (java.util.List<String>) jedis.eval(
                    BUDGET_LUA_SCRIPT,
                    4, // number of keys
                    budgetKey + ":balance_usd",
                    budgetKey + ":alert_threshold_usd",
                    customerKey + ":cost_usd",
                    budgetKey + ":initial_balance_usd",
                    String.valueOf(agg.getCostUsd()),
                    String.valueOf(DEFAULT_ALERT_THRESHOLD_PERCENT),
                    String.valueOf(agg.getTotalTokens()),
                    String.valueOf(agg.getEventCount())
            );

            String status = result.get(0);
            if ("NONE".equals(status)) {
                return; // No budget configured
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
