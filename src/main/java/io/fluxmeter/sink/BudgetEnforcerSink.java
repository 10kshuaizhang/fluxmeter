package io.fluxmeter.sink;

import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.util.TenantKeys;
import io.fluxmeter.util.BillingPeriod;

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
import java.util.concurrent.TimeUnit;

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

    // KEYS[1..23]=atomic counters/budget, [24]=period volume,
    // [25]=month rollup hash, [26]=day rollup hash.
    private static final String SINK_LUA_SCRIPT =
            "if redis.call('SET', KEYS[1], '1', 'NX', 'EX', '2592000') == false then\n" +
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
            "redis.call('INCRBY', KEYS[24], ARGV[3])\n" +
            "local function rollup(hk)\n" +
            " redis.call('HINCRBY', hk, 'input_tokens', ARGV[1]); redis.call('HINCRBY', hk, 'output_tokens', ARGV[2]); redis.call('HINCRBY', hk, 'total_tokens', ARGV[3]); redis.call('HINCRBY', hk, 'event_count', ARGV[4]); redis.call('HINCRBYFLOAT', hk, 'cost_usd', ARGV[5]); redis.call('EXPIRE', hk, 34560000)\n" +
            " if tonumber(ARGV[6]) > 0 then redis.call('HINCRBY', hk, 'cache_read_tokens', ARGV[6]) end; if tonumber(ARGV[7]) > 0 then redis.call('HINCRBY', hk, 'reasoning_tokens', ARGV[7]) end\n" +
            "end\n" +
            "rollup(KEYS[25]); rollup(KEYS[26])\n" +
            "for _,rid in ipairs(redis.call('SMEMBERS', KEYS[27])) do\n" +
            " local rk='reservation:'..rid; local reserved=tonumber(redis.call('HGET', rk, 'reserved_usd') or '0'); local held=tonumber(redis.call('GET', KEYS[28]) or '0'); local release=math.min(held, reserved)\n" +
            " if release > 0 then redis.call('INCRBYFLOAT', KEYS[28], -release) end\n" +
            " local parent=redis.call('HGET', rk, 'parent_span_id') or ''; if parent ~= '' then local sk='span:'..parent..':held_usd'; local sh=tonumber(redis.call('GET', sk) or '0'); if sh > 0 then redis.call('INCRBYFLOAT', sk, -math.min(sh, release)) end end\n" +
            " redis.call('DEL', rk); redis.call('ZREM', KEYS[29], rid)\n" +
            "end\n" +
            "redis.call('DEL', KEYS[27])\n" +
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
        props.put(ProducerConfig.ACKS_CONFIG, "all");
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
            String periodKey = BillingPeriod.periodVolumeKey(
                    agg.getTenantId(), customerId, agg.getModelId(), agg.getWindowEnd());
            String rollupBase = "rollup:" + customerId;
            String reservationSetKey = TenantKeys.windowReservationsKey(windowId);

            @SuppressWarnings("unchecked")
            java.util.List<String> result = (java.util.List<String>) jedis.eval(
                    SINK_LUA_SCRIPT,
                    29,
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
                    periodKey,
                    rollupBase + ":period:" + BillingPeriod.monthUtc(agg.getWindowEnd()),
                    rollupBase + ":d:" + BillingPeriod.dayUtc(agg.getWindowEnd()),
                    reservationSetKey,
                    budgetKey + ":held_usd",
                    "gateway:reservations:pending",
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
            double previousBalance = Double.parseDouble(result.get(2));
            String initialRaw = jedis.get(budgetKey + ":initial_balance_usd");
            if (initialRaw != null) {
                double initial = Double.parseDouble(initialRaw);
                for (int warnPct : new int[] {70, 90}) {
                    double threshold = initial * (1.0 - warnPct / 100.0);
                    if (previousBalance > threshold && newBalance <= threshold) {
                        emitAlert(customerId, "BUDGET_WARN", newBalance, agg, warnPct, initial);
                    }
                }
            }
            if ("EXHAUSTED".equals(status)) {
                emitAlert(customerId, "BUDGET_EXHAUSTED", newBalance, agg);
            } else if ("LOW".equals(status)) {
                emitAlert(customerId, "BUDGET_LOW", newBalance, agg);
            }
        }
    }

    private void emitAlert(String customerId, String alertType, double remainingBalance,
                           UsageAggregate agg) {
        emitAlert(customerId, alertType, remainingBalance, agg, null, null);
    }

    private void emitAlert(String customerId, String alertType, double remainingBalance,
                           UsageAggregate agg, Integer warnPct, Double initialBalance) {
        String value = null;
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
            if (warnPct != null) {
                alert.put("warnPct", warnPct);
                if (initialBalance != null) {
                    alert.put("initialBalanceUsd", initialBalance);
                    alert.put("spentPct", initialBalance <= 0 ? 100.0
                            : Math.min(100.0, Math.max(0.0,
                                    ((initialBalance - remainingBalance) / initialBalance) * 100.0)));
                }
            }

            value = mapper.writeValueAsString(alert);
            alertProducer.send(new ProducerRecord<>(alertTopic, customerId, value))
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Accounting is authoritative; record a durable retry payload instead of
            // losing the transition when Kafka is temporarily unavailable.
            try (Jedis jedis = pool.getResource()) {
                Map<String, Object> pending = new HashMap<>();
                pending.put("topic", alertTopic);
                pending.put("key", customerId);
                pending.put("payload", value == null ? "{}" : value);
                jedis.rpush("budget-alerts:pending", mapper.writeValueAsString(pending));
            } catch (Exception ignored) {
                // Redis failure will fail the next accounting invocation/readiness probe.
            }
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
