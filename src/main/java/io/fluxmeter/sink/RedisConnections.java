package io.fluxmeter.sink;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/** Shared Redis pool factory — honors REDIS_PASSWORD when set. */
public final class RedisConnections {

    private RedisConnections() {}

    public static JedisPool createPool(String host, int port, int maxTotal) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        String password = System.getenv("REDIS_PASSWORD");
        if (password != null && !password.isEmpty()) {
            return new JedisPool(config, host, port, 2000, password);
        }
        return new JedisPool(config, host, port);
    }
}
