package com.redislabs.redisgraph.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * RedisGraph query syntax evaluation exception. An instance of JRedisGraphRunTimeException is thrown when RedisGraph
 * encounters an error during query syntax evaluation.
 */
public class JRedisGraphCompileTimeException extends JedisDataException {
    public JRedisGraphCompileTimeException(String message) {
        super(message);
    }

    public JRedisGraphCompileTimeException(Throwable cause) {
        super(cause);
    }

    public JRedisGraphCompileTimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
