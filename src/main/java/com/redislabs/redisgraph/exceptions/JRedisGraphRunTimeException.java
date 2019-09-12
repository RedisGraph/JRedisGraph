package com.redislabs.redisgraph.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * RedisGraph runtime exception. An instance of JRedisGraphRunTimeException is thrown when RedisGraph
 * encounters a runtime error during query execution.
 */
public class JRedisGraphRunTimeException extends JedisDataException {
    public JRedisGraphRunTimeException(String message) {
        super(message);
    }

    public JRedisGraphRunTimeException(Throwable cause) {
        super(cause);
    }

    public JRedisGraphRunTimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
