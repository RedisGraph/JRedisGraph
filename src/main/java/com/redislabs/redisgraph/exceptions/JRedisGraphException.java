package com.redislabs.redisgraph.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * RedisGraph query evaluation exception. An instance of JRedisGraphException is thrown when RedisGraph
 * encounters an error during query evaluation.
 */
public class JRedisGraphException extends JedisDataException {
    public JRedisGraphException(String message) {
        super(message);
    }

    public JRedisGraphException(Throwable cause) {
        super(cause);
    }

    public JRedisGraphException(String message, Throwable cause) {
        super(message, cause);
    }
}
