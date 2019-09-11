package com.redislabs.redisgraph.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;


public class JRedisGraphCompileTimeError extends JedisDataException {
    public JRedisGraphCompileTimeError(String message) {
        super(message);
    }

    public JRedisGraphCompileTimeError(Throwable cause) {
        super(cause);
    }

    public JRedisGraphCompileTimeError(String message, Throwable cause) {
        super(message, cause);
    }
}
