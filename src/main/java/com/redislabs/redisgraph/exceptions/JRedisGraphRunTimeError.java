package com.redislabs.redisgraph.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;


public class JRedisGraphRunTimeError extends JedisDataException {
    public JRedisGraphRunTimeError(String message) {
        super(message);
    }

    public JRedisGraphRunTimeError(Throwable cause) {
        super(cause);
    }

    public JRedisGraphRunTimeError(String message, Throwable cause) {
        super(message, cause);
    }
}
