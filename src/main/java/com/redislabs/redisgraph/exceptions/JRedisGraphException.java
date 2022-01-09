package com.redislabs.redisgraph.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * RedisGraph query evaluation exception. An instance of JRedisGraphException is
 * thrown when RedisGraph encounters an error during query evaluation.
 */
public class JRedisGraphException extends JedisDataException {
  private static final long serialVersionUID = -476099681322055468L;

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
