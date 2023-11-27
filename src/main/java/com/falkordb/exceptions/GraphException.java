package com.falkordb.exceptions;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Graph query evaluation exception. An instance of GraphException is
 * thrown when Graph encounters an error during query evaluation.
 */
public class GraphException extends JedisDataException {
  private static final long serialVersionUID = -476099681322055468L;

  public GraphException(String message) {
    super(message);
  }

  public GraphException(Throwable cause) {
    super(cause);
  }

  public GraphException(String message, Throwable cause) {
    super(message, cause);
  }
}
