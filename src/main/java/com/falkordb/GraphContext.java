package com.falkordb;

import redis.clients.jedis.Jedis;

public interface GraphContext extends Graph {


    /**
     * Returns implementing class connection context
     * @return Jedis connection
     */
    Jedis getConnectionContext();

    /**
     * Returns a Redis transactional object, over the connection context, with graph API capabilities
     * @return Redis transactional object, over the connection context, with graph API capabilities
     */
    GraphTransaction multi();
    
    /**
     * Returns a Redis pipeline object, over the connection context, with graph API capabilities
     * @return Redis pipeline object, over the connection context, with graph API capabilities
     */
    GraphPipeline pipelined();

    /**
     * Perform watch over given Redis keys
     * @param keys
     * @return "OK"
     */
    String watch(String... keys);

    /**
     * Removes watch from all keys
     * @return
     */
    String unwatch();
}
