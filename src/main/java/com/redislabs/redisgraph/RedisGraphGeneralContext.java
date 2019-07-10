package com.redislabs.redisgraph;

import redis.clients.jedis.Jedis;

public interface RedisGraphGeneralContext extends RedisGraph {

    /**
     * Generate a connection bounded api
     * @return a connection bounded api
     */
    RedisGraphContexted getContextedAPI();

}
