package com.redislabs.redisgraph;

public interface RedisGraphGeneralContext extends RedisGraph {

    /**
     * Generate a connection bounded api
     * @return a connection bounded api
     */
    RedisGraphContexted getContextedAPI();

}
