package com.redislabs.redisgraph;

public interface RedisGraphContextGenerator extends RedisGraph {

    /**
     * Generate a connection bounded api
     * @return a connection bounded api
     */
    RedisGraphContext getContext();

}
