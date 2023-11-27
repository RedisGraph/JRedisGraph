package com.falkordb;

public interface GraphContextGenerator extends Graph {

    /**
     * Generate a connection bounded api
     * @return a connection bounded api
     */
    GraphContext getContext();

}
