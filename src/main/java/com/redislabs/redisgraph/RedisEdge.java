package com.redislabs.redisgraph;

import java.util.Map;

public class RedisEdge {
    String id;
    String relation;
    Map<String, String> attributes;
    RedisNode src;
    RedisNode dest;

    public RedisEdge(String id, RedisNode src, RedisNode dest, String relation, Map<String, String> attributes) {
        this.id = id;
        this.relation = relation;
        this.attributes = attributes;
        this.src = src;
        this.dest = dest;
    }
}
