package com.redislabs.redisgraph;

import java.util.HashMap;

public class RedisEdge {
    private String id;
    private String relation;
    private HashMap<String, String> attributes;
    private RedisNode src;
    private RedisNode dest;

    public RedisEdge(String id, RedisNode src, RedisNode dest, String relation, HashMap<String, String> attributes) {
        this.id = id;
        this.relation = relation;
        this.attributes = attributes;
        this.src = src;
        this.dest = dest;
    }

    public String getId() {
        return id;
    }

    public String getRelation() {
        return relation;
    }

    public HashMap<String, String> getAttributes() {
        return attributes;
    }

    public RedisNode getSrc() {
        return src;
    }

    public RedisNode getDest() {
        return dest;
    }
}
