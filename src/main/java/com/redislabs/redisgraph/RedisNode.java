package com.redislabs.redisgraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisNode {
    String id;
    String label;
    Map<String, String> attributes;

    List<RedisEdge> incomingEdges;
    List<RedisEdge> outgoingEdges;

    public RedisNode(String id, String label, Map<String, String> attributes) {
        this.id = id;
        this.label = label;
        this.attributes = attributes;
        this.incomingEdges = new ArrayList<RedisEdge>();
        this.outgoingEdges = new ArrayList<RedisEdge>();
    }

    public String getId(){
        return this.id;
    }
}
