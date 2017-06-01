package com.redislabs.redisgraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisNode {
    private String id;
    private String label;
    private Map<String, String> attributes;

    private List<RedisEdge> incomingEdges;
    private List<RedisEdge> outgoingEdges;

    public RedisNode(String id, String label, Map<String, String> attributes) {
        this.id = id;
        this.label = label;
        this.attributes = attributes;
        this.incomingEdges = new ArrayList<RedisEdge>();
        this.outgoingEdges = new ArrayList<RedisEdge>();
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public List<RedisEdge> getIncomingEdges() {
        return incomingEdges;
    }

    public List<RedisEdge> getOutgoingEdges() {
        return outgoingEdges;
    }
}
