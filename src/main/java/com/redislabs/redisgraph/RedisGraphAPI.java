package com.redislabs.redisgraph;

import java.util.HashMap;
import java.util.Map;

public class RedisGraphAPI {

    Client client;
    String graphID;

    public RedisGraphAPI(String graphID) {
        this.graphID = graphID;
        client = new Client("localhost", 6379);
    }

    public RedisNode createNode(Object... attributes) {
        String id = client.createNode(this.graphID, attributes);

        Map<String, String> attr = new HashMap<String, String>();

        for(int i = 0; i < attributes.length; i+=2) {
            String key = attributes[i].toString();
            String value = attributes[i+1].toString();
            attr.put(key, value);
        }

        return new RedisNode(id, null, attr);
    }

    public RedisNode createLabeledNode(String label, Object... attributes) {
        String id = client.createNode(this.graphID, label, attributes);

        Map<String, String> attr = new HashMap<String, String>();

        for(int i = 0; i < attributes.length; i+=2) {
            String key = attributes[i].toString();
            String value = attributes[i+1].toString();
            attr.put(key, value);
        }

        return new RedisNode(id, label, attr);
    }

    public RedisNode getNode(String id) {
        Map<String, String> attributes = client.getNode(id);
        return new RedisNode(id, null, attributes);
    }

    public RedisEdge connectNodes(RedisNode src, String relation, RedisNode dest, Object... attributes) {
        String edgeId = client.connectNodes(this.graphID, src.id, relation, dest.id, attributes);
        Map<String, String> attr = new HashMap<String, String>();

        for(int i = 0; i < attributes.length; i+=2) {
            String key = attributes[i].toString();
            String value = attributes[i+1].toString();
            attr.put(key, value);
        }

        return new RedisEdge(edgeId, src, dest, relation, attr);
    }

    public ResultSet query(String query) {
        return client.query(this.graphID, query);
    }
}
