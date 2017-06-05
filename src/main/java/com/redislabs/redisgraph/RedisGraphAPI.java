package com.redislabs.redisgraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisGraphAPI {

    Client client;
    String graphId;

    public RedisGraphAPI(String graphId) {
        this.graphId = graphId;
        client = new Client("localhost", 6379);
    }

    public RedisNode createNode(Object... attributes) {
        String id = client.createNode(this.graphId, attributes);

        Map<String, String> attr = new HashMap<String, String>();

        for(int i = 0; i < attributes.length; i+=2) {
            String key = attributes[i].toString();
            String value = attributes[i+1].toString();
            attr.put(key, value);
        }

        return new RedisNode(id, null, attr);
    }

    public RedisNode createLabeledNode(String label, Object... attributes) {
        String id = client.createNode(this.graphId, label, attributes);

        Map<String, String> attr = new HashMap<String, String>();

        for(int i = 0; i < attributes.length; i+=2) {
            String key = attributes[i].toString();
            String value = attributes[i+1].toString();
            attr.put(key, value);
        }

        return new RedisNode(id, label, attr);
    }

    public RedisNode getNode(String id) {
        HashMap<String, String> attributes = client.getNode(this.graphId, id);

        if(attributes == null) {
            return null;
        }

        String label = attributes.get("label");
        attributes.remove("label");

        return new RedisNode(id, label, attributes);
    }

    public RedisEdge getEdge(String id) {
        HashMap<String, String> attributes = client.getEdge(this.graphId, id);

        if(attributes == null) {
            return  null;
        }

        String edgeId = attributes.get("id");
        String relation = attributes.get("type");
        String srcNodeId = attributes.get("src");
        String destNodeId = attributes.get("dest");

        attributes.remove("id");
        attributes.remove("type");
        attributes.remove("src");
        attributes.remove("dest");

        RedisNode srcNode = getNode(srcNodeId);
        RedisNode destNode = getNode(destNodeId);

        return new RedisEdge(edgeId, srcNode, destNode, relation, attributes);
    }

    public List<RedisEdge> getNodeEdges(String nodeId, String edgeType, int direction) {
        List<String> edgeIds = client.getNodeEdges(this.graphId, nodeId, edgeType, direction);
        ArrayList<RedisEdge> edges = new ArrayList<RedisEdge>();

        for(String id: edgeIds) {
            edges.add(getEdge(id));
        }

        return edges;
    }

    public List<RedisNode> getNeighbours(String nodeId, String edgeType, int direction) {
        List<String> nodeIds = client.getNeighbours(this.graphId, nodeId, edgeType, direction);
        ArrayList<RedisNode> nodes = new ArrayList<RedisNode>();

        for(String id: nodeIds) {
            nodes.add(getNode(id));
        }

        return nodes;
    }

    public RedisEdge connectNodes(RedisNode src, String relation, RedisNode dest, Object... attributes) {
        String edgeId = client.connectNodes(this.graphId, src.getId(), relation, dest.getId(), attributes);
        HashMap<String, String> attr = new HashMap<String, String>();

        for(int i = 0; i < attributes.length; i+=2) {
            String key = attributes[i].toString();
            String value = attributes[i+1].toString();
            attr.put(key, value);
        }

        return new RedisEdge(edgeId, src, dest, relation, attr);
    }

    public ResultSet query(String query) {
        return client.query(this.graphId, query);
    }

    public boolean setProperty(String elementId, String key, Object value) {
        return client.setProperty(elementId, key, value);
    }

    public void deleteGraph() {
        client.deleteGraph(this.graphId);
    }
}
