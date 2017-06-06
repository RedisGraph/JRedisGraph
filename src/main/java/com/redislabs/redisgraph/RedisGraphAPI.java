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
        List<RedisNode> node = getNodes(id);
        if(node != null && node.size() == 1) {
            return node.get(0);
        }
        return null;
    }

    public List<RedisNode> getNodes(Object... ids) {
        List<RedisNode> res = new ArrayList<RedisNode>();
        List<HashMap<String, String>> nodes = client.getNodes(this.graphId, ids);

        if(nodes == null) {
            return null;
        }

        // Scan through raw results
        for(HashMap<String, String> nodeAttributs: nodes) {

            String id = nodeAttributs.get("id");
            nodeAttributs.remove("id");

            String label = nodeAttributs.get("label");
            nodeAttributs.remove("label");

            res.add(new RedisNode(id, label, nodeAttributs));
        }

        return res;
    }

    public RedisEdge getEdge(String id) {
        List<RedisEdge> edge = getEdges(id);
        if(edge != null && edge.size() == 1) {
            return edge.get(0);
        }
        return null;
    }

    public List<RedisEdge> getEdges(String... ids) {
        List<RedisEdge> res = new ArrayList<RedisEdge>();
        List<HashMap<String, String>> edges = client.getEdges(this.graphId, ids);

        if(edges == null) {
            return  null;
        }

        for(HashMap<String, String> edgeAttributs: edges) {
            String edgeId = edgeAttributs.get("id");
            String relation = edgeAttributs.get("label");
            String srcNodeId = edgeAttributs.get("src");
            String destNodeId = edgeAttributs.get("dest");

            edgeAttributs.remove("id");
            edgeAttributs.remove("type");
            edgeAttributs.remove("src");
            edgeAttributs.remove("dest");
            edgeAttributs.remove("label");

            List<RedisNode> nodes = getNodes(srcNodeId, destNodeId);
            RedisNode srcNode = nodes.get(0);
            RedisNode destNode = nodes.get(1);

            res.add(new RedisEdge(edgeId, srcNode, destNode, relation, edgeAttributs));
        }
        return res;
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
        return this.getNodes(nodeIds.toArray());
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
