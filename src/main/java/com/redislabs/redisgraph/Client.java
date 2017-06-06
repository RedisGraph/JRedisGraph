package com.redislabs.redisgraph;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Client {
    private JedisPool pool;

    Jedis _conn() {
        return pool.getResource();
    }

    public Client(String host, int port) {
        pool = new JedisPool(host, port);
    }

    public String createNode(String graph, String label, Object... attributes) {
        Jedis conn = _conn();

        List<String> args = new ArrayList<String>(2 + attributes.length);
        args.add(graph);
        args.add(label);

        for(Object attr: attributes) {
            args.add(attr.toString());
        }

        String[] stringArgs = args.toArray(new String[args.size()]);

        String nodeId = conn.getClient()
                .sendCommand(Commands.Command.CREATENODE, stringArgs)
                .getBulkReply();

        conn.close();
        return nodeId;
    }

    public String createNode(String graph, Object... attributes) {
        Jedis conn = _conn();

        List<String> args = new ArrayList<String>(1 + attributes.length);
        args.add(graph);

        for(Object attr: attributes) {
            args.add(attr.toString());
        }

        String[] stringArgs = args.toArray(new String[args.size()]);

        String nodeId = conn.getClient()
                .sendCommand(Commands.Command.CREATENODE, stringArgs)
                .getBulkReply();

        conn.close();
        return nodeId;
    }

    public String connectNodes(String graph, String srcNodeID, String relation, String destNodeID, Object... attributes) {
        Jedis conn = _conn();

        List<Object> args = new ArrayList<Object>(4 + attributes.length);
        args.add(graph);
        args.add(srcNodeID);
        args.add(relation);
        args.add(destNodeID);

        for(Object attr: attributes) {
            args.add(attr.toString());
        }

        String[] stringArgs = args.toArray(new String[args.size()]);

        String edgeId = conn.getClient()
                .sendCommand(Commands.Command.ADDEDGE, stringArgs)
                .getBulkReply();

        conn.close();
        return edgeId;
    }

    public List<HashMap<String, String>> getNodes(String graphId, Object... ids) {
        Jedis conn = _conn();

        List<String> args = new ArrayList<String>(2);
        args.add(graphId);
        for(Object id: ids) {
            args.add(id.toString());
        }

        String[] stringArgs = args.toArray(new String[args.size()]);
        List<String> replay;
        try {
            replay = conn.getClient()
                    .sendCommand(Commands.Command.GETNODES, stringArgs)
                    .getMultiBulkReply();
        } catch(ClassCastException e) {
            return null;
        }

        List<HashMap<String, String>> nodes = new ArrayList<HashMap<String, String>>();

        int numberOfNodes = Integer.parseInt(replay.get(replay.size()-1));
        int offset = 0;

        for(int i = 0; i < numberOfNodes; i++) {
            int numberOfProperties = Integer.parseInt(replay.get(offset));
            offset++;

            HashMap<String, String> nodeAttributes = new HashMap<String, String>(numberOfProperties/2);
            nodes.add(nodeAttributes);
            for(int j = 0; j < numberOfProperties; j+=2) {
                String key = replay.get(offset + j);
                String value = replay.get(offset + j + 1);
                nodeAttributes.put(key, value);
            }

            offset += numberOfProperties;
        }

        conn.close();
        return nodes;
    }

    public List<HashMap<String, String>> getEdges(String graphId, Object... ids) {
        Jedis conn = _conn();

        List<String> args = new ArrayList<String>(2);
        args.add(graphId);
        for(Object id : ids) {
            args.add(id.toString());
        }

        String[] stringArgs = args.toArray(new String[args.size()]);
        List<String> replay;

        try {
            replay = conn.getClient()
                    .sendCommand(Commands.Command.GETEDGES, stringArgs)
                    .getMultiBulkReply();
        } catch (ClassCastException e) {
            return null;
        }

        List<HashMap<String, String>> edges = new ArrayList<HashMap<String, String>>();

        int numberOfEdges = Integer.parseInt(replay.get(replay.size()-1));
        int offset = 0;

        for(int i = 0; i < numberOfEdges; i++) {
            int numberOfProperties = Integer.parseInt(replay.get(offset));
            offset++;

            HashMap<String, String> edgeAttributes = new HashMap<String, String>(numberOfProperties/2);
            edges.add(edgeAttributes);
            for(int j = 0; j < numberOfProperties; j+=2) {
                String key = replay.get(offset + j);
                String value = replay.get(offset + j + 1);
                edgeAttributes.put(key, value);
            }

            offset += numberOfProperties;
        }

        conn.close();
        return edges;
    }

    public List<String> getNodeEdges(String graphId, String nodeId, String edgeType, int direction) {
        Jedis conn = _conn();
        List<String> args = new ArrayList<String>(4);
        args.add(graphId);
        args.add(nodeId);
        args.add(edgeType);
        args.add(String.valueOf(direction));

        String[] stringArgs = args.toArray(new String[args.size()]);

        List<String> edges = conn.getClient()
                .sendCommand(Commands.Command.GETNODEEDGES, stringArgs)
                .getMultiBulkReply();

        conn.close();
        return edges;
    }

    public List<String> getNeighbours(String graphId, String nodeId, String edgeType, int direction) {
        Jedis conn = _conn();
        List<String> args = new ArrayList<String>(4);
        args.add(graphId);
        args.add(nodeId);
        args.add(edgeType);
        args.add(String.valueOf(direction));

        String[] stringArgs = args.toArray(new String[args.size()]);

        List<String> neighbours = conn.getClient()
                .sendCommand(Commands.Command.GETNEIGHBOURS, stringArgs)
                .getMultiBulkReply();

        conn.close();
        return neighbours;
    }

    public ResultSet query(String graphId, String query) {
        Jedis conn = _conn();

        List<Object> resp = conn.getClient()
                .sendCommand(Commands.Command.QUERY, graphId, query)
                .getObjectMultiBulkReply();

        conn.close();
        return new ResultSet(resp);
    }

    public boolean setProperty(String elementId, String key, Object value) {
        Jedis conn = _conn();
        return conn.hset(elementId, key, value.toString()) == 1;
    }

    public void deleteGraph(String graph) {
        Jedis conn = _conn();
        conn.getClient()
                .sendCommand(Commands.Command.DELETEGRAPH, graph)
                .getStatusCodeReply();
        conn.close();
    }
}
