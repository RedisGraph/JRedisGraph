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
            args.add(attr);
        }

        String[] stringArgs = args.toArray(new String[args.size()]);

        String edgeId = conn.getClient()
                .sendCommand(Commands.Command.ADDEDGE, stringArgs)
                .getBulkReply();
        conn.close();
        return edgeId;
    }

    public HashMap<String, String> getNode(String graphId, String id) {
        Jedis conn = _conn();

        List<String> args = new ArrayList<String>(2);
        args.add(graphId);
        args.add(id);

        String[] stringArgs = args.toArray(new String[args.size()]);

        List<String> properties = conn.getClient()
                .sendCommand(Commands.Command.GETNODE, stringArgs)
                .getMultiBulkReply();

        HashMap<String, String> attributes = new HashMap<String, String>(properties.size()/2);

        for(int i = 0; i < properties.size(); i+=2) {
            String key = properties.get(i) ;
            String value = properties.get(i+1);
            attributes.put(key, value);
        }

        conn.close();
        return attributes;
    }

    public HashMap<String, String> getEdge(String graphId, String id) {
        Jedis conn = _conn();

        List<String> args = new ArrayList<String>(2);
        args.add(graphId);
        args.add(id);

        String[] stringArgs = args.toArray(new String[args.size()]);

        List<String> properties = conn.getClient()
                .sendCommand(Commands.Command.GETEDGE, stringArgs)
                .getMultiBulkReply();

        HashMap<String, String> attributes = new HashMap<String, String>(properties.size()/2);

        for(int i = 0; i < properties.size(); i+=2) {
            String key = properties.get(i) ;
            String value = properties.get(i+1);
            attributes.put(key, value);
        }

        conn.close();
        return attributes;
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

        return neighbours;
    }


    public ResultSet query(String graphId, String query) {
        Jedis conn = _conn();

        List<Object> resp = conn.getClient()
                .sendCommand(Commands.Command.QUERY, graphId, query)
                .getObjectMultiBulkReply();

        return new ResultSet(resp);
    }

    public boolean setProperty(String elementId, String key, Object value) {
        Jedis conn = _conn();
        return conn.hset(elementId, key, value.toString()) == 1;
    }
}
