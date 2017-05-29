package com.redislabs.redisgraph;

import com.sun.org.apache.xerces.internal.xs.datatypes.ObjectList;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private Map<String, String> getGraphEntity(String id) {
        Jedis conn = _conn();
        Map<String, String> properties = conn.hgetAll(id);
        conn.close();
        return properties;
    }

    public Map<String, String> getNode(String id) {
        return getGraphEntity(id);
    }

    public Map<String, String> getEdge(String id) {
        return getGraphEntity(id);
    }

    public ResultSet query(String graphID, String query) {
        Jedis conn = _conn();

        List<Object> resp = conn.getClient()
                .sendCommand(Commands.Command.QUERY, graphID, query)
                .getObjectMultiBulkReply();

        return new ResultSet(resp);
    }
}
