package com.redislabs.redisgraph;

import com.redislabs.redisgraph.impl.ResultSetImpl;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.ProtocolCommand;

public class RedisGraphAPI {

    final private JedisPool client;
    final private String graphId;

    public RedisGraphAPI(String graphId) {
        this(graphId, "localhost", 6379);
    }
    
    public RedisGraphAPI(String graphId, String host, int port) {
        this.graphId = graphId;
        this.client = new JedisPool(host, port);
    }

    public ResultSet query(String query) {
    	 try (Jedis conn = _conn()) {
             return new ResultSetImpl(sendCommand(conn, Commands.Command.QUERY, graphId, query).getObjectMultiBulkReply());
         }
    }

    public void deleteGraph() {
    	_conn().flushDB();
    }

    private BinaryClient sendCommand(Jedis conn, ProtocolCommand provider, String ...args) {
        BinaryClient client = conn.getClient();
        client.sendCommand(provider, args);
        return client;
    }
    
    private Jedis _conn() {
        return this.client.getResource();
    }
}
