package com.redislabs.redisgraph;

import com.redislabs.redisgraph.impl.ResultSetImpl;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * RedisGraph client
 */
public class RedisGraphAPI {

    final private JedisPool client;
    final private String graphId;

    /**
     * Creates a client to a specific graph running on the local machine
     * 
     * @param graphId the graph id
     */
    public RedisGraphAPI(String graphId) {
        this(graphId, "localhost", 6379);
    }
    
    /**
     * Creates a client to a specific graph running on the specific host/post
     * 
     * @param graphId the graph id
     * @param host Redis host
     * @param port Redis port
     */
    public RedisGraphAPI(String graphId, String host, int port) {
        this.graphId = graphId;
        this.client = new JedisPool(host, port);
    }

    /**
     * Execute a Cypher query
     * 
     * @param query Cypher query
     * @return a result set 
     */
    public ResultSet query(String query) {
    	 try (Jedis conn = _conn()) {
             return new ResultSetImpl(sendCommand(conn, Command.QUERY, graphId, query).getObjectMultiBulkReply());
         }
    }
    
    /**
     * Deletes the entire graph
     * 
     * @return delete running time statistics 
     */
    public String deleteGraph() {
		  try (Jedis conn = _conn()) {
		    return sendCommand(conn, Command.DELETE, graphId).getBulkReply();
		  }
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
