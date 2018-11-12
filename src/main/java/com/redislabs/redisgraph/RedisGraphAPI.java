package com.redislabs.redisgraph;

import com.redislabs.redisgraph.impl.ResultSetImpl;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;

/**
 * RedisGraph client
 */
public class RedisGraphAPI {

	private final Pool<Jedis> client;
    private final String graphId;

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
        this(graphId, new JedisPool(host, port));
    }
    
    /**
     * Creates a client to a specific graph using provided Jedis pool
     * 
     * @param graphId the graph id
     * @param jedis bring your own Jedis pool
     */
    public RedisGraphAPI(String graphId, Pool<Jedis> jedis) {
        this.graphId = graphId;
        this.client = jedis;
    }

    /**
     * Execute a Cypher query
     * 
     * @param query Cypher query
     * @return a result set 
     */
    public ResultSet query(String query) {
    	 try (Jedis conn = getConnection()) {
             return new ResultSetImpl(sendCommand(conn, Command.QUERY, graphId, query).getObjectMultiBulkReply());
         }
    }
    
    /**
     * Deletes the entire graph
     * 
     * @return delete running time statistics 
     */
    public String deleteGraph() {
		  try (Jedis conn = getConnection()) {
		    return sendCommand(conn, Command.DELETE, graphId).getBulkReply();
		  }
	  }
   

    private BinaryClient sendCommand(Jedis conn, ProtocolCommand provider, String ...args) {
        BinaryClient binaryClient = conn.getClient();
        binaryClient.sendCommand(provider, args);
        return binaryClient;
    }
    
    private Jedis getConnection() {
        return this.client.getResource();
    }
}
