package com.redislabs.redisgraph;

import com.redislabs.redisgraph.impl.JRedisGraphTransaction;
import com.redislabs.redisgraph.impl.graph_cache.GraphCache;
import com.redislabs.redisgraph.impl.ResultSetImpl;
import org.apache.commons.text.translate.AggregateTranslator;
import org.apache.commons.text.translate.CharSequenceTranslator;
import org.apache.commons.text.translate.LookupTranslator;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 *
 */
public class RedisGraph implements Closeable {



    private final Pool<Jedis> client;
    private final Map<String, GraphCache> graphCaches = new ConcurrentHashMap<>();

    /**
     * Creates a client running on the local machine

     */
    public RedisGraph() {
        this("localhost", 6379);
    }

    /**
     * Creates a client running on the specific host/post
     *
     * @param host Redis host
     * @param port Redis port
     */
    public RedisGraph(String host, int port) {
        this( new JedisPool(host, port));
    }

    /**
     * Creates a client using provided Jedis pool
     *
     * @param jedis bring your own Jedis pool
     */
    public RedisGraph( Pool<Jedis> jedis) {

        this.client = jedis;
    }

    @Override
    public void close(){
        this.client.close();
    }


    /**
     * Execute a Cypher query with arguments
     *
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return a result set
     */
    public ResultSet query(String graphId, String query, Object ...args) {
        String preparedQuery = Utils.prepareQuery(query, args);
        graphCaches.putIfAbsent(graphId, new GraphCache(graphId, this));
        List<Object> rawResponse = null;
        try(Jedis conn = getConnection()){
            rawResponse = (List<Object>) conn.sendCommand(Command.QUERY, graphId, preparedQuery, "--COMPACT");
        }
        return new ResultSetImpl(rawResponse, graphCaches.get(graphId));
    }

    /**
     * Invokes stored procedures without arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return result set with the procedure data
     */
    public ResultSet callProcedure(String graphId, String procedure){
        return callProcedure(graphId, procedure, new ArrayList<>(), new HashMap<>());
    }


    /**
     * Invokes stored procedure with arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @param args procedure arguments
     * @return result set with the procedure data
     */
    public ResultSet callProcedure(String graphId, String procedure, List<String> args  ){
        return callProcedure(graphId, procedure, args, new HashMap<>());
    }

    /**
     * Deletes the entire graph
     * @param graphId graph to delete
     * @return delete running time statistics
     */
    public String deleteGraph(String graphId) {
        //clear local state
        graphCaches.remove(graphId);
        try (Jedis conn = getConnection()) {
            return SafeEncoder.encode((byte[]) conn.sendCommand(Command.DELETE, graphId));
        }

    }

    private Jedis getConnection() {
        return this.client.getResource();
    }


    /**
     * Invoke a stored procedure
     * @param graphId a graph to perform the query on
     * @param procedure - procedure to execute
     * @param args - procedure arguments
     * @param kwargs - procedure output arguments
     * @return result set with the procedure data
     */
    public ResultSet callProcedure(String graphId, String procedure, List<String> args  , Map<String, List<String>> kwargs ){

        String preparedProcedure = Utils.prepareProcedure(procedure, args, kwargs);
        return query(graphId, preparedProcedure);
    }

    public JRedisGraphTransaction multi(){
        Jedis jedis = getConnection();
        Client client = jedis.getClient();
        client.multi();
        client.getOne();
       return new JRedisGraphTransaction(client,this, this.graphCaches);
    }

    public Map<String, GraphCache> getGraphCaches() {
        return graphCaches;
    }
}
