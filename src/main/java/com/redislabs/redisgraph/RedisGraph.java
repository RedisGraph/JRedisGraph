package com.redislabs.redisgraph;

import com.redislabs.redisgraph.impl.graph_cache.GraphCache;
import com.redislabs.redisgraph.impl.ResultSetImpl;
import org.apache.commons.text.translate.AggregateTranslator;
import org.apache.commons.text.translate.CharSequenceTranslator;
import org.apache.commons.text.translate.LookupTranslator;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;

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



    private static final CharSequenceTranslator ESCAPE_CHYPER;
    static {
        final Map<CharSequence, CharSequence> escapeJavaMap = new HashMap<>();
        escapeJavaMap.put("\'", "\\'");
        escapeJavaMap.put("\"", "\\\"");
        ESCAPE_CHYPER = new AggregateTranslator(new LookupTranslator(Collections.unmodifiableMap(escapeJavaMap)));
    }

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
        if(args.length > 0) {
            for(int i=0; i<args.length; ++i) {
                if(args[i] instanceof String) {
                    args[i] = "\'" + ESCAPE_CHYPER.translate((String)args[i]) + "\'";
                }
            }
            query = String.format(query, args);
        }
        graphCaches.putIfAbsent(graphId, new GraphCache(graphId, this));
        List<Object> rawResponse = null;
        try(Jedis conn = getConnection()){
            rawResponse= sendCompactCommand(conn, Command.QUERY, graphId, query).getObjectMultiBulkReply();
        }
        return new ResultSetImpl(rawResponse, graphCaches.get(graphId));

    }

    /**
     * Invokes stored procedures without arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return result set with the procedure data
     */
    public ResultSet callProcedure(String graphId, String procedure  ){
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
     *
     * @return delete running time statistics
     */
    public String deleteGraph(String graphId) {
        //clear local state
        graphCaches.remove(graphId);
        try (Jedis conn = getConnection()) {
            return sendCommand(conn, Command.DELETE, graphId).getBulkReply();
        }

    }


    /**
     * Sends command - will be replaced with sendCompactCommand once graph.delete support --compact flag
     * @param conn - connection
     * @param provider - command type
     * @param args - command arguments
     * @return
     */
    private BinaryClient sendCommand(Jedis conn, ProtocolCommand provider, String ...args) {
        BinaryClient binaryClient = conn.getClient();
        binaryClient.sendCommand(provider, args);
        return binaryClient;
    }


    /**
     * Sends the command with --COMPACT flag
     * @param conn - connection
     * @param provider - command type
     * @param args - command arguments
     * @return
     */
    private BinaryClient sendCompactCommand(Jedis conn, ProtocolCommand provider, String ...args) {
        String[] t = new String[args.length +1];
        System.arraycopy(args, 0 , t, 0, args.length);
        t[args.length]="--COMPACT";
        return sendCommand(conn, provider, t);
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
     * @return
     */
    public ResultSet callProcedure(String graphId, String procedure, List<String> args  , Map<String, List<String>> kwargs ){

        args = args.stream().map( s -> Utils.quoteString(s)).collect(Collectors.toList());
        StringBuilder queryString =  new StringBuilder();
        queryString.append(String.format("CALL %s(%s)", procedure, String.join(",", args)));
        List<String> kwargsList = kwargs.getOrDefault("y", null);
        if(kwargsList != null){
            queryString.append(String.join(",", kwargsList));
        }
        return query(graphId, queryString.toString());
    }
}
