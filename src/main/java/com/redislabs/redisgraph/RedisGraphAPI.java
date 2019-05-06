package com.redislabs.redisgraph;

import java.util.*;
import java.util.stream.Collectors;


import com.redislabs.redisgraph.impl.ResultSetImpl;
import org.apache.commons.text.translate.AggregateTranslator;
import org.apache.commons.text.translate.CharSequenceTranslator;
import org.apache.commons.text.translate.LookupTranslator;

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
    private List<String> labels = new ArrayList<>();
    private List<String> relationshipTypes = new ArrayList<>();
    private List<String> propertyNames = new ArrayList<>();

    private static RedisGraphAPI redisGraphAPI;


    
    private static final CharSequenceTranslator ESCAPE_CHYPER;
    static {
        final Map<CharSequence, CharSequence> escapeJavaMap = new HashMap<>();
        escapeJavaMap.put("\'", "\\'");
        escapeJavaMap.put("\"", "\\\"");
        ESCAPE_CHYPER = new AggregateTranslator(new LookupTranslator(Collections.unmodifiableMap(escapeJavaMap)));
    }

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
        redisGraphAPI = this;

    }

    /**
     * Execute a Cypher query with arguments
     * 
     * @param query Cypher query
     * @param args
     * @return a result set 
     */
    public ResultSet query(String query, Object ...args) {
        StringBuilder sb = new StringBuilder();
      if(args.length > 0) {
        for(int i=0; i<args.length; ++i) {
          if(args[i] instanceof String) {
            args[i] = "\'" + ESCAPE_CHYPER.translate((String)args[i]) + "\'";
          }
        }
        query = String.format(query, args);
      }
      
      try (Jedis conn = getConnection()) {
          return new ResultSetImpl(sendCompactCommand(conn, Command.QUERY, graphId, query).getObjectMultiBulkReply());
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


    /**
     * Sends command
     * @param conn
     * @param provider
     * @param args
     * @return
     */
    private BinaryClient sendCommand(Jedis conn, ProtocolCommand provider, String ...args) {
        BinaryClient binaryClient = conn.getClient();
        binaryClient.sendCommand(provider, args);
        return binaryClient;
    }


    /**
     * Sends the command with --COMPACT flag
     * @param conn
     * @param provider
     * @param args
     * @return
     */
    private BinaryClient sendCompactCommand(Jedis conn, ProtocolCommand provider, String ...args) {
        BinaryClient binaryClient = conn.getClient();
        List<String> largs = new ArrayList<>(Arrays.asList(args));
        largs.add("--COMPACT");
        String[] t = new String[largs.size()];
        binaryClient.sendCommand(provider, largs.toArray(t));
        return binaryClient;
    }
    
    private Jedis getConnection() {
        return this.client.getResource();
    }


    /**
     * Invokes stored procedures without arguments
     * @param procedure procedure name to invoke
     * @return
     */
    public ResultSet callProcedure(String procedure  ){
        return callProcedure(procedure, new ArrayList<>(), new HashMap<>());


    }


    /**
     * Invokes stored procedure with arguments
     * @param procedure
     * @param args
     * @return
     */
    public ResultSet callProcedure(String procedure, List<String> args  ){
        return callProcedure(procedure, args, new HashMap<>());


    }


    /**
     *
     * @param index - index of label
     * @return requested label
     */
    public  String getLabel(int index){
        if (index >= labels.size()){
            labels = getLabels();
        }
        return labels.get(index);
    }

    /**
     *
     * @return list of all the node labels in the graph
     */
    private  List<String> getLabels() {
        ResultSet resultSet = callProcedure("db.labels");
        ArrayList<String> labels = new ArrayList<>();
        while (resultSet.hasNext()){
            Record record = resultSet.next();
            labels.add(record.getString(0));

        }
        return labels;
    }


    /**
     *
     * @param index index of the relationship type
     * @return requested relationship type
     */
    public String getRelationshipType(int index){
        if (index >= relationshipTypes.size()){
            relationshipTypes = getRelationshipTypes();
        }
        return relationshipTypes.get(index);
    }


    /**
     *
     * @return a list of the edge relationship types in the graph
     */
    private List<String> getRelationshipTypes() {
        ResultSet resultSet = callProcedure("db.relationshipTypes");
        ArrayList<String> relationshipTypes = new ArrayList<>();
        while (resultSet.hasNext()){
            Record record = resultSet.next();
            relationshipTypes.add(record.getString(0));

        }
        return relationshipTypes;
    }


    /**
     *
     * @param index index of property name
     * @return requested property
     */
    public String getPropertyName(int index){
        if (index >= propertyNames.size()){
            propertyNames = getPropertyNames();
        }
        return propertyNames.get(index);
    }


    /**
     *
     * @return a list of all the property names in the graph
     */
    private List<String> getPropertyNames() {
        ResultSet resultSet = callProcedure("db.propertyKeys");
        ArrayList<String> propertyNames = new ArrayList<>();
        while (resultSet.hasNext()){
            Record record = resultSet.next();
            propertyNames.add(record.getString(0));

        }
        return propertyNames;
    }

    /**
     * Invoke a stored procedure
     * @param procedure
     * @param args
     * @param kwargs
     * @return
     */
    public ResultSet callProcedure(String procedure, List<String> args  , Map<String, List<String>> kwargs ){

        args = args.stream().map( s -> Utils.quoteString(s)).collect(Collectors.toList());
        StringBuilder q =  new StringBuilder();
        q.append(String.format("CALL %s(%s)", procedure, String.join(",", args)));
        List<String> y = kwargs.getOrDefault("y", null);
        if(y != null){
            q.append(String.join(",", y));
        }
        return query(q.toString());


    }

    /**
     * static function to access an initialized graph api
     * @return an initialized instance of RedisGraphAPI
     */
    public static RedisGraphAPI getInstance(){
        return redisGraphAPI;
    }
}
