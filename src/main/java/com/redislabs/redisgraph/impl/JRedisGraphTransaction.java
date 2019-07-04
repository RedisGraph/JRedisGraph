package com.redislabs.redisgraph.impl;


import com.redislabs.redisgraph.Command;
import com.redislabs.redisgraph.RedisGraph;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.Utils;
import com.redislabs.redisgraph.impl.graph_cache.GraphCache;
import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is extending Jedis Transaction
 */
public class JRedisGraphTransaction extends Transaction {

    private final RedisGraph redisGraph;
    private final Map<String, GraphCache> graphCaches;


    public JRedisGraphTransaction(Client client, RedisGraph redisGraph, Map<String, GraphCache> graphCaches){
        // init as in Jedis
        super(client);

        this.redisGraph = redisGraph;
        this.graphCaches = graphCaches;
    }

    /**
     * Execute a Cypher query with arguments
     *
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return response with a result set
     */
    public Response<ResultSet> graphQuery(String graphId, String query, Object ...args){
        String preparedQuery = Utils.prepareQuery(query, args);
        graphCaches.putIfAbsent(graphId, new GraphCache(graphId, redisGraph));
        client.sendCommand(Command.QUERY, graphId, preparedQuery, "--COMPACT");
        return getResponse(new Builder<ResultSet>() {
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>)o, graphCaches.get(graphId));
            }
        });
    }


    /**
     * Invokes stored procedures without arguments, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> graphCallProcedure(String graphId, String procedure){
        return graphCallProcedure(graphId, procedure, new ArrayList<>(), new HashMap<>());
    }

    /**
     * Invokes stored procedure with arguments, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @param args procedure arguments
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> graphCallProcedure(String graphId, String procedure, List<String> args  ){
        return graphCallProcedure(graphId, procedure, args, new HashMap<>());
    }


    /**
     * Invoke a stored procedure, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure - procedure to execute
     * @param args - procedure arguments
     * @param kwargs - procedure output arguments
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> graphCallProcedure(String graphId, String procedure, List<String> args,
                                                  HashMap<String, List<String>> kwargs) {
        String preparedProcedure = Utils.prepareProcedure(procedure, args, kwargs);
        return graphQuery(graphId, preparedProcedure);
    }


    /**
     * Deletes the entire graph, in multi/exec context
     * @param graphId graph to delete
     * @return response with the deletion running time statistics
     */
    public Response<String> graphDeleteGraph(String graphId){
        graphCaches.remove(graphId);
        client.sendCommand(Command.DELETE, graphId);
        return getResponse(BuilderFactory.STRING);
    }
}
