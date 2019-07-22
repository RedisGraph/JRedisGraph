package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.RedisGraph;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.impl.Utils;
import com.redislabs.redisgraph.impl.graph_cache.RedisGraphCaches;
import com.redislabs.redisgraph.impl.resultset.ResultSetImpl;
import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Map;

/**
 * This class is extending Jedis Transaction
 */
public class RedisGraphTransaction extends Transaction implements com.redislabs.redisgraph.RedisGraphTransaction, RedisGraphCacheHolder {

    private final RedisGraph redisGraph;
    private RedisGraphCaches caches;


    public RedisGraphTransaction(Client client, RedisGraph redisGraph){
        // init as in Jedis
        super(client);

        this.redisGraph = redisGraph;
    }

    /**
     * Execute a Cypher query with arguments
     *
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return response with a result set
     */
    public Response<ResultSet> query(String graphId, String query, Object ...args){
        String preparedQuery = Utils.prepareQuery(query, args);
        client.sendCommand(RedisGraphCommand.QUERY, graphId, preparedQuery, "--COMPACT");
        return getResponse(new Builder<ResultSet>() {
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>)o, redisGraph, graphId, caches.getGraphCache(graphId));
            }
        });
    }


    /**
     * Invokes stored procedures without arguments, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> callProcedure(String graphId, String procedure){
        return callProcedure(graphId, procedure, Utils.DUMMY_LIST, Utils.DUMMY_MAP);
    }

    /**
     * Invokes stored procedure with arguments, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @param args procedure arguments
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> callProcedure(String graphId, String procedure, List<String> args  ){
        return callProcedure(graphId, procedure, args, Utils.DUMMY_MAP);
    }


    /**
     * Invoke a stored procedure, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure - procedure to execute
     * @param args - procedure arguments
     * @param kwargs - procedure output arguments
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> callProcedure(String graphId, String procedure, List<String> args,
                                                  Map<String, List<String>> kwargs) {
        String preparedProcedure = Utils.prepareProcedure(procedure, args, kwargs);
        return query(graphId, preparedProcedure);
    }


    /**
     * Deletes the entire graph, in multi/exec context
     * @param graphId graph to delete
     * @return response with the deletion running time statistics
     */
    public Response<String> deleteGraph(String graphId){

        client.sendCommand(RedisGraphCommand.DELETE, graphId);
        Response<String> response =  getResponse(BuilderFactory.STRING);
        caches.removeGraphCache(graphId);
        return response;
    }

    @Override
    public void setRedisGraphCaches(RedisGraphCaches caches) {
        this.caches = caches;
    }

}
