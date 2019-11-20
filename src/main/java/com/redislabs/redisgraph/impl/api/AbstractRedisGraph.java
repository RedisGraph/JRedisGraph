package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.RedisGraph;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.impl.Utils;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * An abstract class to handle non implementation specific user requests
 */
public abstract class AbstractRedisGraph implements RedisGraph {



    /**
     * Inherited classes should return a Jedis connection, with respect to their context
     * @return Jedis connection
     */
    protected abstract Jedis getConnection();

    /**
     * Sends a query to the redis graph. Implementation and context dependent
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @return Result set
     */
    protected abstract ResultSet sendQuery(String graphId, String preparedQuery);

    /**
     * Execute a Cypher query with arguments
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @return a result set
     */
    public ResultSet query(String graphId, String query) {
        return sendQuery(graphId, query);
    }

    /**
     * Execute a Cypher query with arguments
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return a result set
     */
    @Deprecated
    public ResultSet query(String graphId, String query, Object ...args) {
        String preparedQuery = Utils.prepareQuery(query, args);
        return sendQuery(graphId, preparedQuery);
    }

    /**
     * Executes a parameterized cypher query.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @return a result set.
     */
    public ResultSet query(String graphId, String query, Map<String, Object> params) {
        String preparedQuery = Utils.prepareQuery(query, params);
        return sendQuery(graphId, preparedQuery);
    }


    public ResultSet callProcedure(String graphId, String procedure){
        return callProcedure(graphId, procedure, Utils.DUMMY_LIST, Utils.DUMMY_MAP);
    }

    public ResultSet callProcedure(String graphId, String procedure, List<String> args){
        return callProcedure(graphId, procedure, args, Utils.DUMMY_MAP);
    }

    public ResultSet callProcedure(String graphId, String procedure, List<String> args  , Map<String, List<String>> kwargs){

        String preparedProcedure = Utils.prepareProcedure(procedure, args, kwargs);
        return query(graphId, preparedProcedure);
    }
}
