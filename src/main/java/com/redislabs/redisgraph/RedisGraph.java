package com.redislabs.redisgraph;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public interface RedisGraph extends Closeable {

    /**
     * Execute a Cypher query.
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @return a result set
     */
    ResultSet query(String graphId, String query);

    /**
     * Execute a Cypher query with timeout.
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param timeout
     * @return a result set
     */
    ResultSet query(String graphId, String query, long timeout);

    /**
     * Execute a Cypher query with arguments
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return a result set
     * @deprecated use {@link #query(String, String, Map)} instead.
     */
    @Deprecated
    ResultSet query(String graphId, String query, Object ...args);

    /**
     * Executes a cypher query with parameters.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @return a result set.
     */
    ResultSet query(String graphId, String query, Map<String, Object> params);

    /**
     * Executes a cypher query with parameters and timeout.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @param timeout
     * @return a result set.
     */
    ResultSet query(String graphId, String query, Map<String, Object> params, long timeout);

    /**
     * Invokes stored procedures without arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return result set with the procedure data
     */
    ResultSet callProcedure(String graphId, String procedure);

    /**
     * Invokes stored procedure with arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @param args procedure arguments
     * @return result set with the procedure data
     */
    ResultSet callProcedure(String graphId, String procedure, List<String> args);

    /**
     * Invoke a stored procedure
     * @param graphId a graph to perform the query on
     * @param procedure - procedure to execute
     * @param args - procedure arguments
     * @param kwargs - procedure output arguments
     * @return result set with the procedure data
     */
    ResultSet callProcedure(String graphId, String procedure, List<String> args  , Map<String, List<String>> kwargs);

    /**
     * Deletes the entire graph
     * @param graphId graph to delete
     * @return delete running time statistics
     */
    String deleteGraph(String graphId);

    @Override
    void close();
}
