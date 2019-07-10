package com.redislabs.redisgraph;

import redis.clients.jedis.Response;
import redis.clients.jedis.commands.BasicRedisPipeline;
import redis.clients.jedis.commands.BinaryRedisPipeline;
import redis.clients.jedis.commands.BinaryScriptingCommandsPipeline;
import redis.clients.jedis.commands.ClusterPipeline;
import redis.clients.jedis.commands.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.commands.MultiKeyCommandsPipeline;
import redis.clients.jedis.commands.RedisPipeline;
import redis.clients.jedis.commands.ScriptingCommandsPipeline;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * An interface which aligned to Jedis transactional interface
 */
public interface RedisGraphTransaction extends
        MultiKeyBinaryRedisPipeline,
        MultiKeyCommandsPipeline, ClusterPipeline,
        BinaryScriptingCommandsPipeline, ScriptingCommandsPipeline,
        BasicRedisPipeline, BinaryRedisPipeline, RedisPipeline, Closeable {
    /**
     * Execute a Cypher query with arguments
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return a response which builds the result set with the query answer
     */
    Response<ResultSet> query(String graphId, String query, Object ...args);

    /**
     * Invokes stored procedures without arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return a response which builds result set with the procedure data
     */
    Response<ResultSet> callProcedure(String graphId, String procedure);

    /**
     * Invokes stored procedure with arguments
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @param args procedure arguments
     * @return a response which builds result set with the procedure data
     */
    Response<ResultSet> callProcedure(String graphId, String procedure, List<String> args);

    /**
     * Invoke a stored procedure
     * @param graphId a graph to perform the query on
     * @param procedure - procedure to execute
     * @param args - procedure arguments
     * @param kwargs - procedure output arguments
     * @return a response which builds result set with the procedure data
     */
    Response<ResultSet> callProcedure(String graphId, String procedure, List<String> args  , Map<String, List<String>> kwargs);

    /**
     * Deletes the entire graph
     * @param graphId graph to delete
     * @return a response which builds the delete running time statistics
     */
    Response<String> deleteGraph(String graphId);


    /**
     * executes the transaction
     * @return a list of the executed transaction commands answers, in case of successful transaction, null otherwise
     */
    List<Object> exec();

    /**
     * If object is in transaction mode,
     * flushes all previously queued commands in a transaction and restores the connection state to normal
     */
    void clear();

    /**
     *
     * @return
     */
    List<Response<?>> execGetResponse();

    /**
     * Flushes all previously queued commands in a transaction and restores the connection state to normal
     */
    String discard();
}
