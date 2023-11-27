package com.falkordb.impl.api;

import com.falkordb.Graph;
import com.falkordb.ResultSet;
import com.falkordb.impl.Utils;
import com.falkordb.impl.graph_cache.GraphCaches;
import com.falkordb.impl.resultset.ResultSetImpl;
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
public class GraphTransaction extends Transaction
        implements com.falkordb.GraphTransaction, GraphCacheHolder {

    private final Graph graph;
    private GraphCaches caches;

    public GraphTransaction(Client client, Graph graph) {
        // init as in Jedis
        super(client);

        this.graph = graph;
    }

    /**
     * Execute a Cypher query.
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> query(String graphId, String query) {
        client.sendCommand(GraphCommand.QUERY, graphId, query, Utils.COMPACT_STRING);
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Execute a Cypher read-oly query.
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> readOnlyQuery(String graphId, String query) {
        client.sendCommand(GraphCommand.RO_QUERY, graphId, query, Utils.COMPACT_STRING);
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Execute a Cypher query with timeout.
     *
     * NOTE: timeout is simply sent to DB. Socket timeout will not be changed.
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param timeout
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> query(String graphId, String query, long timeout) {
        client.sendCommand(GraphCommand.QUERY, graphId, query, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING,
                Long.toString(timeout));
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Execute a Cypher read-only query with timeout.
     *
     * NOTE: timeout is simply sent to DB. Socket timeout will not be changed.
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param timeout
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> readOnlyQuery(String graphId, String query, long timeout) {
        client.sendCommand(GraphCommand.RO_QUERY, graphId, query, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING,
                Long.toString(timeout));
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Execute a Cypher query with arguments
     *
     * @param graphId a graph to perform the query on
     * @param query Cypher query
     * @param args
     * @return response with a result set
     * @deprecated use {@link #query(String, String, Map)} instead.
     */
    @Deprecated
    @Override
    public Response<ResultSet> query(String graphId, String query, Object... args) {
        String preparedQuery = Utils.prepareQuery(query, args);
        client.sendCommand(GraphCommand.QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Executes a cypher query with parameters.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> query(String graphId, String query, Map<String, Object> params) {
        String preparedQuery = Utils.prepareQuery(query, params);
        client.sendCommand(GraphCommand.QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Executes a cypher read-only query with parameters.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> readOnlyQuery(String graphId, String query, Map<String, Object> params) {
        String preparedQuery = Utils.prepareQuery(query, params);
        client.sendCommand(GraphCommand.RO_QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Executes a cypher query with parameters and timeout.
     *
     * NOTE: timeout is simply sent to DB. Socket timeout will not be changed.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @param timeout
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> query(String graphId, String query, Map<String, Object> params, long timeout) {
        String preparedQuery = Utils.prepareQuery(query, params);
        client.sendCommand(GraphCommand.QUERY, graphId, preparedQuery, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING,
                Long.toString(timeout));
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Executes a cypher read-only query with parameters and timeout.
     *
     * NOTE: timeout is simply sent to DB. Socket timeout will not be changed.
     * @param graphId a graph to perform the query on.
     * @param query Cypher query.
     * @param params parameters map.
     * @param timeout
     * @return a response which builds the result set with the query answer.
     */
    @Override
    public Response<ResultSet> readOnlyQuery(String graphId, String query, Map<String, Object> params, long timeout) {
        String preparedQuery = Utils.prepareQuery(query, params);
        client.sendCommand(GraphCommand.RO_QUERY, graphId, preparedQuery, Utils.COMPACT_STRING,
                Utils.TIMEOUT_STRING, Long.toString(timeout));
        return getResponse(new Builder<ResultSet>() {
            @SuppressWarnings("unchecked")
            @Override
            public ResultSet build(Object o) {
                return new ResultSetImpl((List<Object>) o, graph, caches.getGraphCache(graphId));
            }
        });
    }

    /**
     * Invokes stored procedures without arguments, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> callProcedure(String graphId, String procedure) {
        return callProcedure(graphId, procedure, Utils.DUMMY_LIST, Utils.DUMMY_MAP);
    }

    /**
     * Invokes stored procedure with arguments, in multi/exec context
     * @param graphId a graph to perform the query on
     * @param procedure procedure name to invoke
     * @param args procedure arguments
     * @return response with result set with the procedure data
     */
    public Response<ResultSet> callProcedure(String graphId, String procedure, List<String> args) {
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
    public Response<String> deleteGraph(String graphId) {
        client.sendCommand(GraphCommand.DELETE, graphId);
        Response<String> response = getResponse(BuilderFactory.STRING);
        caches.removeGraphCache(graphId);
        return response;
    }

    @Override
    public void setGraphCaches(GraphCaches caches) {
        this.caches = caches;
    }

}
