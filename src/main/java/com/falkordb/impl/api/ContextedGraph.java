package com.falkordb.impl.api;

import java.util.List;

import com.falkordb.GraphContext;
import com.falkordb.ResultSet;
import com.falkordb.exceptions.GraphException;
import com.falkordb.impl.Utils;
import com.falkordb.impl.graph_cache.GraphCaches;
import com.falkordb.impl.resultset.ResultSetImpl;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

/**
 * An implementation of GraphContext. Allows sending Graph and some Redis commands,
 * within a specific connection context
 */
public class ContextedGraph extends AbstractGraph implements GraphContext, GraphCacheHolder {

    private final Jedis connectionContext;
    private GraphCaches caches;

    /**
     * Generates a new instance with a specific Jedis connection
     * @param connectionContext
     */
    public ContextedGraph(Jedis connectionContext) {
        this.connectionContext = connectionContext;
    }

    /**
     * Overrides the abstract method. Return the instance only connection
     * @return
     */
    @Override
    protected Jedis getConnection() {
        return this.connectionContext;
    }

    /**
     * Sends the query over the instance only connection
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendQuery(String graphId, String preparedQuery) {
        Jedis conn = getConnection();
        try {
            @SuppressWarnings("unchecked")
            List<Object> rawResponse = (List<Object>) conn.sendCommand(GraphCommand.QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        } catch (GraphException rt) {
            throw rt;
        } catch (JedisDataException j) {
            throw new GraphException(j);
        }
    }

    /**
     * Sends the read-only query over the instance only connection
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendReadOnlyQuery(String graphId, String preparedQuery) {
        Jedis conn = getConnection();
        try {
            @SuppressWarnings("unchecked")
            List<Object> rawResponse = (List<Object>) conn.sendCommand(GraphCommand.RO_QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        } catch (GraphException ge) {
            throw ge;
        } catch (JedisDataException de) {
            throw new GraphException(de);
        }
    }

    /**
     * Sends the query over the instance only connection
     * @param graphId graph to be queried
     * @param timeout
     * @param preparedQuery prepared query
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendQuery(String graphId, String preparedQuery, long timeout) {
        Jedis conn = getConnection();
        try {
            @SuppressWarnings("unchecked")
            List<Object> rawResponse = (List<Object>) conn.sendBlockingCommand(GraphCommand.QUERY,
                    graphId, preparedQuery, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING, Long.toString(timeout));
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        } catch (GraphException rt) {
            throw rt;
        } catch (JedisDataException j) {
            throw new GraphException(j);
        }
    }

    /**
     * Sends the read-only query over the instance only connection
     * @param graphId graph to be queried
     * @param timeout
     * @param preparedQuery prepared query
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendReadOnlyQuery(String graphId, String preparedQuery, long timeout) {
        Jedis conn = getConnection();
        try {
            @SuppressWarnings("unchecked")
            List<Object> rawResponse = (List<Object>) conn.sendBlockingCommand(GraphCommand.RO_QUERY,
                    graphId, preparedQuery, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING, Long.toString(timeout));
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        } catch (GraphException ge) {
            throw ge;
        } catch (JedisDataException de) {
            throw new GraphException(de);
        }
    }

    /**
     * @return Returns the instance Jedis connection.
     */
    @Override
    public Jedis getConnectionContext() {
        return this.connectionContext;
    }

    /**
     * Creates a new GraphTransaction transactional object
     * @return new GraphTransaction
     */
    @Override
    public GraphTransaction multi() {
        Jedis jedis = getConnection();
        Client client = jedis.getClient();
        client.multi();
        client.getOne();
        GraphTransaction transaction = new GraphTransaction(client, this);
        transaction.setGraphCaches(caches);
        return transaction;
    }

    /**
     * Creates a new GraphPipeline pipeline object
     * @return new GraphPipeline
     */
    @Override
    public GraphPipeline pipelined() {
        Jedis jedis = getConnection();
        Client client = jedis.getClient();
        GraphPipeline pipeline = new GraphPipeline(client, this);
        pipeline.setGraphCaches(caches);
        return pipeline;
    }

    /**
     * Perfrom watch over given Redis keys
     * @param keys
     * @return "OK"
     */
    @Override
    public String watch(String... keys) {
        return this.getConnection().watch(keys);
    }

    /**
     * Removes watch from all keys
     * @return
     */
    @Override
    public String unwatch() {
        return this.getConnection().unwatch();
    }

    /**
     * Deletes the entire graph
     * @param graphId graph to delete
     * @return delete running time statistics
     */
    @Override
    public String deleteGraph(String graphId) {
        Jedis conn = getConnection();
        Object response;
        try {
            response = conn.sendCommand(GraphCommand.DELETE, graphId);
        } catch (Exception e) {
            conn.close();
            throw e;
        }
        //clear local state
        caches.removeGraphCache(graphId);
        return SafeEncoder.encode((byte[]) response);
    }

    /**
     * closes the Jedis connection
     */
    @Override
    public void close() {
        this.connectionContext.close();

    }

    @Override
    public void setGraphCaches(GraphCaches caches) {
        this.caches = caches;
    }

}
