package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.RedisGraphContext;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.exceptions.JRedisGraphException;
import com.redislabs.redisgraph.impl.Utils;
import com.redislabs.redisgraph.impl.graph_cache.RedisGraphCaches;
import com.redislabs.redisgraph.impl.resultset.ResultSetImpl;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;
import redis.clients.jedis.exceptions.JedisDataException;
import java.util.List;
import java.util.Map;

/**
 * An implementation of RedisGraphContext. Allows sending RedisGraph and some Redis commands,
 * within a specific connection context
 */
public class ContextedRedisGraph extends AbstractRedisGraph implements RedisGraphContext, RedisGraphCacheHolder {

    private final Jedis connectionContext;
    private RedisGraphCaches caches;

    /**
     * Generates a new instance with a specific Jedis connection
     * @param connectionContext
     */
    public ContextedRedisGraph(Jedis connectionContext){
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
            List<Object> rawResponse = (List<Object>) conn.sendCommand(RedisGraphCommand.QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        }
        catch (JRedisGraphException rt) {
            throw rt;
        }
        catch (JedisDataException j) {
            throw new JRedisGraphException(j);
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
            List<Object> rawResponse = (List<Object>) conn.sendCommand(RedisGraphCommand.RO_QUERY, graphId, preparedQuery, Utils.COMPACT_STRING);
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        }
        catch (JRedisGraphException ge) {
            throw ge;
        }
        catch (JedisDataException de) {
            throw new JRedisGraphException(de);
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
            List<Object> rawResponse = (List<Object>) conn.sendBlockingCommand(RedisGraphCommand.QUERY,
                graphId, preparedQuery, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING, Long.toString(timeout));
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        }
        catch (JRedisGraphException rt) {
            throw rt;
        }
        catch (JedisDataException j) {
            throw new JRedisGraphException(j);
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
            List<Object> rawResponse = (List<Object>) conn.sendBlockingCommand(RedisGraphCommand.RO_QUERY,
                    graphId, preparedQuery, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING, Long.toString(timeout));
            return new ResultSetImpl(rawResponse, this, caches.getGraphCache(graphId));
        }
        catch (JRedisGraphException ge) {
            throw ge;
        }
        catch (JedisDataException de) {
            throw new JRedisGraphException(de);
        }
    }

    /**
     * Executes a cypher query with parameters and redisgraph timeout.
     * After that block the current client until all the previous cypher write queries
     * are successfully transferred and acknowledged by at least 1 replica.
     * If the replicationTimeout, specified in milliseconds, is reached,
     * the method returns even if the specified number of replicas were not yet reached.
     *
     * @param graphId            graph to be queried
     * @param preparedQuery      prepared query
     * @param redisGraphTimeout
     * @param replicationTimeout replication timeout, specified in milliseconds
     * @return a result set
     */
    @Override
    protected ResultSet sendReplicatedQuery(String graphId, String preparedQuery, long redisGraphTimeout, long replicationTimeout) {
        Jedis conn = getConnection();
        try {
            Pipeline pipe = conn.pipelined();
            pipe.setClient(conn.getClient());
            Response<Object> rawResponse = pipe.sendCommand(RedisGraphCommand.QUERY,
                    graphId, preparedQuery, Utils.COMPACT_STRING, Utils.TIMEOUT_STRING, Long.toString(redisGraphTimeout));
            Response<Object> waitResponse = pipe.sendCommand(RedisGraphCommand.WAIT,"1", Long.toString(replicationTimeout));
            pipe.sync();
            ResultSetImpl resultSet = new ResultSetImpl((List<Object>) rawResponse.get(), this, caches.getGraphCache(graphId));
            Long numberReplicasReached = (Long) waitResponse.get();
            resultSet.setNumberReplicasReached(numberReplicasReached);
            return resultSet;
        }
        catch (JRedisGraphException ge) {
            throw ge;
        }
        catch (JedisDataException de) {
            throw new JRedisGraphException(de);
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
     * Creates a new RedisGraphTransaction transactional object
     * @return new RedisGraphTransaction
     */
    @Override
    public RedisGraphTransaction multi() {
        Jedis jedis = getConnection();
        Client client = jedis.getClient();
        client.multi();
        client.getOne();
        RedisGraphTransaction transaction =  new RedisGraphTransaction(client,this);
        transaction.setRedisGraphCaches(caches);
        return transaction;
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
            response = conn.sendCommand(RedisGraphCommand.DELETE, graphId);
        }
        catch (Exception e) {
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
    public void setRedisGraphCaches(RedisGraphCaches caches) {
        this.caches = caches;
    }

}
