package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.RedisGraphContext;
import com.redislabs.redisgraph.RedisGraphContextGenerator;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.exceptions.JRedisGraphException;
import com.redislabs.redisgraph.impl.Utils;
import com.redislabs.redisgraph.impl.graph_cache.RedisGraphCaches;
import com.redislabs.redisgraph.impl.resultset.ResultSetImpl;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class RedisGraph extends AbstractRedisGraph implements RedisGraphContextGenerator {

    private final Pool<Jedis> client;
    private final RedisGraphCaches caches = new RedisGraphCaches();

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


    /**
     * Overrides the abstract function. Gets and returns a Jedis connection from the Jedis pool
     * @return a Jedis connection
     */
    @Override
    protected Jedis getConnection() {
        return client.getResource();
    }

    /**
     * Overrides the abstract function.
     * Sends the query from any Jedis connection received from the Jedis pool and closes it once done
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendQuery(String graphId, String preparedQuery){
        try (ContextedRedisGraph contextedRedisGraph = new ContextedRedisGraph(getConnection())) {
            contextedRedisGraph.setRedisGraphCaches(caches);
            return contextedRedisGraph.sendQuery(graphId, preparedQuery);
        }
    }

    /**
     * Overrides the abstract function.
     * Sends the read-only query from any Jedis connection received from the Jedis pool and closes it once done
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendReadOnlyQuery(String graphId, String preparedQuery){
        try (ContextedRedisGraph contextedRedisGraph = new ContextedRedisGraph(getConnection())) {
            contextedRedisGraph.setRedisGraphCaches(caches);
            return contextedRedisGraph.sendReadOnlyQuery(graphId, preparedQuery);
        }
    }

    /**
     * Overrides the abstract function.
     * Sends the query from any Jedis connection received from the Jedis pool and closes it once done
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @param timeout
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendQuery(String graphId, String preparedQuery, long timeout){
        try (ContextedRedisGraph contextedRedisGraph = new ContextedRedisGraph(getConnection())) {
            contextedRedisGraph.setRedisGraphCaches(caches);
            return contextedRedisGraph.sendQuery(graphId, preparedQuery, timeout);
        }
    }

    /**
     * Overrides the abstract function.
     * Sends the read-only query from any Jedis connection received from the Jedis pool and closes it once done
     * @param graphId graph to be queried
     * @param preparedQuery prepared query
     * @param timeout
     * @return Result set with the query answer
     */
    @Override
    protected ResultSet sendReadOnlyQuery(String graphId, String preparedQuery, long timeout){
        try (ContextedRedisGraph contextedRedisGraph = new ContextedRedisGraph(getConnection())) {
            contextedRedisGraph.setRedisGraphCaches(caches);
            return contextedRedisGraph.sendReadOnlyQuery(graphId, preparedQuery, timeout);
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
        try (ContextedRedisGraph contextedRedisGraph = new ContextedRedisGraph(getConnection())) {
            contextedRedisGraph.setRedisGraphCaches(caches);
            return contextedRedisGraph.sendReplicatedQuery(graphId, preparedQuery, redisGraphTimeout,replicationTimeout);
        }
    }

    /**
     * Closes the Jedis pool
     */
    @Override
    public void close(){
        this.client.close();
    }

    /**
     * Deletes the entire graph
     * @param graphId graph to delete
     * @return delete running time statistics
     */
    @Override
    public String deleteGraph(String graphId) {
        try (Jedis conn = getConnection()) {
            Object response = conn.sendCommand(RedisGraphCommand.DELETE, graphId);
            //clear local state
            caches.removeGraphCache(graphId);
            return SafeEncoder.encode((byte[]) response);
        }
    }

    /**
     * Returns a new ContextedRedisGraph bounded to a Jedis connection from the Jedis pool
     * @return ContextedRedisGraph
     */
    @Override
    public RedisGraphContext getContext() {
        ContextedRedisGraph contextedRedisGraph =  new ContextedRedisGraph(getConnection());
        contextedRedisGraph.setRedisGraphCaches(this.caches);
        return contextedRedisGraph;
    }
}
