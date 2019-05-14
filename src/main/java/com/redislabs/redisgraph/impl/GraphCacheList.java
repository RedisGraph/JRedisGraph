package com.redislabs.redisgraph.impl;

import com.redislabs.redisgraph.Record;
import com.redislabs.redisgraph.RedisGraph;
import com.redislabs.redisgraph.ResultSet;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a local cache of list of strings. Holds data from a specific procedure, for a specific graph.
 */
public class GraphCacheList extends ArrayList<String> {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final String graphId;
    private final String procedure;
    private final RedisGraph redisGraph;

    /**
     *
     * @param graphId - graph id
     * @param procedure - exact procedure command
     * @param redisGraph - a client to use in the cache, for re-validate it by calling procedures
     */
    public GraphCacheList(String graphId, String procedure, RedisGraph redisGraph) {
        this.graphId = graphId;
        this.procedure = procedure;
        this.redisGraph = redisGraph;
    }


    /**
     * A method to return a cached item if it is in the cache, or re-validate the cache if its invalidated
     * @param index index of data item
     * @return The string value of the specific procedure response, at the given index.
     */
    public String getCachedData(int index) {
        if (index >= this.size()) {
            writeLock.lock();
            //check again
            if (index >= this.size()) {
                getProcedureInfo();
            }
            writeLock.unlock();
        }
        readLock.lock();
        String s = this.get(index);
        readLock.unlock();
        return s;

    }

    /**
     * Auxiliary method to parse a procedure result set and refresh the cache
     */
    private void getProcedureInfo() {
        ResultSet resultSet = redisGraph.callProcedure(graphId, procedure);
        this.clear();
        while (resultSet.hasNext()) {
            Record record = resultSet.next();
            this.add(record.getString(0));
        }
    }
}
