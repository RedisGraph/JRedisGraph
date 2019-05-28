package com.redislabs.redisgraph.impl;

import com.redislabs.redisgraph.Record;
import com.redislabs.redisgraph.RedisGraph;
import com.redislabs.redisgraph.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a local cache of list of strings. Holds data from a specific procedure, for a specific graph.
 */
public class GraphCacheList {

    private Object mutex = new Object();
    private final String graphId;
    private final String procedure;
    private final RedisGraph redisGraph;
    private final List<String>  data = new CopyOnWriteArrayList<>();



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
        if (index >= data.size()) {
            synchronized (mutex){
                if (index >= data.size()) {
                    getProcedureInfo();
                }
            }
        }
        String s = data.get(index);
        return s;

    }

    /**
     * Auxiliary method to parse a procedure result set and refresh the cache
     */
    private void getProcedureInfo() {
        ResultSet resultSet = redisGraph.callProcedure(graphId, procedure);
        List<String> newData = new ArrayList<>();
        int i = 0;
        while (resultSet.hasNext()) {
            Record record = resultSet.next();
            if(i >= data.size()){
                newData.add(record.getString(0));
            }
            i++;
        }
        data.addAll(newData);
    }
}
