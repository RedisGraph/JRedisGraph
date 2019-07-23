package com.redislabs.redisgraph.impl.graph_cache;

import com.redislabs.redisgraph.RedisGraph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisGraphCaches {

    private final Map<String, GraphCache> graphCaches = new ConcurrentHashMap<>();

    public GraphCache getGraphCache(String graphId){
        if (!graphCaches.containsKey(graphId)){
            graphCaches.putIfAbsent(graphId, new GraphCache(graphId));
        }
        return graphCaches.get(graphId);
    }

    /**
     * Removes a graph meta data cache
     * @param graphId
     */
    public void removeGraphCache(String graphId){
        graphCaches.remove(graphId);
    }
}
