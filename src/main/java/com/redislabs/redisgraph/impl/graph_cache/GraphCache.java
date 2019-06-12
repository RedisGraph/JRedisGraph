package com.redislabs.redisgraph.impl.graph_cache;

import com.redislabs.redisgraph.RedisGraph;

/**
 * A class to store a local cache in the client, for a specific graph.
 * Holds the labels, property names and relationship types
 */
public class GraphCache {

    private final GraphCacheList labels;
    private final GraphCacheList propertyNames;
    private final GraphCacheList relationshipTypes;

    /**
     *
     * @param graphId - graph Id
     * @param redisGraph - a client to use in the cache, for re-validate it by calling procedures
     */
    public GraphCache(String graphId, RedisGraph redisGraph) {
        this.labels = new GraphCacheList(graphId, "db.labels", redisGraph);
        this.propertyNames = new GraphCacheList(graphId, "db.propertyKeys", redisGraph);
        this.relationshipTypes = new GraphCacheList(graphId, "db.relationshipTypes", redisGraph);
    }

    /**
     * @param index - index of label
     * @return requested label
     */
    public String getLabel(int index) {
        return labels.getCachedData(index);
    }

    /**
     * @param index index of the relationship type
     * @return requested relationship type
     */
    public String getRelationshipType(int index) {
        return relationshipTypes.getCachedData(index);
    }

    /**
     * @param index index of property name
     * @return requested property
     */
    public String getPropertyName(int index) {

        return propertyNames.getCachedData(index);
    }
}
