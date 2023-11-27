package com.falkordb.impl.graph_cache;

import com.falkordb.Graph;

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
     */
    public GraphCache(String graphId) {
        this.labels = new GraphCacheList(graphId, "db.labels");
        this.propertyNames = new GraphCacheList(graphId, "db.propertyKeys");
        this.relationshipTypes = new GraphCacheList(graphId, "db.relationshipTypes");
    }

    /**
     * @param index - index of label
     * @return requested label
     */
    public String getLabel(int index, Graph redisGraph) {
        return labels.getCachedData(index, redisGraph);
    }

    /**
     * @param index index of the relationship type
     * @return requested relationship type
     */
    public String getRelationshipType(int index, Graph redisGraph) {
        return relationshipTypes.getCachedData(index, redisGraph);
    }

    /**
     * @param index index of property name
     * @return requested property
     */
    public String getPropertyName(int index, Graph redisGraph) {

        return propertyNames.getCachedData(index, redisGraph);
    }
}
