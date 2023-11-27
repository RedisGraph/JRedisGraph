package com.falkordb.impl.graph_cache;

import com.falkordb.Graph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GraphCaches {

    private final Map<String, GraphCache> graphCaches = new ConcurrentHashMap<>();

    public GraphCache getGraphCache(String graphId){
        if (!graphCaches.containsKey(graphId)){
            graphCaches.putIfAbsent(graphId, new GraphCache(graphId));
        }
        return graphCaches.get(graphId);
    }

    /**
     * Returns a String which represents the name of the label mapped to the label id
     * @param graphId graph to perform the query
     * @param index label index
     * @param graph GraphAPI implementation
     * @return label name
     */
    public String getLabel(String graphId, int index, Graph graph) {
        return getGraphCache(graphId).getLabel(index, graph);
    }

    /**
     * Returns a String which represents the name of the relationship mapped to the label id
     * @param graphId graph to perform the query
     * @param index relationship index
     * @param graph GraphAPI implementation
     * @return relationship name
     */
    public String getRelationshipType(String graphId, int index, Graph graph){
        return getGraphCache(graphId).getRelationshipType(index, graph);
    }

    /**
     * Returns a String which represents the name of the property mapped to the label id
     * @param graphId graph to perform the query
     * @param index property index
     * @param graph GraphAPI implementation
     * @return property name
     */
    public String getPropertyName(String graphId, int index, Graph graph){
        return getGraphCache(graphId).getPropertyName(index, graph);
    }

    /**
     * Removes a graph meta data cache
     * @param graphId
     */
    public void removeGraphCache(String graphId){
        graphCaches.remove(graphId);
    }
}
