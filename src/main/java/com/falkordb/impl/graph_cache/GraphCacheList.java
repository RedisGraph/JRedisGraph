package com.falkordb.impl.graph_cache;

import com.falkordb.Record;
import com.falkordb.Graph;
import com.falkordb.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Represents a local cache of list of strings. Holds data from a specific procedure, for a specific graph.
 */
class GraphCacheList {

    private final String graphId;
    private final String procedure;
    private final List<String>  data = new CopyOnWriteArrayList<>();

    /**
     *
     * @param graphId - graph id
     * @param procedure - exact procedure command
     */
    public GraphCacheList(String graphId, String procedure) {
        this.graphId = graphId;
        this.procedure = procedure;
    }


    /**
     * A method to return a cached item if it is in the cache, or re-validate the cache if its invalidated
     * @param index index of data item
     * @return The string value of the specific procedure response, at the given index.
     */
    public String getCachedData(int index, Graph graph) {
        if (index >= data.size()) {
            synchronized (data){
                if (index >= data.size()) {
                    getProcedureInfo(graph);
                }
            }
        }
        return data.get(index);

    }

    /**
     * Auxiliary method to parse a procedure result set and refresh the cache
     */
    private void getProcedureInfo(Graph graph) {
        ResultSet resultSet = graph.callProcedure(graphId, procedure);
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
