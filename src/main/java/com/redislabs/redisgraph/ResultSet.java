package com.redislabs.redisgraph;

import java.util.ArrayList;
import java.util.List;

public class ResultSet {
    public int totalResults;
    public String[] header;
    public List<String[]> results;

    public ResultSet(List<Object> resp) {

        // Empty result set
        if(resp.size() == 0) {
            totalResults = 0;
            results = new ArrayList<String[]>(0);
        }

        // First row is a header row
        String header_row = new String((byte[]) resp.get(0));
        header = header_row.split(",");
        totalResults = resp.size()-2;
        results = new ArrayList<String[]>(totalResults);

        // Skips last row (runtime info)
        for (int i = 1; i < resp.size()-1; i++) {
            String row = new String((byte[]) resp.get(i));
            results.add(row.split(","));
        }
    }
}
