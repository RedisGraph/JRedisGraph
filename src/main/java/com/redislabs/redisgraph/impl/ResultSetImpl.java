package com.redislabs.redisgraph.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.redislabs.redisgraph.Record;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.Statistics;

import redis.clients.jedis.util.SafeEncoder;

public class ResultSetImpl implements ResultSet{
	
    final private int totalResults;
    final private List<String> header;
    final private List<Record> results;
    final private Statistics statistics;
    private int position = 0;

    public ResultSetImpl(List<Object> resp) {

    	this.statistics = new StatisticsImpl((List<byte[]>)resp.get(1));
    	
    	ArrayList<ArrayList<byte[]>> result = (ArrayList<ArrayList<byte[]>>) resp.get(0);
    	
        // Empty result set
        if(result == null || result.size() == 0) {
        	header = new ArrayList<>(0);
            totalResults = 0;
            results = new ArrayList<Record>(0);
        } else {
        	ArrayList<byte[]> headers = result.get(0);
        	header = headers.stream().map( h -> new String(h)).collect(Collectors.toList());

        	// First row is a header row
	        totalResults = result.size()-1;
	        results = new ArrayList<Record>(totalResults);
	        // Skips last row (runtime info)
            for (int i = 1; i <= totalResults; i++) {
            	ArrayList<byte[]> row = result.get(i);
            	Record record = new RecordImpl(header, row.stream().map( h -> SafeEncoder.encode(h)).collect(Collectors.toList()));
            	results.add(record);
            }
        }
    }
    
    public List<String> getHeader(){
    	return header;
    }
    
	@Override
	public boolean hasNext() {
		return position < results.size();
	}

	@Override
	public Record next() {
		return results.get(position++);
	}

	@Override
	public Statistics getStatistics() {
		return statistics;
	}
}
