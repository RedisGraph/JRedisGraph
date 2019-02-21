package com.redislabs.redisgraph.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import com.redislabs.redisgraph.Record;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.Statistics;

import redis.clients.jedis.util.SafeEncoder;

public class ResultSetImpl implements ResultSet{
  
  private final int totalResults;
  private final List<String> header;
  private final List<Record> results;
  private final Statistics statistics;
  private int position = 0;

  public ResultSetImpl(List<Object> resp) {
    
    this.statistics = new StatisticsImpl((List<byte[]>)resp.get(1));
    
    ArrayList<ArrayList<?>> result = (ArrayList<ArrayList<?>>) resp.get(0);
    
    // Empty result set
    if(result == null || result.isEmpty()) {
      header = new ArrayList<>(0);
      totalResults = 0;
      results = new ArrayList<>(0);
    } else {
      ArrayList<byte[]> headers = (ArrayList<byte[]>)result.get(0);
      header = headers.stream().map( String::new).collect(Collectors.toList());

      // First row is a header row
      totalResults = result.size()-1;
      results = new ArrayList<>(totalResults);
      // Skips last row (runtime info)
      for (int i = 1; i <= totalResults; i++) {
        ArrayList<?> row = result.get(i);
        Record record = new RecordImpl(header, row.stream().map( obj -> {
          if(obj instanceof byte[]) {
            return SafeEncoder.encode((byte[])obj);  
          } 
          return obj;
        }).collect(Collectors.toList()));
        results.add(record);
      }
    }
  }

  @Override
  public List<String> getHeader(){
    return header;
  }

  @Override
  public boolean hasNext() {
    return position < results.size();
  }

  @Override
  public Record next() {
    if (!hasNext())
      throw new NoSuchElementException();
    return results.get(position++);
  }

  @Override
  public Statistics getStatistics() {
    return statistics;
  }

  @Override
  public String toString() {
    return this.header + "\n" + this.results + "\n" + this.statistics;
  }
}
