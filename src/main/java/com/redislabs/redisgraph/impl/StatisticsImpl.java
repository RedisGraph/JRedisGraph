package com.redislabs.redisgraph.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.redislabs.redisgraph.Statistics;

import redis.clients.jedis.util.SafeEncoder;

public class StatisticsImpl implements Statistics  {
	final private List<byte[]> raw;
	final private Map<Statistics.Label, String> statistics;
	
	StatisticsImpl(List<byte[]> raw){
		this.raw = raw;
		this.statistics = new HashMap<Statistics.Label, String>(raw.size()); // lazy loaded
	}
	
	@Override
	public String getStringValue(Statistics.Label label) {
		return getStatistics().get(label);
	}
	
	private Map<Statistics.Label, String> getStatistics(){
		if(statistics.size() == 0) {		
			for(byte[]  touple : this.raw) {
				String row = SafeEncoder.encode(touple);
				String[] rowTouple = row.split(":");
				this.statistics.put( Statistics.Label.getEnum(rowTouple[0]), rowTouple[1].trim());
			}
		}
		return statistics;
	}
}
