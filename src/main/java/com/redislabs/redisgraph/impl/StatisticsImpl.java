package com.redislabs.redisgraph.impl;

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
				String[] rowTouple = SafeEncoder.encode(touple).split(":");
				this.statistics.put( Statistics.Label.getEnum(rowTouple[0]), rowTouple[1].trim());
			}
		}
		return statistics;
	}
	
	public int getIntValue(Statistics.Label label) {
		String value = getStringValue(label);
		return value==null ? 0 : Integer.parseInt(value);
	}

	@Override
	public int nodesCreated() {
		return getIntValue(Label.NODES_CREATED);
	}

	@Override
	public int nodesDeleted() {
		return getIntValue(Label.NODES_DELETED);
	}

	@Override
	public int labelsAdded() {
		return getIntValue(Label.LABELS_ADDED);
	}

	@Override
	public int relationshipsDeleted() {
		return getIntValue(Label.RELATIONSHIPS_DELETED);
	}

	@Override
	public int relationshipsCreated() {
		return getIntValue(Label.RELATIONSHIPS_CREATED);
	}

	@Override
	public int propertiesSet() {
		return getIntValue(Label.PROPERTIES_SET);
	}
}
