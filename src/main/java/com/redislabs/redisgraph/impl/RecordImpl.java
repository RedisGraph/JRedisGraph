package com.redislabs.redisgraph.impl;

import java.util.List;

import com.redislabs.redisgraph.Record;

public class RecordImpl implements Record {
	
    final private List<String> header;
    final private List<String> values;

    RecordImpl(List<String> header, List<String> values){
    	this.header=header;
    	this.values = values;
    }
    
	@Override
	public String getString(int index) {
		return this.values.get(index);
	}

	@Override
	public String getString(String key) {
		return getString(this.header.indexOf(key));
	}

	@Override
	public List<String> keys() {
		return header;
	}

	@Override
	public List<String> values() {
		return this.values;
	}

	@Override
	public boolean containsKey(String key) {
		return this.header.contains(key);
	}

	@Override
	public int size() {
		return this.header.size();
	}

}
