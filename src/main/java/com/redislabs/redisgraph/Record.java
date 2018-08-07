package com.redislabs.redisgraph;

import java.util.List;

/**
 * Container for RedisGraph result values.
 * 
 * List records are returned from RedisGraph statement execution, contained within a ResultSet.
 */
public interface Record {
	
	/**
	 * The value at the given field index (represented as String)
	 * 
	 * @param index
	 * @return
	 */
	String getString(int index);
	
	/**
	 * The value at the given field (represented as String)
	 * 
	 * @param key
	 * @return
	 */
	String getString(String key);
	
	/**
	 * The keys of the record
	 * 
	 * @return
	 */
	List<String> keys();
	
	/**
	 * The values of the record
	 * 
	 * @return
	 */
	List<String> values();
	
	/**
	 * Check if the keys contain the given key
	 * 
	 * @param key
	 * @return
	 */
	boolean	containsKey(String key);
	
	/**
	 * The number of fields in this record
	 * 
	 * @return
	 */
	int size();
}
