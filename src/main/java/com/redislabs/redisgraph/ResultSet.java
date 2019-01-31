package com.redislabs.redisgraph;

import java.util.Iterator;
import java.util.List;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterator<Record>{
	/**
	 * Return the query statistics
	 * @return statistics object
	 */
	Statistics getStatistics();

	List<String> getHeader();
}
