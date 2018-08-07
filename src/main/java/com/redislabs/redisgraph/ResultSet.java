package com.redislabs.redisgraph;

import java.util.Iterator;

public interface ResultSet extends Iterator<Record>{
	Statistics getStatistics();
}
