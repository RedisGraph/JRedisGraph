package com.redislabs.redisgraph;

import java.util.Iterator;
import java.util.List;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterator<Record> {

    int size();

    Statistics getStatistics();

    Header getHeader();

    List<Record> getResults();

}