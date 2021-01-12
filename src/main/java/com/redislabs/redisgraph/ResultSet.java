package com.redislabs.redisgraph;

import java.util.Iterator;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterable<Record>, Iterator<Record> {

    int size();

    Statistics getStatistics();

    Header getHeader();

}