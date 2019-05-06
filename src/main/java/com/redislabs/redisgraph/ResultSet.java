package com.redislabs.redisgraph;

import java.util.Iterator;
import java.util.List;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterator<Record> {

    public enum ResultSetScalarTypes {
        PROPERTY_UNKNOWN,
        PROPERTY_NULL,
        PROPERTY_STRING,
        PROPERTY_INTEGER,
        PROPERTY_BOOLEAN,
        PROPERTY_DOUBLE,
    }

    public int size();

    Statistics getStatistics();

    Header getHeader();

}