package com.redislabs.redisgraph;

import java.util.Iterator;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterator<Record> {

    enum ResultSetScalarTypes {
        PROPERTY_UNKNOWN,
        PROPERTY_NULL,
        PROPERTY_STRING,
        PROPERTY_INTEGER,
        PROPERTY_BOOLEAN,
        PROPERTY_DOUBLE,
    }

    int size();

    Statistics getStatistics();

    Header getHeader();

}