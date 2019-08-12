package com.redislabs.redisgraph;

import java.util.Iterator;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterator<Record> {

    @Deprecated
    enum ResultSetScalarTypes {
        PROPERTY_UNKNOWN,
        PROPERTY_NULL,
        PROPERTY_STRING,
        PROPERTY_INTEGER,
        PROPERTY_BOOLEAN,
        PROPERTY_DOUBLE
    }

    enum ResultSetValueTypes {
        VALUE_UNKNOWN,
        VALUE_NULL,
        VALUE_STRING,
        VALUE_INTEGER,
        VALUE_BOOLEAN,
        VALUE_DOUBLE,
        VALUE_ARRAY,
        VALUE_EDGE,
        VALUE_NODE
    }

    int size();

    Statistics getStatistics();

    Header getHeader();

}