package com.redislabs.redisgraph;

import java.util.Iterator;

/**
 * Hold a query result
 */
public interface ResultSet extends Iterator<Record> {


    enum ResultSetScalarTypes {
        @Deprecated
        PROPERTY_UNKNOWN,
        @Deprecated
        PROPERTY_NULL,
        @Deprecated
        PROPERTY_STRING,
        @Deprecated
        PROPERTY_INTEGER,
        @Deprecated
        PROPERTY_BOOLEAN,
        @Deprecated
        PROPERTY_DOUBLE,
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