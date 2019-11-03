package com.redislabs.redisgraph.impl.resultset;

import redis.clients.jedis.exceptions.JedisDataException;

enum ResultSetScalarTypes {
    VALUE_UNKNOWN,
    VALUE_NULL,
    VALUE_STRING,
    VALUE_INTEGER,
    VALUE_BOOLEAN,
    VALUE_DOUBLE,
    VALUE_ARRAY,
    VALUE_EDGE,
    VALUE_NODE,
    VALUE_PATH;


    static ResultSetScalarTypes[] values = values();

    public static ResultSetScalarTypes getValue(int index) {
        if (index < 0 || index > values.length) throw new JedisDataException("Unrecognized response type");
        return values[index];
    }

}
