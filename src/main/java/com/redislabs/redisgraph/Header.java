package com.redislabs.redisgraph;

import java.util.List;

/**
 * Query response header interface. Represents the response schame (column names and types)
 */
public interface Header {


    public enum ResultSetColumnTypes {
        COLUMN_UNKNOWN,
        COLUMN_SCALAR,
        COLUMN_NODE,
        COLUMN_RELATION;

    }


    List<String> getSchemaNames();

    List<ResultSetColumnTypes> getSchemaTypes();
}
