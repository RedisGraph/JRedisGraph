package com.redislabs.redisgraph;

import java.util.List;

/**
 * Query response header interface. Represents the response schema (column names and types)
 */
public interface Header {


    enum ResultSetColumnTypes {
        COLUMN_UNKNOWN,
        COLUMN_SCALAR,
        COLUMN_NODE,
        COLUMN_RELATION

    }


    List<String> getSchemaNames();

    List<ResultSetColumnTypes> getSchemaTypes();
}
