package com.redislabs.redisgraph;

import java.util.List;

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
