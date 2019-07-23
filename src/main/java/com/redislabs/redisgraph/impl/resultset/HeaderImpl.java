package com.redislabs.redisgraph.impl.resultset;

import com.redislabs.redisgraph.Header;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Query result header interface implementation
 */
public class HeaderImpl implements Header {

    //members
    private final List<List<Object>> raw;
    private final List<ResultSetColumnTypes> schemaTypes = new ArrayList<>();
    private final List<String> schemaNames = new ArrayList<>();


    /**
     * Parameterized constructor
     * A raw representation of a header (query response schema) is a list.
     * Each entry in the list is a tuple (list of size 2).
     * tuple[0] represents the type of the column, and tuple[1] represents the name of the column.
     *
     * @param raw - raw representation of a header
     */
    public HeaderImpl(List<List<Object>> raw) {
        this.raw = raw;
    }


    /**
     * @return a list of column names, ordered by they appearance in the query
     */
    @Override
    public List<String> getSchemaNames() {
        if (schemaNames.size() == 0) {
            buildSchema();
        }
        return schemaNames;
    }

    /**
     * @return a list of column types, ordered by they appearance in the query
     */
    @Override
    public List<ResultSetColumnTypes> getSchemaTypes() {
        if (schemaTypes.size() == 0) {
            buildSchema();
        }
        return schemaTypes;
    }

    /**
     * Extracts schema names and types from the raw representation
     */
    private void buildSchema() {
        for (List<Object> tuple : this.raw) {

            //get type
            ResultSetColumnTypes type = ResultSetColumnTypes.values()[((Long) tuple.get(0)).intValue()];
            //get text
            String text = SafeEncoder.encode((byte[]) tuple.get(1));
            if (type != null) {
                schemaTypes.add(type);
                schemaNames.add(text);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HeaderImpl)) return false;
        HeaderImpl header = (HeaderImpl) o;
        return Objects.equals(getSchemaTypes(), header.getSchemaTypes()) &&
                Objects.equals(getSchemaNames(), header.getSchemaNames());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSchemaTypes(), getSchemaNames());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HeaderImpl{");
        sb.append("schemaTypes=").append(schemaTypes);
        sb.append(", schemaNames=").append(schemaNames);
        sb.append('}');
        return sb.toString();
    }
}
