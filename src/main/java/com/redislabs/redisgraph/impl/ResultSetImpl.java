package com.redislabs.redisgraph.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.redislabs.redisgraph.*;

import redis.clients.jedis.util.SafeEncoder;

public class ResultSetImpl implements ResultSet {

    Header header = new HeaderImpl(new ArrayList<>());
    Statistics statistics = new StatisticsImpl(new ArrayList<>());

    private final List<Record> results = new ArrayList<>();

    private int position = 0;

    /**
     *
     * @param rawResponse the raw representation of response is at most 3 lists of objects.
     *                    The last list is the statistics list.
     */
    public ResultSetImpl(List<Object> rawResponse){

        if(rawResponse.size() != 3){

            parseStatistics(rawResponse.get(rawResponse.size()-1));

        }
        else{

            parseHeader((List<List<Object>>)rawResponse.get(0));
            parseResult((List<List<Object>>)rawResponse.get(1));
            parseStatistics((List<Object>)rawResponse.get(2));
        }
    }


    /**
     *
     * @param rawResultSet - raw result set representation
     */
    private void parseResult(List<List<Object>> rawResultSet) {
        if (rawResultSet == null || rawResultSet.isEmpty()) {
            return;
        } else {
            //go over each raw result
            for (List<Object> row : rawResultSet) {

                List<Object> parsedRow = new ArrayList<>();
                //go over each object in the result
                for (int i = 0; i < row.size(); i++) {
                    //get raw representation of the object
                    List<Object> obj = (List<Object>) row.get(i);
                    //get object type
                    Header.ResultSetColumnTypes objType = header.getSchemaTypes().get(i);
                    //deserialize according to type and
                    switch (objType) {
                        case COLUMN_NODE:
                            parsedRow.add(deserializeNode(obj));
                            break;
                        case COLUMN_RELATION:
                            parsedRow.add(deserializeEdge(obj));
                            break;
                        case COLUMN_SCALAR: {
                            parsedRow.add(deserializeScalar(obj));

                        }
                    }

                }
                //create new record from deserialized objects
                Record record = new RecordImpl(header.getSchemaNames(), parsedRow);
                results.add(record);


            }

        }

    }

    /**
     *
     * @param rawStatistics raw statistics representation
     */
    private void parseStatistics(Object rawStatistics){
        statistics = new StatisticsImpl((List<byte[]>)rawStatistics);
    }


    /**
     *
     * @param rawHeader raw header representation
     */
    private void parseHeader(List<List<Object>> rawHeader){
        header = new HeaderImpl(rawHeader);
    }

    @Override
    public Statistics getStatistics() {
        return statistics;
    }

    @Override
    public Header getHeader(){
        return header;
    }


    /**
     * @param rawNodeData - raw node object in the form of list of object
     *                    rawNodeData.get(0) - id (long)
     *                    rawNodeData.get(1) - a list y which contains the labels of this node. Each entry is a label id from the type of long
     *                    rawNodeData.get(2) - a list  which contains the properties of the node.
     * @return Node object
     */
    private Node deserializeNode(List<Object> rawNodeData) {
        Node node = new Node();
        deserializeGraphEntityId(node, rawNodeData.get(0));
        List<Long> labelsIndices = (List<Long>) rawNodeData.get(1);
        for (long labelIndex : labelsIndices) {
            String label = RedisGraphAPI.getInstance().getLabel((int) labelIndex);
            node.addLabel(label);
        }
        deserializeGraphEntityProperties(node, (List<List<Object>>) rawNodeData.get(2));

        return node;

    }

    /**
     * @param graphEntity graph entity
     * @param rawEntityId raw representation of entity id to be set to the graph entity
     */
    private void deserializeGraphEntityId(GraphEntity graphEntity, Object rawEntityId) {
        int id = (int) (long) rawEntityId;
        graphEntity.setId(id);
    }


    /**
     * @param rawEdgeData - a list of objects
     *                    rawEdgeData[0] - edge id
     *                    rawEdgeData[1] - edge relationship type
     *                    rawEdgeData[2] - edge source
     *                    rawEdgeData[3] - edge destination
     *                    rawEdgeData[4] - edge properties
     * @return Edge object
     */
    private Edge deserializeEdge(List<Object> rawEdgeData) {
        Edge edge = new Edge();
        deserializeGraphEntityId(edge, rawEdgeData.get(0));

        String relationshipType = RedisGraphAPI.getInstance().getRelationshipType((int) (long) rawEdgeData.get(1));
        edge.setRelationshipType(relationshipType);

        edge.setSource((int) (long) rawEdgeData.get(2));
        edge.setDestination((int) (long) rawEdgeData.get(3));

        deserializeGraphEntityProperties(edge, (List<List<Object>>) rawEdgeData.get(4));

        return edge;
    }

    /**
     * @param entity        graph entity for adding the properties to
     * @param rawProperties raw representation of a list of graph entity properties. Each entry is a list (rawProperty)
     *                      is a raw representation of property, as follows:
     *                      rawProperty.get(0) - property key
     *                      rawProperty.get(1) - property type
     *                      rawProperty.get(2) - property value
     */
    void deserializeGraphEntityProperties(GraphEntity entity, List<List<Object>> rawProperties) {


        for (List<Object> rawProperty : rawProperties) {
            Property property = new Property();
            property.setName(RedisGraphAPI.getInstance().getPropertyName((int) (long) rawProperty.get(0)));

            //trimmed for getting to value using deserializeScalar
            List<Object> propertyScalar = rawProperty.subList(1, rawProperty.size());
            property.setType(getScalarTypeFromObject(propertyScalar.get(0)));
            property.setValue(deserializeScalar(propertyScalar));

            entity.addProperty(property);

        }

    }

    /**
     * @param rawScalarData - a list of object. list[0] is the scalar type, list[1] is the scalar value
     * @return value of the specific scalar type
     */
    private Object deserializeScalar(List<Object> rawScalarData) {
        ResultSetScalarTypes type = getScalarTypeFromObject(rawScalarData.get(0));
        Object obj = rawScalarData.get(1);
        switch (type) {
            case PROPERTY_NULL:
                return null;
            case PROPERTY_BOOLEAN:
                return Boolean.parseBoolean(SafeEncoder.encode((byte[])obj));
            case PROPERTY_DOUBLE:
                return Double.parseDouble(SafeEncoder.encode((byte[])obj));
            case PROPERTY_INTEGER:
                return (Integer) ((Long) obj).intValue();
            case PROPERTY_STRING:
                return SafeEncoder.encode((byte[]) obj);
            case PROPERTY_UNKNOWN:
            default:
                return obj;
        }
    }

    /**
     * Auxiliary function to retrieve scalar types
     *
     * @param rawScalarType
     * @return scalar type
     */
    private ResultSetScalarTypes getScalarTypeFromObject(Object rawScalarType) {
        return ResultSetScalarTypes.values()[(int) (long) rawScalarType];
    }

    @Override
    public boolean hasNext() {
        return position < results.size();
    }

    @Override
    public Record next() {
        if (!hasNext())
            throw new NoSuchElementException();
        return results.get(position++);
    }


    @Override
    public int size() {
        return results.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResultSetImpl)) return false;
        ResultSetImpl resultSet = (ResultSetImpl) o;
        return Objects.equals(getHeader(), resultSet.getHeader()) &&
                Objects.equals(getStatistics(), resultSet.getStatistics()) &&
                Objects.equals(results, resultSet.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getHeader(), getStatistics(), results);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResultSetImpl{");
        sb.append("header=").append(header);
        sb.append(", statistics=").append(statistics);
        sb.append(", results=").append(results);
        sb.append('}');
        return sb.toString();
    }
}
