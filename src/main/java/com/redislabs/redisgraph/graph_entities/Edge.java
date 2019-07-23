package com.redislabs.redisgraph.graph_entities;

import java.util.Objects;

/**
 * A class represent an edge (graph entity). In addition to the base class id and properties, an edge shows its source,
 * destination and relationship type
 */
public class Edge extends GraphEntity {

    //members
    private  String relationshipType;
    private int source;
    private int destination;


    //getters & setters

    /**
     * @return the edge relationship type
     */
    public String getRelationshipType() {
        return relationshipType;
    }

    /**
     * @param relationshipType - the relationship type to be set.
     */
    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }


    /**
     * @return The id of the source node
     */
    public int getSource() {
        return source;
    }

    /**
     * @param source - The id of the source node to be set
     */
    public void setSource(int source) {
        this.source = source;
    }

    /**
     *
     * @return the id of the destination node
     */
    public int getDestination() {
        return destination;
    }

    /**
     *
     * @param destination - The id of the destination node to be set
     */
    public void setDestination(int destination) {
        this.destination = destination;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Edge)) return false;
        if (!super.equals(o)) return false;
        Edge edge = (Edge) o;
        return getSource() == edge.getSource() &&
                getDestination() == edge.getDestination() &&
                Objects.equals(getRelationshipType(), edge.getRelationshipType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relationshipType, source, destination);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Edge{");
        sb.append("relationshipType='").append(getRelationshipType()).append('\'');
        sb.append(", source=").append(getSource());
        sb.append(", destination=").append(getDestination());
        sb.append(", id=").append(getId());
        sb.append(", propertyMap=").append(propertyMap);
        sb.append('}');
        return sb.toString();
    }
}
