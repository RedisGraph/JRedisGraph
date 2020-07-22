package com.redislabs.redisgraph.graph_entities;

import java.util.Objects;

/**
 * A Graph entity property. Has a name, type, and value
 */
public class Property <T> {

    //members
    private String name;
    private T value;


    /**
     * Default constructor
     */
    public Property() {

    }

    /**
     * Parameterized constructor
     *
     * @param name
     * @param value
     */
    public Property(String name, T value) {
        this.name = name;
        this.value = value;
    }


    //getters & setters

    /**
     * @return property name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name - property name to be set
     */
    public void setName(String name) {
        this.name = name;
    }


    /**
     * @return property value
     */
    public T getValue() {
        return value;
    }


    /**
     * @param value property value to be set
     */
    public void setValue(T value) {
        this.value = value;
    }

    private boolean valueEquals(Object value1, Object value2) {
        if(value1 instanceof Integer) value1 = Long.valueOf(((Integer) value1).longValue());
        if(value2 instanceof Integer) value2 = Long.valueOf(((Integer) value2).longValue());
        return Objects.equals(value1, value2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Property)) return false;
        Property property = (Property) o;
        return Objects.equals(name, property.name) &&
                valueEquals(value, property.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    /**
     * Default toString implementation
     * @return
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Property{");
        sb.append("name='").append(name).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
