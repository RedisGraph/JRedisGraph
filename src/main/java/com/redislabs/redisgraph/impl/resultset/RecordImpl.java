package com.redislabs.redisgraph.impl.resultset;

import java.util.List;
import java.util.Objects;

import com.redislabs.redisgraph.Record;

public class RecordImpl implements Record {

    private final List<String> header;
    private final List<Object> values;

 public RecordImpl(List<String> header, List<Object> values){
    this.header=header;
    this.values = values;
  }

    public List<String> getHeader() {
        return header;
    }

    public List<Object> getValues() {
        return values;
    }

  @Override
  public <T> T getValue(int index) {
    return (T)this.values.get(index);
  }

  @Override
  public <T> T getValue(String key) {
    return getValue(this.header.indexOf(key));
  }

  @Override
  public String getString(int index) {
    return this.values.get(index).toString();
  }

  @Override
  public String getString(String key) {
    return getString(this.header.indexOf(key));
  }

  @Override
  public List<String> keys() {
    return header;
  }

  @Override
  public List<Object> values() {
    return this.values;
  }

  @Override
  public boolean containsKey(String key) {
    return this.header.contains(key);
  }

  @Override
  public int size() {
    return this.header.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RecordImpl)) return false;
    RecordImpl record = (RecordImpl) o;
    return Objects.equals(header, record.header) &&
            Objects.equals(values, record.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, values);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Record{");
    sb.append("values=").append(values);
    sb.append('}');
    return sb.toString();
  }
}
