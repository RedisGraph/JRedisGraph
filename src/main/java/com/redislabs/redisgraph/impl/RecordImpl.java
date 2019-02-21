package com.redislabs.redisgraph.impl;

import java.util.List;

import com.redislabs.redisgraph.Record;

public class RecordImpl implements Record {

  private final List<String> header;
  private final List<Object> values;

  RecordImpl(List<String> header, List<Object> values){
    this.header=header;
    this.values = values;
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
  public String toString() {
    return this.values.toString();
  }


}
