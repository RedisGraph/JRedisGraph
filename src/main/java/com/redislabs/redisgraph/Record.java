package com.redislabs.redisgraph;

import java.util.List;

public interface Record {
	String getString(int index);
	String getString(String key);
	List<String> keys();
	List<String> values();
	boolean	containsKey(String key);
	int size();
}
