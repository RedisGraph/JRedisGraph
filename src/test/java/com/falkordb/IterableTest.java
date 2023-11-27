package com.falkordb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.falkordb.impl.api.Graph;
import com.falkordb.GraphContextGenerator;
import com.falkordb.ResultSet;

public class IterableTest {

    private GraphContextGenerator api;

    @Before
    public void createApi() {
        api = new Graph();
    }

    @After
    public void deleteGraph() {

        api.deleteGraph("social");
        api.close();
    }

    @Test
    public void testRecordsIterator() {
        api.query("social", "UNWIND(range(0,50)) as i CREATE(:N{i:i})");

        ResultSet rs = api.query("social", "MATCH(n) RETURN n");
        int count = 0;
        while (rs.hasNext()) {
            rs.next();
            count++;
        }
        assertEquals(rs.size(), count);
    }

    @Test
    public void testRecordsIterable() {
        api.query("social", "UNWIND(range(0,50)) as i CREATE(:N{i:i})");

        ResultSet rs = api.query("social", "MATCH(n) RETURN n");
        int count = 0;
        for (@SuppressWarnings("unused")
        Record row : rs) {
            count++;
        }
        assertEquals(rs.size(), count);
    }

    @Test
    public void testRecordsIteratorAndIterable() {
        api.query("social", "UNWIND(range(0,50)) as i CREATE(:N{i:i})");

        ResultSet rs = api.query("social", "MATCH(n) RETURN n");
        rs.next();
        int count = 0;
        for (@SuppressWarnings("unused")
        Record row : rs) {
            count++;
        }
        assertEquals(rs.size(), count);
    }

}
