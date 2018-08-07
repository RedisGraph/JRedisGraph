package com.redislabs.redisgraph;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.redislabs.redisgraph.Statistics.Label;

public class RedisGraphAPITest {
    RedisGraphAPI api;

    public RedisGraphAPITest() {
        api = new RedisGraphAPI("social");
    }

    @Before
    public void flushDB() {
    	api.deleteGraph();
    }

    
    @Test
    public void testCreateNode() throws Exception {
        // Create a node    	
    	ResultSet result = api.query("CREATE ({name:\"roi\",age:32})");
    	Assert.assertFalse(result.hasNext());
    	
    	Assert.assertEquals("1", result.getStatistics().getStringValue(Label.NODES_CREATED));
    	Assert.assertNull(result.getStatistics().getStringValue(Label.NODES_DELETED));
    	Assert.assertNull(result.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
    	Assert.assertNull(result.getStatistics().getStringValue(Label.RELATIONSHIPS_DELETED));
    	Assert.assertEquals("2", result.getStatistics().getStringValue(Label.PROPERTIES_SET));
    	Assert.assertNotNull(result.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
    }

    @Test
    public void testCreateLabeledNode() throws Exception {    	
        // Create a node with a label
    	ResultSet result = api.query("CREATE (:human{name:\"danny\",age:12})");
    	Assert.assertFalse(result.hasNext());
    	
    	Assert.assertEquals("1", result.getStatistics().getStringValue(Label.NODES_CREATED));
    	Assert.assertEquals("2", result.getStatistics().getStringValue(Label.PROPERTIES_SET));
    	Assert.assertNotNull(result.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME)); 
    }

    @Test
    public void testConnectNodes() throws Exception {
        // Create both source and destination nodes
    	ResultSet createResult1 = api.query("CREATE (:person{name:'roi',age:32})");
    	ResultSet createResult2 = api.query("CREATE (:person{name:'amit',age:30})");
    	
    	// Connect source and destination nodes.
    	ResultSet matchResult = api.query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[knows]->(a)");
    	
    	Assert.assertFalse(matchResult.hasNext());
    	Assert.assertNull(matchResult.getStatistics().getStringValue(Label.NODES_CREATED));
    	Assert.assertNull(matchResult.getStatistics().getStringValue(Label.PROPERTIES_SET));
    	Assert.assertEquals("1", matchResult.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
    	Assert.assertNull("0", matchResult.getStatistics().getStringValue(Label.RELATIONSHIPS_DELETED));
    	Assert.assertNotNull(matchResult.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME)); 
    }

    @Test
    public void testQuery() throws Exception {
    	
        // Create both source and destination nodes    	
    	ResultSet create1Result = api.query("CREATE (:qhuman{name:'roi',age:32})");
    	ResultSet create2Result = api.query("CREATE (:qhuman{name:'amit',age:30})");
    	
    	// Connect source and destination nodes.
    	ResultSet connectResult= api.query("MATCH (a:qhuman), (b:qhuman) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[knows]->(b)");

        // Query
        ResultSet resultSet = api.query("MATCH (a:qhuman)-[knows]->(:qhuman) RETURN a");
        
    	Assert.assertTrue(resultSet.hasNext());
    	Assert.assertNull(resultSet.getStatistics().getStringValue(Label.NODES_CREATED));
    	Assert.assertNull(resultSet.getStatistics().getStringValue(Label.PROPERTIES_SET));
    	Assert.assertNull(resultSet.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
    	Assert.assertNull(resultSet.getStatistics().getStringValue(Label.RELATIONSHIPS_DELETED));
    	Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME)); 

    	Record record = resultSet.next();
    	Assert.assertEquals( "roi", record.getString(1));
    	Assert.assertEquals( "32.000000", record.getString(0));
    	
    }
}