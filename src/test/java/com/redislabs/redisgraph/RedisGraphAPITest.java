package com.redislabs.redisgraph;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.redislabs.redisgraph.Statistics.Label;

public class RedisGraphAPITest {
    RedisGraphAPI api;

    public RedisGraphAPITest() {
        api = new RedisGraphAPI("social");
        
        /* Dummy call to generate graph so the first deleteGraph() won't fail */
        api.query("CREATE ({name:'roi',age:32})");
    }

    @Before
    public void deleteGraph(){
    	api.deleteGraph();
    }
    
    @Test
    public void testCreateNode(){
        // Create a node    	
    	ResultSet result = api.query("CREATE ({name:'roi',age:32})");
    	Assert.assertFalse(result.hasNext());
    	
    	try {
    	  result.next();
    	  Assert.fail();
    	}catch(NoSuchElementException e) {}
    	
    	Assert.assertEquals(1, result.getStatistics().nodesCreated());
    	Assert.assertNull(result.getStatistics().getStringValue(Label.NODES_DELETED));
    	Assert.assertNull(result.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
    	Assert.assertNull(result.getStatistics().getStringValue(Label.RELATIONSHIPS_DELETED));
    	Assert.assertEquals(2, result.getStatistics().propertiesSet());
    	Assert.assertNotNull(result.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
    }

    @Test
    public void testCreateLabeledNode(){    	
        // Create a node with a label
    	ResultSet result = api.query("CREATE (:human{name:'danny',age:12})");
    	Assert.assertFalse(result.hasNext());
    	
    	Assert.assertEquals("1", result.getStatistics().getStringValue(Label.NODES_CREATED));
    	Assert.assertEquals("2", result.getStatistics().getStringValue(Label.PROPERTIES_SET));
    	Assert.assertNotNull(result.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME)); 
    }

    @Test
    public void testConnectNodes(){
        // Create both source and destination nodes
        Assert.assertNotNull(api.query("CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("CREATE (:person{name:'amit',age:30})"));
    	
    	// Connect source and destination nodes.
    	ResultSet matchResult = api.query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)");
    	
    	Assert.assertFalse(matchResult.hasNext());
    	Assert.assertNull(matchResult.getStatistics().getStringValue(Label.NODES_CREATED));
    	Assert.assertNull(matchResult.getStatistics().getStringValue(Label.PROPERTIES_SET));
    	Assert.assertEquals(1, matchResult.getStatistics().relationshipsCreated());
    	Assert.assertEquals(0, matchResult.getStatistics().relationshipsDeleted());
    	Assert.assertNotNull(matchResult.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME)); 
    }
    
    @Test
    public void testIndex(){
        // Create both source and destination nodes
      Assert.assertNotNull(api.query("CREATE (:person{name:'roi',age:32})"));

      ResultSet createIndexResult = api.query("CREATE INDEX ON :person(age)");
      Assert.assertFalse(createIndexResult.hasNext());
      Assert.assertEquals(1, createIndexResult.getStatistics().indicesAdded());
      
      ResultSet failCreateIndexResult = api.query("CREATE INDEX ON :person(age1)");
      Assert.assertFalse(failCreateIndexResult.hasNext());
      Assert.assertNull(failCreateIndexResult.getStatistics().getStringValue(Label.INDICES_ADDED));
      Assert.assertEquals(0, failCreateIndexResult.getStatistics().indicesAdded());
    }


    @Test
    public void testQuery(){
    	
        // Create both source and destination nodes    	
        Assert.assertNotNull(api.query("CREATE (:qhuman{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("CREATE (:qhuman{name:'amit',age:30})"));
    	
    	// Connect source and destination nodes.
        Assert.assertNotNull(api.query("MATCH (a:qhuman), (b:qhuman) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        // Query
        ResultSet resultSet = api.query("MATCH (a:qhuman)-[knows]->(:qhuman) RETURN a");
        
        Assert.assertEquals(Arrays.asList("a.age", "a.name"), resultSet.getHeader());
        Assert.assertEquals("[a.age, a.name]\n[[32, roi]]\n" + resultSet.getStatistics(), resultSet.toString());        
        
    	Assert.assertTrue(resultSet.hasNext());
    	Assert.assertEquals(0, resultSet.getStatistics().nodesCreated());
    	Assert.assertEquals(0, resultSet.getStatistics().nodesDeleted());
    	Assert.assertEquals(0, resultSet.getStatistics().labelsAdded());
    	Assert.assertEquals(0, resultSet.getStatistics().propertiesSet());
    	Assert.assertEquals(0, resultSet.getStatistics().relationshipsCreated());
    	Assert.assertEquals(0, resultSet.getStatistics().relationshipsDeleted());
    	Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME)); 

    	Record record = resultSet.next();
    	Assert.assertEquals( Arrays.asList("a.age", "a.name"), record.keys());
    	Assert.assertEquals( Arrays.asList(32L, "roi"), record.values());
    	Assert.assertTrue(record.containsKey("a.name"));
        Assert.assertEquals( 2, record.size());
        Assert.assertEquals( "[32, roi]", record.toString());
        
    	Assert.assertEquals( "roi", record.getString(1));
    	Assert.assertEquals( "32", record.getString(0));
    	Assert.assertEquals( 32L, ((Long)record.getValue(0)).longValue());
    	Assert.assertEquals( 32L, ((Long)record.getValue("a.age")).longValue());
        Assert.assertEquals( "roi", record.getString("a.name"));
        Assert.assertEquals( "32", record.getString("a.age"));     
    }
}
