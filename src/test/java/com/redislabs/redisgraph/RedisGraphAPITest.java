package com.redislabs.redisgraph;


import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.redislabs.redisgraph.graph_entities.Edge;
import com.redislabs.redisgraph.graph_entities.Node;
import com.redislabs.redisgraph.graph_entities.Property;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.redislabs.redisgraph.impl.resultset.ResultSetImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.redislabs.redisgraph.Statistics.Label;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.exceptions.JedisDataException;

import static com.redislabs.redisgraph.Header.ResultSetColumnTypes.*;

public class RedisGraphAPITest {
    private RedisGraphContextGenerator api;

    public RedisGraphAPITest() {
    }

    @Before
    public void createApi(){
        api = new RedisGraph();
    }
    @After
    public void deleteGraph() {

        api.deleteGraph("social");
        api.close();
    }


    @Test
    public void testCreateNode() {
        // Create a node    	
        ResultSet resultSet = api.query("social", "CREATE ({name:'roi',age:32})");


        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
        Assert.assertNull(resultSet.getStatistics().getStringValue(Label.NODES_DELETED));
        Assert.assertNull(resultSet.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
        Assert.assertNull(resultSet.getStatistics().getStringValue(Label.RELATIONSHIPS_DELETED));
        Assert.assertEquals(2, resultSet.getStatistics().propertiesSet());
        Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));


        Assert.assertFalse(resultSet.hasNext());

        try {
            resultSet.next();
            Assert.fail();
        } catch (NoSuchElementException ignored) {
        }
    }

    @Test
    public void testCreateLabeledNode() {
        // Create a node with a label
        ResultSet resultSet = api.query("social", "CREATE (:human{name:'danny',age:12})");
        Assert.assertFalse(resultSet.hasNext());
        Assert.assertEquals("1", resultSet.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertEquals("2", resultSet.getStatistics().getStringValue(Label.PROPERTIES_SET));
        Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
    }

    @Test
    public void testConnectNodes() {
        // Create both source and destination nodes
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'amit',age:30})"));

        // Connect source and destination nodes.
        ResultSet resultSet = api.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)");

        Assert.assertFalse(resultSet.hasNext());
        Assert.assertNull(resultSet.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(resultSet.getStatistics().getStringValue(Label.PROPERTIES_SET));
        Assert.assertEquals(1, resultSet.getStatistics().relationshipsCreated());
        Assert.assertEquals(0, resultSet.getStatistics().relationshipsDeleted());
        Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));
    }

    @Test
    public void testDeleteNodes(){
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'amit',age:30})"));
        ResultSet deleteResult = api.query("social", "MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        Assert.assertFalse(deleteResult.hasNext());
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.PROPERTIES_SET));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.RELATIONSHIPS_DELETED));
        Assert.assertEquals(1, deleteResult.getStatistics().nodesDeleted());
        Assert.assertNotNull(deleteResult.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));

        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));
        deleteResult = api.query("social", "MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        Assert.assertFalse(deleteResult.hasNext());
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.PROPERTIES_SET));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
        Assert.assertEquals(1, deleteResult.getStatistics().relationshipsDeleted());
        Assert.assertEquals(1, deleteResult.getStatistics().nodesDeleted());

        Assert.assertNotNull(deleteResult.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));



    }

    @Test
    public void testDeleteRelationship(){

        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));
        ResultSet deleteResult = api.query("social", "MATCH (a:person)-[e]->() WHERE (a.name = 'roi') DELETE e");

        Assert.assertFalse(deleteResult.hasNext());
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.PROPERTIES_SET));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.RELATIONSHIPS_CREATED));
        Assert.assertNull(deleteResult.getStatistics().getStringValue(Label.NODES_DELETED));
        Assert.assertEquals(1, deleteResult.getStatistics().relationshipsDeleted());

        Assert.assertNotNull(deleteResult.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));

    }


    @Test
    public void testIndex() {
        // Create both source and destination nodes
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));

        ResultSet createIndexResult = api.query("social", "CREATE INDEX ON :person(age)");
        Assert.assertFalse(createIndexResult.hasNext());
        Assert.assertEquals(1, createIndexResult.getStatistics().indicesAdded());

        ResultSet failCreateIndexResult = api.query("social", "CREATE INDEX ON :person(age1)");
        Assert.assertFalse(failCreateIndexResult.hasNext());
        Assert.assertNull(failCreateIndexResult.getStatistics().getStringValue(Label.INDICES_ADDED));
        Assert.assertEquals(0, failCreateIndexResult.getStatistics().indicesAdded());
    }

    @Test
    public void testHeader(){

        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));

        ResultSet queryResult = api.query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.age");

        Assert.assertNotNull(queryResult.getHeader());
        Header header = queryResult.getHeader();

        List<String> schemaNames = header.getSchemaNames();
        List<Header.ResultSetColumnTypes> schemaTypes = header.getSchemaTypes();

        Assert.assertNotNull(schemaNames);
        Assert.assertNotNull(schemaTypes);

        Assert.assertEquals(3, schemaNames.size());
        Assert.assertEquals(3, schemaTypes.size());

        Assert.assertEquals("a", schemaNames.get(0));
        Assert.assertEquals("r", schemaNames.get(1));
        Assert.assertEquals("a.age", schemaNames.get(2));

        Assert.assertEquals(COLUMN_NODE, schemaTypes.get(0));
        Assert.assertEquals(COLUMN_RELATION, schemaTypes.get(1));
        Assert.assertEquals(COLUMN_SCALAR, schemaTypes.get(2));

    }

    @Test
    public void testRecord(){
        String name = "roi";
        int age = 32;
        double doubleValue = 3.14;
        boolean boolValue  = true;

        String place = "TLV";
        int since = 2000;



        Property nameProperty = new Property("name", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, name);
        Property ageProperty = new Property("age", ResultSet.ResultSetScalarTypes.PROPERTY_INTEGER, age);
        Property doubleProperty = new Property("doubleValue", ResultSet.ResultSetScalarTypes.PROPERTY_DOUBLE, doubleValue);
        Property trueBooleanProperty = new Property("boolValue", ResultSet.ResultSetScalarTypes.PROPERTY_BOOLEAN, true);
        Property falseBooleanProperty = new Property("boolValue", ResultSet.ResultSetScalarTypes.PROPERTY_BOOLEAN, false);
        Property nullProperty = new Property("nullValue", ResultSet.ResultSetScalarTypes.PROPERTY_NULL, null);

        Property placeProperty = new Property("place", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, place);
        Property sinceProperty = new Property("since", ResultSet.ResultSetScalarTypes.PROPERTY_INTEGER, since);

        Node expectedNode = new Node();
        expectedNode.setId(0);
        expectedNode.addLabel("person");
        expectedNode.addProperty(nameProperty);
        expectedNode.addProperty(ageProperty);
        expectedNode.addProperty(doubleProperty);
        expectedNode.addProperty(trueBooleanProperty);
        expectedNode.addProperty(nullProperty);
        Assert.assertEquals(
            "Node{labels=[person], id=0, "
            + "propertyMap={name=Property{name='name', type=PROPERTY_STRING, value=roi}, "
            + "boolValue=Property{name='boolValue', type=PROPERTY_BOOLEAN, value=true}, "
            + "doubleValue=Property{name='doubleValue', type=PROPERTY_DOUBLE, value=3.14}, "
            + "nullValue=Property{name='nullValue', type=PROPERTY_NULL, value=null}, "
            + "age=Property{name='age', type=PROPERTY_INTEGER, value=32}}}", expectedNode.toString());

        Edge expectedEdge = new Edge();
        expectedEdge.setId(0);
        expectedEdge.setSource(0);
        expectedEdge.setDestination(1);
        expectedEdge.setRelationshipType("knows");
        expectedEdge.addProperty(placeProperty);
        expectedEdge.addProperty(sinceProperty);
        expectedEdge.addProperty(doubleProperty);
        expectedEdge.addProperty(falseBooleanProperty);
        expectedEdge.addProperty(nullProperty);
        Assert.assertEquals("Edge{relationshipType='knows', source=0, destination=1, id=0, "
            + "propertyMap={boolValue=Property{name='boolValue', type=PROPERTY_BOOLEAN, value=false}, "
            + "place=Property{name='place', type=PROPERTY_STRING, value=TLV}, "
            + "doubleValue=Property{name='doubleValue', type=PROPERTY_DOUBLE, value=3.14}, "
            + "nullValue=Property{name='nullValue', type=PROPERTY_NULL, value=null}, "
            + "since=Property{name='since', type=PROPERTY_INTEGER, value=2000}}}", expectedEdge.toString());

        Assert.assertNotNull(api.query("social", "CREATE (:person{name:%s,age:%d, doubleValue:%f, boolValue:%b, nullValue:null})", name, age, doubleValue, boolValue));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  " +
                "CREATE (a)-[:knows{place:'TLV', since:2000,doubleValue:3.14, boolValue:false, nullValue:null}]->(b)"));

        ResultSet resultSet = api.query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, " +
                "a.name, a.age, a.doubleValue, a.boolValue, a.nullValue, " +
                "r.place, r.since, r.doubleValue, r.boolValue, r.nullValue");
        Assert.assertNotNull(resultSet);


        Assert.assertEquals(0, resultSet.getStatistics().nodesCreated());
        Assert.assertEquals(0, resultSet.getStatistics().nodesDeleted());
        Assert.assertEquals(0, resultSet.getStatistics().labelsAdded());
        Assert.assertEquals(0, resultSet.getStatistics().propertiesSet());
        Assert.assertEquals(0, resultSet.getStatistics().relationshipsCreated());
        Assert.assertEquals(0, resultSet.getStatistics().relationshipsDeleted());
        Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));


        Assert.assertEquals(1, resultSet.size());
        Assert.assertTrue(resultSet.hasNext());
        Record record = resultSet.next();
        Assert.assertFalse(resultSet.hasNext());

        Node node = record.getValue(0);
        Assert.assertNotNull(node);

        Assert.assertEquals(expectedNode, node);

        node = record.getValue("a");
        Assert.assertEquals(expectedNode, node);

        Edge edge = record.getValue(1);
        Assert.assertNotNull(edge);
        Assert.assertEquals(expectedEdge, edge);

        edge = record.getValue("r");
        Assert.assertEquals(expectedEdge, edge);

        Assert.assertEquals(Arrays.asList("a", "r", "a.name", "a.age", "a.doubleValue", "a.boolValue", "a.nullValue",
                "r.place", "r.since", "r.doubleValue", "r.boolValue", "r.nullValue"), record.keys());

        Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge,
                name, age, doubleValue, true, null,
                place, since, doubleValue, false, null),
                record.values());

        Node a = record.getValue("a");
        for (String propertyName : expectedNode.getEntityPropertyNames()){
            Assert.assertEquals(expectedNode.getProperty(propertyName) ,a.getProperty(propertyName));
        }

        Assert.assertEquals( "roi", record.getString(2));
        Assert.assertEquals( "32", record.getString(3));
        Assert.assertEquals( 32L, ((Integer)(record.getValue(3))).longValue());
        Assert.assertEquals( 32L, ((Integer)record.getValue("a.age")).longValue());
        Assert.assertEquals( "roi", record.getString("a.name"));
        Assert.assertEquals( "32", record.getString("a.age"));

    }


    @Test
    public void tinyTestMultiThread(){
        ResultSet resultSet = api.query("social", "CREATE ({name:'roi',age:32})");
        api.query("social", "MATCH (a:person) RETURN a");
        for (int i =0; i < 10000; i++){
            List<ResultSet> resultSets = IntStream.range(0,16).parallel().
                    mapToObj(
                            j-> api.query("social", "MATCH (a:person) RETURN a")).
                    collect(Collectors.toList());

        }

    }

    @Test
    public void testMultiThread(){

        Assert.assertNotNull(api.query("social", "CREATE (:person {name:'roi', age:32})-[:knows]->(:person {name:'amit',age:30}) "));

        List<ResultSet> resultSets = IntStream.range(0,16).parallel().
                mapToObj(i-> api.query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.age")).
                collect(Collectors.toList());

        Property nameProperty = new Property("name", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, "roi");
        Property ageProperty = new Property("age", ResultSet.ResultSetScalarTypes.PROPERTY_INTEGER, 32);
        Property lastNameProperty =new Property("lastName", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, "a");

        Node expectedNode = new Node();
        expectedNode.setId(0);
        expectedNode.addLabel("person");
        expectedNode.addProperty(nameProperty);
        expectedNode.addProperty(ageProperty);


        Edge expectedEdge = new Edge();
        expectedEdge.setId(0);
        expectedEdge.setSource(0);
        expectedEdge.setDestination(1);
        expectedEdge.setRelationshipType("knows");


        for (ResultSet resultSet : resultSets){
            Assert.assertNotNull(resultSet.getHeader());
            Header header = resultSet.getHeader();
            List<String> schemaNames = header.getSchemaNames();
            List<Header.ResultSetColumnTypes> schemaTypes = header.getSchemaTypes();
            Assert.assertNotNull(schemaNames);
            Assert.assertNotNull(schemaTypes);
            Assert.assertEquals(3, schemaNames.size());
            Assert.assertEquals(3, schemaTypes.size());
            Assert.assertEquals("a", schemaNames.get(0));
            Assert.assertEquals("r", schemaNames.get(1));
            Assert.assertEquals("a.age", schemaNames.get(2));
            Assert.assertEquals(COLUMN_NODE, schemaTypes.get(0));
            Assert.assertEquals(COLUMN_RELATION, schemaTypes.get(1));
            Assert.assertEquals(COLUMN_SCALAR, schemaTypes.get(2));
            Assert.assertEquals(1, resultSet.size());
            Assert.assertTrue(resultSet.hasNext());
            Record record = resultSet.next();
            Assert.assertFalse(resultSet.hasNext());
            Assert.assertEquals(Arrays.asList("a", "r", "a.age"), record.keys());
            Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge, 32), record.values());
        }

        //test for update in local cache
        expectedNode.removeProperty("name");
        expectedNode.removeProperty("age");
        expectedNode.addProperty(lastNameProperty);
        expectedNode.removeLabel("person");
        expectedNode.addLabel("worker");
        expectedNode.setId(2);


        expectedEdge.setRelationshipType("worksWith");
        expectedEdge.setSource(2);
        expectedEdge.setDestination(3);
        expectedEdge.setId(1);

        Assert.assertNotNull(api.query("social", "CREATE (:worker{lastName:'a'})"));
        Assert.assertNotNull(api.query("social", "CREATE (:worker{lastName:'b'})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:worker), (b:worker) WHERE (a.lastName = 'a' AND b.lastName='b')  CREATE (a)-[:worksWith]->(b)"));

        resultSets = IntStream.range(0,16).parallel().
                mapToObj(i-> api.query("social", "MATCH (a:worker)-[r:worksWith]->(b:worker) RETURN a,r")).
                collect(Collectors.toList());

        for (ResultSet resultSet : resultSets){
            Assert.assertNotNull(resultSet.getHeader());
            Header header = resultSet.getHeader();
            List<String> schemaNames = header.getSchemaNames();
            List<Header.ResultSetColumnTypes> schemaTypes = header.getSchemaTypes();
            Assert.assertNotNull(schemaNames);
            Assert.assertNotNull(schemaTypes);
            Assert.assertEquals(2, schemaNames.size());
            Assert.assertEquals(2, schemaTypes.size());
            Assert.assertEquals("a", schemaNames.get(0));
            Assert.assertEquals("r", schemaNames.get(1));
            Assert.assertEquals(COLUMN_NODE, schemaTypes.get(0));
            Assert.assertEquals(COLUMN_RELATION, schemaTypes.get(1));
            Assert.assertEquals(1, resultSet.size());
            Assert.assertTrue(resultSet.hasNext());
            Record record = resultSet.next();
            Assert.assertFalse(resultSet.hasNext());
            Assert.assertEquals(Arrays.asList("a", "r"), record.keys());
            Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge), record.values());
        }
    }


    @Test
    public void testAdditionToProcedures(){

        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'roi',age:32})"));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'amit',age:30})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        //expected objects init
        Property nameProperty = new Property("name", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, "roi");
        Property ageProperty = new Property("age", ResultSet.ResultSetScalarTypes.PROPERTY_INTEGER, 32);
        Property lastNameProperty =new Property("lastName", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, "a");

        Node expectedNode = new Node();
        expectedNode.setId(0);
        expectedNode.addLabel("person");
        expectedNode.addProperty(nameProperty);
        expectedNode.addProperty(ageProperty);


        Edge expectedEdge = new Edge();
        expectedEdge.setId(0);
        expectedEdge.setSource(0);
        expectedEdge.setDestination(1);
        expectedEdge.setRelationshipType("knows");


        ResultSet resultSet = api.query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r");
        Assert.assertNotNull(resultSet.getHeader());
        Header header = resultSet.getHeader();
        List<String> schemaNames = header.getSchemaNames();
        List<Header.ResultSetColumnTypes> schemaTypes = header.getSchemaTypes();
        Assert.assertNotNull(schemaNames);
        Assert.assertNotNull(schemaTypes);
        Assert.assertEquals(2, schemaNames.size());
        Assert.assertEquals(2, schemaTypes.size());
        Assert.assertEquals("a", schemaNames.get(0));
        Assert.assertEquals("r", schemaNames.get(1));
        Assert.assertEquals(COLUMN_NODE, schemaTypes.get(0));
        Assert.assertEquals(COLUMN_RELATION, schemaTypes.get(1));
        Assert.assertEquals(1, resultSet.size());
        Assert.assertTrue(resultSet.hasNext());
        Record record = resultSet.next();
        Assert.assertFalse(resultSet.hasNext());
        Assert.assertEquals(Arrays.asList("a", "r"), record.keys());
        Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge), record.values());

        //test for local cache updates

        expectedNode.removeProperty("name");
        expectedNode.removeProperty("age");
        expectedNode.addProperty(lastNameProperty);
        expectedNode.removeLabel("person");
        expectedNode.addLabel("worker");
        expectedNode.setId(2);
        expectedEdge.setRelationshipType("worksWith");
        expectedEdge.setSource(2);
        expectedEdge.setDestination(3);
        expectedEdge.setId(1);
        Assert.assertNotNull(api.query("social", "CREATE (:worker{lastName:'a'})"));
        Assert.assertNotNull(api.query("social", "CREATE (:worker{lastName:'b'})"));
        Assert.assertNotNull(api.query("social", "MATCH (a:worker), (b:worker) WHERE (a.lastName = 'a' AND b.lastName='b')  CREATE (a)-[:worksWith]->(b)"));
        resultSet = api.query("social", "MATCH (a:worker)-[r:worksWith]->(b:worker) RETURN a,r");
        Assert.assertNotNull(resultSet.getHeader());
        header = resultSet.getHeader();
        schemaNames = header.getSchemaNames();
        schemaTypes = header.getSchemaTypes();
        Assert.assertNotNull(schemaNames);
        Assert.assertNotNull(schemaTypes);
        Assert.assertEquals(2, schemaNames.size());
        Assert.assertEquals(2, schemaTypes.size());
        Assert.assertEquals("a", schemaNames.get(0));
        Assert.assertEquals("r", schemaNames.get(1));
        Assert.assertEquals(COLUMN_NODE, schemaTypes.get(0));
        Assert.assertEquals(COLUMN_RELATION, schemaTypes.get(1));
        Assert.assertEquals(1, resultSet.size());
        Assert.assertTrue(resultSet.hasNext());
        record = resultSet.next();
        Assert.assertFalse(resultSet.hasNext());
        Assert.assertEquals(Arrays.asList("a", "r"), record.keys());
        Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge), record.values());

    }


    @Test
    public void testEscapedQuery() {
        Assert.assertNotNull(api.query("social", "CREATE (:escaped{s1:%s,s2:%s})", "S\"'", "S'\""));
        Assert.assertNotNull(api.query("social", "MATCH (n) where n.s1=%s and n.s2=%s RETURN n", "S\"'", "S'\""));
        Assert.assertNotNull(api.query("social", "MATCH (n) where n.s1='S\"' RETURN n"));
    }


    @Test
    public void testMultiExec(){
        try (RedisGraphContext c = api.getContext()) {
            RedisGraphTransaction transaction = api.getContext().multi();

            transaction.set("x", "1");
            transaction.query("social", "CREATE (:Person {name:'a'})");
            transaction.query("g", "CREATE (:Person {name:'a'})");
            transaction.incr("x");
            transaction.get("x");
            transaction.query("social", "MATCH (n:Person) RETURN n");
            transaction.deleteGraph("g");
            transaction.callProcedure("social", "db.labels");
            List<Object> results = transaction.exec();

            // Redis set command
            Assert.assertEquals(String.class, results.get(0).getClass());
            Assert.assertEquals("OK", results.get(0));

            // Redis graph command
            Assert.assertEquals(ResultSetImpl.class, results.get(1).getClass());
            ResultSet resultSet = (ResultSet) results.get(1);
            Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
            Assert.assertEquals(1, resultSet.getStatistics().propertiesSet());


            Assert.assertEquals(ResultSetImpl.class, results.get(2).getClass());
            resultSet = (ResultSet) results.get(2);
            Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
            Assert.assertEquals(1, resultSet.getStatistics().propertiesSet());

            // Redis incr command
            Assert.assertEquals(Long.class, results.get(3).getClass());
            Assert.assertEquals((long)2, results.get(3));

            // Redis get command
            Assert.assertEquals(String.class, results.get(4).getClass());
            Assert.assertEquals("2", results.get(4));

            // Graph query result
            Assert.assertEquals(ResultSetImpl.class, results.get(5).getClass());
            resultSet = (ResultSet) results.get(5);

            Assert.assertNotNull(resultSet.getHeader());
            Header header = resultSet.getHeader();


            List<String> schemaNames = header.getSchemaNames();
            List<Header.ResultSetColumnTypes> schemaTypes = header.getSchemaTypes();

            Assert.assertNotNull(schemaNames);
            Assert.assertNotNull(schemaTypes);

            Assert.assertEquals(1, schemaNames.size());
            Assert.assertEquals(1, schemaTypes.size());

            Assert.assertEquals("n", schemaNames.get(0));

            Assert.assertEquals(COLUMN_NODE, schemaTypes.get(0));

            Property nameProperty = new Property("name", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, "a");

            Node expectedNode = new Node();
            expectedNode.setId(0);
            expectedNode.addLabel("Person");
            expectedNode.addProperty(nameProperty);
            // see that the result were pulled from the right graph
            Assert.assertEquals(1, resultSet.size());
            Assert.assertTrue(resultSet.hasNext());
            Record record = resultSet.next();
            Assert.assertFalse(resultSet.hasNext());
            Assert.assertEquals(Arrays.asList("n"), record.keys());
            Assert.assertEquals(expectedNode, record.getValue("n"));

            // Graph delete
            Assert.assertTrue(((String)results.get(6)).startsWith("Graph removed"));


            Assert.assertEquals(ResultSetImpl.class, results.get(7).getClass());
            resultSet = (ResultSet) results.get(7);

            Assert.assertNotNull(resultSet.getHeader());
            header = resultSet.getHeader();


            schemaNames = header.getSchemaNames();
            schemaTypes = header.getSchemaTypes();

            Assert.assertNotNull(schemaNames);
            Assert.assertNotNull(schemaTypes);

            Assert.assertEquals(1, schemaNames.size());
            Assert.assertEquals(1, schemaTypes.size());

            Assert.assertEquals("label", schemaNames.get(0));

            Assert.assertEquals(COLUMN_SCALAR, schemaTypes.get(0));

            Assert.assertEquals(1, resultSet.size());
            Assert.assertTrue(resultSet.hasNext());
            record = resultSet.next();
            Assert.assertFalse(resultSet.hasNext());
            Assert.assertEquals(Arrays.asList("label"), record.keys());
            Assert.assertEquals("Person", record.getValue("label"));
        }
    }

    @Test
    public void testContextedAPI() {

        String name = "roi";
        int age = 32;
        double doubleValue = 3.14;
        boolean boolValue = true;

        String place = "TLV";
        int since = 2000;


        Property nameProperty = new Property("name", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, name);
        Property ageProperty = new Property("age", ResultSet.ResultSetScalarTypes.PROPERTY_INTEGER, age);
        Property doubleProperty = new Property("doubleValue", ResultSet.ResultSetScalarTypes.PROPERTY_DOUBLE, doubleValue);
        Property trueBooleanProperty = new Property("boolValue", ResultSet.ResultSetScalarTypes.PROPERTY_BOOLEAN, true);
        Property falseBooleanProperty = new Property("boolValue", ResultSet.ResultSetScalarTypes.PROPERTY_BOOLEAN, false);
        Property nullProperty = new Property("nullValue", ResultSet.ResultSetScalarTypes.PROPERTY_NULL, null);

        Property placeProperty = new Property("place", ResultSet.ResultSetScalarTypes.PROPERTY_STRING, place);
        Property sinceProperty = new Property("since", ResultSet.ResultSetScalarTypes.PROPERTY_INTEGER, since);

        Node expectedNode = new Node();
        expectedNode.setId(0);
        expectedNode.addLabel("person");
        expectedNode.addProperty(nameProperty);
        expectedNode.addProperty(ageProperty);
        expectedNode.addProperty(doubleProperty);
        expectedNode.addProperty(trueBooleanProperty);
        expectedNode.addProperty(nullProperty);

        Edge expectedEdge = new Edge();
        expectedEdge.setId(0);
        expectedEdge.setSource(0);
        expectedEdge.setDestination(1);
        expectedEdge.setRelationshipType("knows");
        expectedEdge.addProperty(placeProperty);
        expectedEdge.addProperty(sinceProperty);
        expectedEdge.addProperty(doubleProperty);
        expectedEdge.addProperty(falseBooleanProperty);
        expectedEdge.addProperty(nullProperty);

        try (RedisGraphContext c = api.getContext()) {
            Assert.assertNotNull(c.query("social", "CREATE (:person{name:%s,age:%d, doubleValue:%f, boolValue:%b, nullValue:null})", name, age, doubleValue, boolValue));
            Assert.assertNotNull(c.query("social", "CREATE (:person{name:'amit',age:30})"));
            Assert.assertNotNull(c.query("social", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  " +
                    "CREATE (a)-[:knows{place:'TLV', since:2000,doubleValue:3.14, boolValue:false, nullValue:null}]->(b)"));

            ResultSet resultSet = c.query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, " +
                    "a.name, a.age, a.doubleValue, a.boolValue, a.nullValue, " +
                    "r.place, r.since, r.doubleValue, r.boolValue, r.nullValue");
            Assert.assertNotNull(resultSet);


            Assert.assertEquals(0, resultSet.getStatistics().nodesCreated());
            Assert.assertEquals(0, resultSet.getStatistics().nodesDeleted());
            Assert.assertEquals(0, resultSet.getStatistics().labelsAdded());
            Assert.assertEquals(0, resultSet.getStatistics().propertiesSet());
            Assert.assertEquals(0, resultSet.getStatistics().relationshipsCreated());
            Assert.assertEquals(0, resultSet.getStatistics().relationshipsDeleted());
            Assert.assertNotNull(resultSet.getStatistics().getStringValue(Label.QUERY_INTERNAL_EXECUTION_TIME));


            Assert.assertEquals(1, resultSet.size());
            Assert.assertTrue(resultSet.hasNext());
            Record record = resultSet.next();
            Assert.assertFalse(resultSet.hasNext());

            Node node = record.getValue(0);
            Assert.assertNotNull(node);

            Assert.assertEquals(expectedNode, node);

            node = record.getValue("a");
            Assert.assertEquals(expectedNode, node);

            Edge edge = record.getValue(1);
            Assert.assertNotNull(edge);
            Assert.assertEquals(expectedEdge, edge);

            edge = record.getValue("r");
            Assert.assertEquals(expectedEdge, edge);

            Assert.assertEquals(Arrays.asList("a", "r", "a.name", "a.age", "a.doubleValue", "a.boolValue", "a.nullValue",
                    "r.place", "r.since", "r.doubleValue", "r.boolValue", "r.nullValue"), record.keys());

            Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge,
                    name, age, doubleValue, true, null,
                    place, since, doubleValue, false, null),
                    record.values());

            Node a = record.getValue("a");
            for (String propertyName : expectedNode.getEntityPropertyNames()) {
                Assert.assertEquals(expectedNode.getProperty(propertyName), a.getProperty(propertyName));
            }

            Assert.assertEquals("roi", record.getString(2));
            Assert.assertEquals("32", record.getString(3));
            Assert.assertEquals(32L, ((Integer) (record.getValue(3))).longValue());
            Assert.assertEquals(32L, ((Integer) record.getValue("a.age")).longValue());
            Assert.assertEquals("roi", record.getString("a.name"));
            Assert.assertEquals("32", record.getString("a.age"));
        }
    }

    @Test
    public void testWriteTransactionWatch(){

        RedisGraphContext c1 = api.getContext();
        RedisGraphContext c2 = api.getContext();

        c1.watch("social");
        RedisGraphTransaction t1 = c1.multi();


        t1.query("social", "CREATE (:Person {name:'a'})");
        c2.query("social", "CREATE (:Person {name:'b'})");
        List<Object> returnValue = t1.exec();
        Assert.assertNull(returnValue);
        c1.close();
        c2.close();
    }

    @Test
    public void testReadTransactionWatch(){

        RedisGraphContext c1 = api.getContext();
        RedisGraphContext c2 = api.getContext();
        Assert.assertNotEquals(c1.getConnectionContext(), c2.getConnectionContext());
        c1.query("social", "CREATE (:Person {name:'a'})");
        c1.watch("social");
        RedisGraphTransaction t1 = c1.multi();

        t1.query("social", "CREATE (:Person {name:'b'})");
        c2.query("social", "MATCH (n) return n");
        List<Object> returnValue = t1.exec();

        Assert.assertNotNull(returnValue);
        c1.close();
        c2.close();
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testRuntimeException() {

        api.query("social", "create ()");
        exceptionRule.expect(JedisDataException.class);
        exceptionRule.expectMessage("Type mismatch: expected Integer but was String");
        api.query("social", "RETURN abs('q')");

    }
}
