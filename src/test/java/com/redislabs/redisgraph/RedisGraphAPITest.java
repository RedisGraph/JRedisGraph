package com.redislabs.redisgraph;


import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.redislabs.redisgraph.graph_entities.Edge;
import com.redislabs.redisgraph.graph_entities.Node;
import com.redislabs.redisgraph.graph_entities.Path;
import com.redislabs.redisgraph.graph_entities.Property;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.redislabs.redisgraph.impl.resultset.ResultSetImpl;
import com.redislabs.redisgraph.test.utils.PathBuilder;
import org.junit.*;

import com.redislabs.redisgraph.Statistics.Label;

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

        // since RediSearch as index, those action are allowed
        ResultSet createNonExistingIndexResult = api.query("social", "CREATE INDEX ON :person(age1)");
        Assert.assertFalse(createNonExistingIndexResult.hasNext());
        Assert.assertNotNull(createNonExistingIndexResult.getStatistics().getStringValue(Label.INDICES_ADDED));
        Assert.assertEquals(1, createNonExistingIndexResult.getStatistics().indicesAdded());

        ResultSet createExistingIndexResult = api.query("social", "CREATE INDEX ON :person(age)");
        Assert.assertFalse(createExistingIndexResult.hasNext());
        Assert.assertNotNull(createExistingIndexResult.getStatistics().getStringValue(Label.INDICES_ADDED));
        Assert.assertEquals(0, createExistingIndexResult.getStatistics().indicesAdded());

        ResultSet deleteExistingIndexResult = api.query("social", "DROP INDEX ON :person(age)");
        Assert.assertFalse(deleteExistingIndexResult.hasNext());
        Assert.assertNotNull(deleteExistingIndexResult.getStatistics().getStringValue(Label.INDICES_DELETED));
        Assert.assertEquals(1, deleteExistingIndexResult.getStatistics().indicesDeleted());

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

        Assert.assertNotNull(schemaNames);

        Assert.assertEquals(3, schemaNames.size());

        Assert.assertEquals("a", schemaNames.get(0));
        Assert.assertEquals("r", schemaNames.get(1));
        Assert.assertEquals("a.age", schemaNames.get(2));


    }

    @Test
    public void testRecord(){
        String name = "roi";
        int age = 32;
        double doubleValue = 3.14;
        boolean boolValue  = true;

        String place = "TLV";
        int since = 2000;



        Property nameProperty = new Property("name", name);
        Property ageProperty = new Property("age", age);
        Property doubleProperty = new Property("doubleValue", doubleValue);
        Property trueBooleanProperty = new Property("boolValue", true);
        Property falseBooleanProperty = new Property("boolValue", false);
        Property nullProperty = new Property("nullValue", null);

        Property placeProperty = new Property("place", place);
        Property sinceProperty = new Property("since", since);

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
            + "propertyMap={name=Property{name='name', value=roi}, "
            + "boolValue=Property{name='boolValue', value=true}, "
            + "doubleValue=Property{name='doubleValue', value=3.14}, "
            + "nullValue=Property{name='nullValue', value=null}, "
            + "age=Property{name='age', value=32}}}", expectedNode.toString());

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
            + "propertyMap={boolValue=Property{name='boolValue', value=false}, "
            + "place=Property{name='place', value=TLV}, "
            + "doubleValue=Property{name='doubleValue', value=3.14}, "
            + "nullValue=Property{name='nullValue', value=null}, "
            + "since=Property{name='since', value=2000}}}", expectedEdge.toString());

        Map<String, Object> params = new HashMap<>();
        params.put("name", name);
        params.put("age", age);
        params.put("boolValue", boolValue);
        params.put("doubleValue", doubleValue);

        Assert.assertNotNull(api.query("social", "CREATE (:person{name:$name,age:$age, doubleValue:$doubleValue, boolValue:$boolValue, nullValue:null})", params));
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
                name, (long)age, doubleValue, true, null,
                place, (long)since, doubleValue, false, null),
                record.values());

        Node a = record.getValue("a");
        for (String propertyName : expectedNode.getEntityPropertyNames()){
            Assert.assertEquals(expectedNode.getProperty(propertyName) ,a.getProperty(propertyName));
        }

        Assert.assertEquals( "roi", record.getString(2));
        Assert.assertEquals( "32", record.getString(3));
        Assert.assertEquals( 32L, ((Long)record.getValue(3)).longValue());
        Assert.assertEquals( 32L, ((Long)record.getValue("a.age")).longValue());
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

        Property nameProperty = new Property("name", "roi");
        Property ageProperty = new Property("age", 32);
        Property lastNameProperty =new Property("lastName", "a");

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
            Assert.assertNotNull(schemaNames);
            Assert.assertEquals(3, schemaNames.size());
            Assert.assertEquals("a", schemaNames.get(0));
            Assert.assertEquals("r", schemaNames.get(1));
            Assert.assertEquals("a.age", schemaNames.get(2));
            Assert.assertEquals(1, resultSet.size());
            Assert.assertTrue(resultSet.hasNext());
            Record record = resultSet.next();
            Assert.assertFalse(resultSet.hasNext());
            Assert.assertEquals(Arrays.asList("a", "r", "a.age"), record.keys());
            Assert.assertEquals(Arrays.asList(expectedNode, expectedEdge, 32L), record.values());
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
            Assert.assertNotNull(schemaNames);
            Assert.assertEquals(2, schemaNames.size());
            Assert.assertEquals("a", schemaNames.get(0));
            Assert.assertEquals("r", schemaNames.get(1));
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
        Property nameProperty = new Property("name", "roi");
        Property ageProperty = new Property("age", 32);
        Property lastNameProperty =new Property("lastName", "a");

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
        Assert.assertNotNull(schemaNames);
        Assert.assertEquals(2, schemaNames.size());
        Assert.assertEquals("a", schemaNames.get(0));
        Assert.assertEquals("r", schemaNames.get(1));
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
        Assert.assertNotNull(schemaNames);
        Assert.assertEquals(2, schemaNames.size());
        Assert.assertEquals("a", schemaNames.get(0));
        Assert.assertEquals("r", schemaNames.get(1));
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
            Assert.assertEquals(2L, results.get(3));

            // Redis get command
            Assert.assertEquals(String.class, results.get(4).getClass());
            Assert.assertEquals("2", results.get(4));

            // Graph query result
            Assert.assertEquals(ResultSetImpl.class, results.get(5).getClass());
            resultSet = (ResultSet) results.get(5);

            Assert.assertNotNull(resultSet.getHeader());
            Header header = resultSet.getHeader();


            List<String> schemaNames = header.getSchemaNames();
            Assert.assertNotNull(schemaNames);
            Assert.assertEquals(1, schemaNames.size());
            Assert.assertEquals("n", schemaNames.get(0));

            Property nameProperty = new Property("name", "a");

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

            Assert.assertEquals(ResultSetImpl.class, results.get(7).getClass());
            resultSet = (ResultSet) results.get(7);

            Assert.assertNotNull(resultSet.getHeader());
            header = resultSet.getHeader();


            schemaNames = header.getSchemaNames();
            Assert.assertNotNull(schemaNames);
            Assert.assertEquals(1, schemaNames.size());
            Assert.assertEquals("label", schemaNames.get(0));

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


        Property nameProperty = new Property("name", name);
        Property ageProperty = new Property("age", age);
        Property doubleProperty = new Property("doubleValue", doubleValue);
        Property trueBooleanProperty = new Property("boolValue", true);
        Property falseBooleanProperty = new Property("boolValue", false);
        Property nullProperty = new Property("nullValue", null);

        Property placeProperty = new Property("place", place);
        Property sinceProperty = new Property("since", since);

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

        Map<String, Object> params = new HashMap<>();
        params.put("name", name);
        params.put("age", age);
        params.put("boolValue", boolValue);
        params.put("doubleValue", doubleValue);
        try (RedisGraphContext c = api.getContext()) {
            Assert.assertNotNull(c.query("social", "CREATE (:person{name:$name, age:$age, doubleValue:$doubleValue, boolValue:$boolValue, nullValue:null})", params));
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
                    name, (long)age, doubleValue, true, null,
                    place, (long)since, doubleValue, false, null),
                    record.values());

            Node a = record.getValue("a");
            for (String propertyName : expectedNode.getEntityPropertyNames()) {
                Assert.assertEquals(expectedNode.getProperty(propertyName), a.getProperty(propertyName));
            }

            Assert.assertEquals("roi", record.getString(2));
            Assert.assertEquals("32", record.getString(3));
            Assert.assertEquals(32L, ((Long) (record.getValue(3))).longValue());
            Assert.assertEquals(32L, ((Long) record.getValue("a.age")).longValue());
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

        Map<String, Object> params = new HashMap<>();
        params.put("name", 'b');
        t1.query("social", "CREATE (:Person {name:$name})", params);
        c2.query("social", "MATCH (n) return n");
        List<Object> returnValue = t1.exec();

        Assert.assertNotNull(returnValue);
        c1.close();
        c2.close();
    }

    @Test
    public void testArraySupport() {

        Node expectedANode = new Node();
        expectedANode.setId(0);
        expectedANode.addLabel("person");
        Property aNameProperty = new Property("name", "a");
        Property aAgeProperty = new Property("age", 32);
        Property aListProperty = new Property("array", Arrays.asList(0L, 1L, 2L));
        expectedANode.addProperty(aNameProperty);
        expectedANode.addProperty(aAgeProperty);
        expectedANode.addProperty(aListProperty);


        Node expectedBNode = new Node();
        expectedBNode.setId(1);
        expectedBNode.addLabel("person");
        Property bNameProperty = new Property("name", "b");
        Property bAgeProperty = new Property("age", 30);
        Property bListProperty = new Property("array", Arrays.asList(3L, 4L, 5L));
        expectedBNode.addProperty(bNameProperty);
        expectedBNode.addProperty(bAgeProperty);
        expectedBNode.addProperty(bListProperty);



        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'a',age:32,array:[0,1,2]})"));
        Assert.assertNotNull(api.query("social", "CREATE (:person{name:'b',age:30,array:[3,4,5]})"));


        // test array

        ResultSet resultSet = api.query("social", "WITH [0,1,2] as x return x");

        // check header
        Assert.assertNotNull(resultSet.getHeader());
        Header header = resultSet.getHeader();


        List<String> schemaNames = header.getSchemaNames();
        Assert.assertNotNull(schemaNames);
        Assert.assertEquals(1, schemaNames.size());
        Assert.assertEquals("x", schemaNames.get(0));

        // check record
        Assert.assertEquals(1, resultSet.size());
        Assert.assertTrue(resultSet.hasNext());
        Record record = resultSet.next();
        Assert.assertFalse(resultSet.hasNext());
        Assert.assertEquals(Arrays.asList("x"), record.keys());


        List x = record.getValue("x");
        Assert.assertEquals(Arrays.asList(0L, 1L, 2L), x);

        // test collect
        resultSet = api.query("social", "MATCH(n) return collect(n) as x");

        Assert.assertNotNull(resultSet.getHeader());
        header = resultSet.getHeader();


        schemaNames = header.getSchemaNames();
        Assert.assertNotNull(schemaNames);
        Assert.assertEquals(1, schemaNames.size());
        Assert.assertEquals("x", schemaNames.get(0));

        // check record
        Assert.assertEquals(1, resultSet.size());
        Assert.assertTrue(resultSet.hasNext());
        record = resultSet.next();
        Assert.assertFalse(resultSet.hasNext());
        Assert.assertEquals(Arrays.asList("x"), record.keys());
        x = record.getValue("x");
        Assert.assertEquals(Arrays.asList(expectedANode, expectedBNode), x);


        // test unwind
        resultSet = api.query("social", "unwind([0,1,2]) as x return x");

        Assert.assertNotNull(resultSet.getHeader());
        header = resultSet.getHeader();


        schemaNames = header.getSchemaNames();
        Assert.assertNotNull(schemaNames);
        Assert.assertEquals(1, schemaNames.size());
        Assert.assertEquals("x", schemaNames.get(0));

        // check record
        Assert.assertEquals(3, resultSet.size());

        for (long i = 0; i < 3; i++) {
            Assert.assertTrue(resultSet.hasNext());
            record = resultSet.next();
            Assert.assertEquals(Arrays.asList("x"), record.keys());
            Assert.assertEquals(i, (long)record.getValue("x"));

        }

    }

    @Test
    public void testPath(){
        List<Node> nodes =  new ArrayList<>(3);
        for(int i =0; i < 3; i++){
            Node node = new Node();
            node.setId(i);
            node.addLabel("L1");
            nodes.add(node);
        }

        List<Edge> edges = new ArrayList<>(2);
        for(int i =0; i <2; i++){
            Edge edge = new Edge();
            edge.setId(i);
            edge.setRelationshipType("R1");
            edge.setSource(i);
            edge.setDestination(i + 1);
            edges.add(edge);
        }

        Set<Path> expectedPaths = new HashSet<>();

        Path path01 = new PathBuilder(2).append(nodes.get(0)).append(edges.get(0)).append(nodes.get(1)).build();
        Path path12 = new PathBuilder(2).append(nodes.get(1)).append(edges.get(1)).append(nodes.get(2)).build();
        Path path02 = new PathBuilder(3).append(nodes.get(0)).append(edges.get(0)).append(nodes.get(1)).append(edges.get(1)).append(nodes.get(2)).build();

        expectedPaths.add(path01);
        expectedPaths.add(path12);
        expectedPaths.add(path02);

        api.query("social", "CREATE (:L1)-[:R1]->(:L1)-[:R1]->(:L1)");

        ResultSet resultSet = api.query("social", "MATCH p = (:L1)-[:R1*]->(:L1) RETURN p");

        Assert.assertEquals(expectedPaths.size(), resultSet.size());
        for(int i =0; i < resultSet.size(); i++){
            Path p = resultSet.next().getValue("p");
            Assert.assertTrue(expectedPaths.contains(p));
            expectedPaths.remove(p);
        }

    }

    @Test
    public void testParameters(){
        Object[] parameters = {1, 2.3, true, false, null, "str", Arrays.asList(1,2,3), new Integer[]{1,2,3}};
        Object[] expected_anwsers = {1L, 2.3, true, false, null, "str", Arrays.asList(1L, 2L, 3L), new Long[]{1L, 2L, 3L}};
        Map<String, Object> params = new HashMap<>();
        for (int i=0; i < parameters.length; i++) {
            Object param = parameters[i];
            params.put("param", param);
            ResultSet resultSet = api.query("social", "RETURN $param", params);
            Assert.assertEquals(1, resultSet.size());
            Record r = resultSet.next();
            Object o = r.getValue(0);
            Object expected = expected_anwsers[i];
            if(i == parameters.length-1) {
                expected = Arrays.asList((Object[])expected);
            }
            Assert.assertEquals(expected, o);
        }
    }

    @Test
    public void testNullGraphEntities() {
        // Create two nodes connected by a single outgoing edge.
        Assert.assertNotNull(api.query("social", "CREATE (:L)-[:E]->(:L2)"));
        // Test a query that produces 1 record with 3 null values.
        ResultSet resultSet = api.query("social", "OPTIONAL MATCH (a:NONEXISTENT)-[e]->(b) RETURN a, e, b");
        Assert.assertEquals(1, resultSet.size());
        Assert.assertTrue(resultSet.hasNext());
        Record record = resultSet.next();
        Assert.assertFalse(resultSet.hasNext());
        Assert.assertEquals(Arrays.asList(null, null, null), record.values());

        // Test a query that produces 2 records, with 2 null values in the second.
        resultSet = api.query("social", "MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY ID(a)");
        Assert.assertEquals(2, resultSet.size());
        record = resultSet.next();
        Assert.assertEquals(3, record.size());

        Assert.assertNotNull(record.getValue(0));
        Assert.assertNotNull(record.getValue(1));
        Assert.assertNotNull(record.getValue(2));

        record = resultSet.next();
        Assert.assertEquals(3, record.size());

        Assert.assertNotNull(record.getValue(0));
        Assert.assertNull(record.getValue(1));
        Assert.assertNull(record.getValue(2));

        // Test a query that produces 2 records, the first containing a path and the second containing a null value.
        resultSet = api.query("social", "MATCH (a) OPTIONAL MATCH p = (a)-[e]->(b) RETURN p");
        Assert.assertEquals(2, resultSet.size());
        record = resultSet.next();
        Assert.assertEquals(1, record.size());

        Object path = record.getValue(0);
        Assert.assertNotNull(record.getValue(0));

        record = resultSet.next();
        Assert.assertEquals(1, record.size());

        path = record.getValue(0);
        Assert.assertNull(record.getValue(0));

    }

    @Test
    public void test64bitnumber(){
        long value = 1 << 40;
        Map<String, Object> params = new HashMap<>();
        params.put("val", value);
        ResultSet resultSet = api.query("social","CREATE (n {val:$val}) RETURN n.val", params);
        Assert.assertEquals(1, resultSet.size());
        Record r = resultSet.next();
        Assert.assertEquals(Long.valueOf(value), r.getValue(0));
    }

    @Test
    public void testCachedExecution() {
        api.query("social", "CREATE (:N {val:1}), (:N {val:2})");
        
        // First time should not be loaded from execution cache         
        Map<String, Object> params = new HashMap<>();
        params.put("val", 1L);
        ResultSet resultSet = api.query("social","MATCH (n:N {val:$val}) RETURN n.val", params);
        Assert.assertEquals(1, resultSet.size());
        Record r = resultSet.next();
        Assert.assertEquals(params.get("val"), r.getValue(0));
        Assert.assertFalse(resultSet.getStatistics().cachedExecution());
        
        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0 ; i < 64; i++){
            resultSet = api.query("social","MATCH (n:N {val:$val}) RETURN n.val", params);
        }
        Assert.assertEquals(1, resultSet.size());
        r = resultSet.next();
        Assert.assertEquals(params.get("val"), r.getValue(0));
        Assert.assertTrue(resultSet.getStatistics().cachedExecution());
    }
}
