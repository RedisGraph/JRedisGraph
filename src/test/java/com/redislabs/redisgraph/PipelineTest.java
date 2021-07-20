package com.redislabs.redisgraph;

import com.redislabs.redisgraph.Statistics.Label;
import com.redislabs.redisgraph.graph_entities.Edge;
import com.redislabs.redisgraph.graph_entities.Node;
import com.redislabs.redisgraph.graph_entities.Property;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.redislabs.redisgraph.impl.resultset.ResultSetImpl;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.*;

public class PipelineTest {

    private RedisGraphContextGenerator api;

    public PipelineTest() {
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
    public void testPipelineExec(){
        try (RedisGraphContext c = api.getContext()) {
            RedisGraphPipeline pipeline = api.getContext().pipelined();

            pipeline.set("x", "1");
            pipeline.query("social", "CREATE (:Person {name:'a'})");
            pipeline.query("g", "CREATE (:Person {name:'a'})");
            pipeline.incr("x");
            pipeline.get("x");
            pipeline.query("social", "MATCH (n:Person) RETURN n");
            pipeline.deleteGraph("g");
            pipeline.callProcedure("social", "db.labels");
            List<Object> results = pipeline.syncAndReturnAll();

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

            Property<String> nameProperty = new Property<>("name", "a");

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
    public void testMultiExecWithReadOnlyQueries(){
        try (RedisGraphContext c = api.getContext()) {
            RedisGraphPipeline pipeline = api.getContext().pipelined();
                
            pipeline.set("x", "1");
            pipeline.query("social", "CREATE (:Person {name:'a'})");
            pipeline.query("g", "CREATE (:Person {name:'a'})");
            pipeline.readOnlyQuery("social", "MATCH (n:Person) RETURN n");
            pipeline.deleteGraph("g");
            pipeline.callProcedure("social", "db.labels");
            List<Object> results = pipeline.syncAndReturnAll();

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

            // Graph read-only query result
            Assert.assertEquals(ResultSetImpl.class, results.get(5).getClass());
            resultSet = (ResultSet) results.get(3);

            Assert.assertNotNull(resultSet.getHeader());
            Header header = resultSet.getHeader();

            List<String> schemaNames = header.getSchemaNames();
            Assert.assertNotNull(schemaNames);
            Assert.assertEquals(1, schemaNames.size());
            Assert.assertEquals("n", schemaNames.get(0));

            Property<String> nameProperty = new Property<>("name", "a");

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

            Assert.assertEquals(ResultSetImpl.class, results.get(5).getClass());
            resultSet = (ResultSet) results.get(5);

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
}
