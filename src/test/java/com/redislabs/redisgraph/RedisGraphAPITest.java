package com.redislabs.redisgraph;
import org.junit.Assert;

public class RedisGraphAPITest {
    @org.testng.annotations.Test
    public void testCreateNode() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create a node
        RedisNode node = api.createNode("name", "roi", "age", 32);

        // Validate node
        Assert.assertNotNull(node.id);
        Assert.assertSame(node.attributes.get("name"), "roi");
        Assert.assertSame(Integer.parseInt(node.attributes.get("age")), 32);
        Assert.assertSame(node.attributes.size(),2);
    }

    @org.testng.annotations.Test
    public void testCreateLabeledNode() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create a node with a label
        RedisNode node = api.createLabeledNode("human","name", "roi", "age", 32);

        // Validate node
        Assert.assertNotNull(node.id);
        Assert.assertSame(node.label, "human");
        Assert.assertSame(node.attributes.get("name"), "roi");
        Assert.assertSame(Integer.parseInt(node.attributes.get("age")), 32);
        Assert.assertSame(node.attributes.size(),2);
    }

    @org.testng.annotations.Test
    public void testGetNode() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create a node with a label
        RedisNode node = api.createLabeledNode("human","name", "roi", "age", 32);

        // Retrieve node
        RedisNode clone = api.getNode(node.id);

        // Make sure retrieved node is the same as the one created.
        Assert.assertSame(clone.id, node.id);
//        TODO: store label as one of the node attributes.
//        Assert.assertSame(clone.label, node.label);
        Assert.assertEquals(clone.attributes.get("name"), node.attributes.get("name"));
        Assert.assertEquals(clone.attributes.get("age"), node.attributes.get("age"));
        Assert.assertEquals(clone.attributes.size(), node.attributes.size());

    }

    @org.testng.annotations.Test
    public void testConnectNodes() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode src = api.createNode("name", "roi", "age", 32);
        RedisNode dest = api.createNode("name", "amit", "age", 30);

        // Connect source and destination nodes.
        RedisEdge edge = api.connectNodes(src, "knows", dest, "strength", "3", "from", "high-school");

        // Validate edge
        Assert.assertNotNull(edge.id);
        Assert.assertEquals(edge.relation, "knows");
        Assert.assertSame(edge.attributes.size(), 2);
        Assert.assertSame(edge.attributes.get("strength"), "3");
        Assert.assertSame(edge.attributes.get("from"), "high-school");
        Assert.assertSame(edge.src, src);
        Assert.assertSame(edge.dest, dest);
    }

    @org.testng.annotations.Test
    public void testQuery() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode src = api.createLabeledNode("human","name", "roi", "age", 32);
        RedisNode dest = api.createLabeledNode("human","name", "amit", "age", 30);

        // Connect source and destination nodes.
        api.connectNodes(src, "knows", dest, "strength", "3", "from", "high-school");

        // Query
        ResultSet resultSet = api.query("MATCH (a:human)-[]->(:human) RETURN a");

        // Expecting a single result
        Assert.assertEquals(resultSet.totalResults, 1);
        Assert.assertEquals(resultSet.results.size(), resultSet.totalResults);
        Assert.assertEquals(resultSet.results.get(0)[0], "roi");
        Assert.assertEquals(resultSet.results.get(0)[1], "32");
    }
}