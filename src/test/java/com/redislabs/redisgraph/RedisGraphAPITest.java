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
    public void testConnectNodes() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode src = api.createNode("name", "roi", "age", 32);
        RedisNode dest = api.createNode("name", "amit", "age", 30);

        // Connect source and destination nodes.
        RedisEdge edge = api.connectNodes(src, "knows", dest, "strength", "3", "from", "high-school");

        // Validate edge
        Assert.assertNotNull(edge.getId());
        Assert.assertEquals(edge.getRelation(), "knows");
        Assert.assertSame(edge.getAttributes().size(), 2);
        Assert.assertSame(edge.getAttributes().get("strength"), "3");
        Assert.assertSame(edge.getAttributes().get("from"), "high-school");
        Assert.assertSame(edge.getSrc(), src);
        Assert.assertSame(edge.getDest(), dest);
    }

    @org.testng.annotations.Test
    public void testQuery() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode src = api.createLabeledNode("Qhuman","name", "roi", "age", 32);
        RedisNode dest = api.createLabeledNode("Qhuman","name", "amit", "age", 30);

        // Connect source and destination nodes.
        api.connectNodes(src, "knows", dest, "strength", "3", "from", "high-school");

        // Query
        ResultSet resultSet = api.query("MATCH (a:Qhuman)-[]->(:Qhuman) RETURN a");

        // Expecting a single result
        Assert.assertEquals(resultSet.totalResults, 1);
        Assert.assertEquals(resultSet.results.size(), resultSet.totalResults);
        Assert.assertEquals(resultSet.results.get(0)[0], "roi");
        Assert.assertEquals(resultSet.results.get(0)[1], "32");
    }

    @org.testng.annotations.Test
    public void testGetNode() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create node
        RedisNode node = api.createLabeledNode("human","name", "shany", "age", 23);

        // Get node
        RedisNode retrievedNode = api.getNode(node.id);

        // Expecting a single result
        Assert.assertNotNull(retrievedNode);
        Assert.assertEquals(node.id, retrievedNode.id);
        Assert.assertEquals(node.label, retrievedNode.label);
        Assert.assertEquals(retrievedNode.attributes.size(), 2);
        Assert.assertEquals(retrievedNode.attributes.get("name"), "shany");
        Assert.assertEquals(Integer.parseInt(retrievedNode.attributes.get("age")), 23);
    }

    @org.testng.annotations.Test
    public void testGetEdge() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode src = api.createLabeledNode("human","name", "roi", "age", 32);
        RedisNode dest = api.createLabeledNode("human","name", "amit", "age", 30);

        // Connect source and destination nodes.
        RedisEdge edge = api.connectNodes(src, "knows", dest, "strength", "3", "from", "high-school");

        // Get edge
        RedisEdge retrievedEdge = api.getEdge(edge.getId());

        // Expecting a single result
        Assert.assertNotNull(retrievedEdge);
        Assert.assertEquals(edge.getId(), retrievedEdge.getId());
        Assert.assertEquals(edge.getRelation(), retrievedEdge.getRelation());
        Assert.assertEquals(retrievedEdge.getAttributes().size(), 2);
        Assert.assertEquals(retrievedEdge.getAttributes().get("strength"), "3");
        Assert.assertEquals(retrievedEdge.getAttributes().get("from"), "high-school");

        Assert.assertEquals(retrievedEdge.getSrc().id, src.id);
        Assert.assertEquals(retrievedEdge.getDest().id, dest.id);

    }
}