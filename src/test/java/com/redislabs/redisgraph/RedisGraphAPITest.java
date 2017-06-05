package com.redislabs.redisgraph;
import org.junit.Assert;

import java.util.List;

public class RedisGraphAPITest {
    @org.testng.annotations.Test
    public void testCreateNode() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create a node
        RedisNode node = api.createNode("name", "roi", "age", 32);

        // Validate node
        Assert.assertNotNull(node.getId());
        Assert.assertSame(node.getAttributes().get("name"), "roi");
        Assert.assertSame(Integer.parseInt(node.getAttributes().get("age")), 32);
        Assert.assertSame(node.getAttributes().size(),2);
    }

    @org.testng.annotations.Test
    public void testCreateLabeledNode() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create a node with a label
        RedisNode node = api.createLabeledNode("human","name", "roi", "age", 32);

        // Validate node
        Assert.assertNotNull(node.getId());
        Assert.assertSame(node.getLabel(), "human");
        Assert.assertSame(node.getAttributes().get("name"), "roi");
        Assert.assertSame(Integer.parseInt(node.getAttributes().get("age")), 32);
        Assert.assertSame(node.getAttributes().size(),2);
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
        RedisNode retrievedNode = api.getNode(node.getId());

        // Expecting a single result
        Assert.assertNotNull(retrievedNode);
        Assert.assertEquals(node.getId(), retrievedNode.getId());
        Assert.assertEquals(node.getLabel(), retrievedNode.getLabel());
        Assert.assertEquals(retrievedNode.getAttributes().size(), 2);
        Assert.assertEquals(retrievedNode.getAttributes().get("name"), "shany");
        Assert.assertEquals(Integer.parseInt(retrievedNode.getAttributes().get("age")), 23);
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

        Assert.assertEquals(retrievedEdge.getSrc().getId(), src.getId());
        Assert.assertEquals(retrievedEdge.getDest().getId(), dest.getId());
    }

    @org.testng.annotations.Test
    public void testSetProperty() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create a node
        RedisNode node = api.createNode("name", "shimshon", "age", 60);
        api.setProperty(node.getId(), "age", 61);

        node = api.getNode(node.getId());

        // Validate node
        Assert.assertSame(Integer.parseInt(node.getAttributes().get("age")), 61);
    }

    @org.testng.annotations.Test
    public void testGetNodeEdges() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode roi = api.createNode("name", "roi", "age", 32);
        RedisNode amit = api.createNode("name", "amit", "age", 30);
        RedisNode shany = api.createNode("name", "shany", "age", 23);

        // Connect source and destination nodes.
        api.connectNodes(roi, "knows", amit);
        api.connectNodes(roi, "knows", shany);
        api.connectNodes(amit, "knows", roi);
        api.connectNodes(shany, "knows", roi);

        int DIR_OUT = 0;
        int DIR_IN = 1;
        int DIR_BOTH = 2;

        List<RedisEdge> edges;
        edges = api.getNodeEdges(roi.getId(), "knows", DIR_OUT);
        Assert.assertEquals(edges.size(), 2);

        edges = api.getNodeEdges(roi.getId(), "knows", DIR_IN);
        Assert.assertEquals(edges.size(), 2);

        edges = api.getNodeEdges(roi.getId(), "knows", DIR_BOTH);
        Assert.assertEquals(edges.size(), 4);

        edges = api.getNodeEdges(amit.getId(), "knows", DIR_OUT);
        Assert.assertEquals(edges.size(), 1);

        edges = api.getNodeEdges(amit.getId(), "knows", DIR_IN);
        Assert.assertEquals(edges.size(), 1);

        edges = api.getNodeEdges(amit.getId(), "knows", DIR_BOTH);
        Assert.assertEquals(edges.size(), 2);
    }

    @org.testng.annotations.Test
    public void testGetNeighbours() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create both source and destination nodes
        RedisNode roi = api.createNode("name", "roi", "age", 32);
        RedisNode amit = api.createNode("name", "amit", "age", 30);
        RedisNode shany = api.createNode("name", "shany", "age", 23);

        // Connect source and destination nodes.
        api.connectNodes(roi, "knows", amit);
        api.connectNodes(roi, "knows", shany);
        api.connectNodes(amit, "knows", roi);
        api.connectNodes(shany, "knows", roi);

        int DIR_OUT = 0;
        int DIR_IN = 1;
        int DIR_BOTH = 2;

        List<RedisNode> neighbours;
        neighbours = api.getNeighbours(roi.getId(), "knows", DIR_OUT);
        Assert.assertEquals(neighbours.size(), 2);

        neighbours = api.getNeighbours(roi.getId(), "knows", DIR_IN);
        Assert.assertEquals(neighbours.size(), 2);

        neighbours = api.getNeighbours(roi.getId(), "knows", DIR_BOTH);
        Assert.assertEquals(neighbours.size(), 4);

        neighbours = api.getNeighbours(amit.getId(), "knows", DIR_OUT);
        Assert.assertEquals(neighbours.size(), 1);

        neighbours = api.getNeighbours(amit.getId(), "knows", DIR_IN);
        Assert.assertEquals(neighbours.size(), 1);

        neighbours = api.getNeighbours(amit.getId(), "knows", DIR_BOTH);
        Assert.assertEquals(neighbours.size(), 2);
    }

    @org.testng.annotations.Test
    public void testGraphDelete() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");
        RedisNode roi = api.createNode("name", "roi", "age", 32);
        RedisNode amit = api.createNode("name", "amit", "age", 30);

        // Connect source and destination nodes.
        RedisEdge edge = api.connectNodes(roi, "knows", amit);

        api.deleteGraph();

        roi = api.getNode(roi.getId());
        Assert.assertNull(roi);

        edge = api.getEdge(edge.getId());
        Assert.assertNull(edge);
    }
}