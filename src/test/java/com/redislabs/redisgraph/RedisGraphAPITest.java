package com.redislabs.redisgraph;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RedisGraphAPITest {
    RedisGraphAPI api;

    public RedisGraphAPITest() {
        api = new RedisGraphAPI("social");
    }

    @org.testng.annotations.BeforeSuite
    @org.testng.annotations.BeforeMethod
    public void flushDB() {
        api.deleteGraph();
    }

    @org.testng.annotations.Test
    public void testCreateNode() throws Exception {
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
        // Create both source and destination nodes
        RedisNode src = api.createLabeledNode("qhuman","name", "roi", "age", 32);
        RedisNode dest = api.createLabeledNode("qhuman","name", "amit", "age", 30);

        // Connect source and destination nodes.
        api.connectNodes(src, "knows", dest, "strength", "3", "from", "high-school");

        // Query
        ResultSet resultSet = api.query("MATCH (a:qhuman)-[]->(:qhuman) RETURN a");

        // Expecting a single result
        Assert.assertEquals(resultSet.totalResults, 1);
        Assert.assertEquals(resultSet.results.size(), resultSet.totalResults);
        Assert.assertEquals(resultSet.results.get(0)[0], "roi");
        Assert.assertEquals(resultSet.results.get(0)[1], "32");
    }

    @org.testng.annotations.Test
    public void testGetNodes() throws Exception {
        RedisGraphAPI api = new RedisGraphAPI("social");

        // Create nodes
        HashMap<String, RedisNode> expected = new HashMap<String, RedisNode>(3);
        expected.put("roi", api.createLabeledNode("human", "name", "roi", "age", 32));
        expected.put("amit", api.createLabeledNode("human", "name", "amit", "age", 30));
        expected.put("shany", api.createLabeledNode("human", "name", "shany", "age", 23));

        // Get all three nodes
        List<RedisNode> retrievedNodes = api.getNodes();

        // Expecting a single result
        Assert.assertNotNull(retrievedNodes);
        Assert.assertEquals(retrievedNodes.size(), expected.size());

        for(RedisNode actualNode: retrievedNodes) {
            RedisNode expectedNode = expected.get(actualNode.getAttributes().get("name"));
            Assert.assertNotNull(expectedNode);

            Assert.assertEquals(actualNode.getId(), expectedNode.getId());
            Assert.assertEquals(actualNode.getLabel(), expectedNode.getLabel());
            Assert.assertEquals(actualNode.getAttributes().size(), 2);
            Assert.assertEquals(Integer.parseInt(expectedNode.getAttributes().get("age")), Integer.parseInt(actualNode.getAttributes().get("age")));
        }

        // List of node ids created.
        List<String> ids = new ArrayList<String>(expected.size());
        for (RedisNode n: expected.values()) {
            ids.add(n.getId());
        }

        retrievedNodes = api.getNodes(ids.toArray());
        for(RedisNode actualNode: retrievedNodes) {
            RedisNode expectedNode = expected.get(actualNode.getAttributes().get("name"));
            Assert.assertNotNull(expectedNode);

            Assert.assertEquals(actualNode.getId(), expectedNode.getId());
            Assert.assertEquals(actualNode.getLabel(), expectedNode.getLabel());
            Assert.assertEquals(actualNode.getAttributes().size(), 2);
            Assert.assertEquals(Integer.parseInt(expectedNode.getAttributes().get("age")), Integer.parseInt(actualNode.getAttributes().get("age")));
        }
    }

    @org.testng.annotations.Test
    public void testGetEdge() throws Exception {
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

    @org.testng.annotations.Test
    public void testGetNodeEdges() throws Exception {
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
}