package com.redislabs.redisgraph.graph_entities;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class PathTest {

    private Node buildNode(int id){
        Node n = new Node();
        n.setId(0);
        return n;
    }

    private Edge buildEdge(int id, int src, int dst){
        Edge e = new Edge();
        e.setId(id);
        e.setSource(src);
        e.setDestination(dst);
        return e;
    }

    private List<Node> buildNodeArray(int size) {
        List<Node> nodes = new ArrayList<>();
        return IntStream.range(0, size).mapToObj(i -> buildNode(i)).collect(Collectors.toList());
    }

    private List<Edge> buildEdgeArray(int size){
        List<Node> nodes = new ArrayList<>();
        return IntStream.range(0, size).mapToObj(i -> buildEdge(i, i, i+1)).collect(Collectors.toList());
    }

    private Path buildPath(int nodeCount){
        return new Path(buildNodeArray(nodeCount), buildEdgeArray(nodeCount-1));
    }

    @Test
    public void testEmptyPath(){
        Path path = buildPath(0);
        assertEquals(0, path.length());
        assertEquals(0, path.nodeCount());
        assertThrows(IndexOutOfBoundsException.class, ()->path.getNode(0));
        assertThrows(IndexOutOfBoundsException.class, ()->path.getEdge(0));
    }

    @Test
    public void testSingleNodePath(){
        Path path = buildPath(1);
        assertEquals(0, path.length());
        assertEquals(1, path.nodeCount());
        Node n = new Node();
        n.setId(0);
        assertEquals(n, path.firstNode());
        assertEquals(n, path.lastNode());
        assertEquals(n, path.getNode(0));
    }

    @Test
    public void testRandomLengthPath(){
        int nodeCount = ThreadLocalRandom.current().nextInt(2, 100 + 1);
        Path path = buildPath(nodeCount);
        assertEquals(buildNodeArray(nodeCount), path.getNodes());
        assertEquals(buildEdgeArray(nodeCount-1), path.getEdges());
        assertDoesNotThrow(()->path.getEdge(0));
    }

    @Test
    public void hashCodeEqualTest(){
        EqualsVerifier.forClass(Path.class).verify();
    }
}