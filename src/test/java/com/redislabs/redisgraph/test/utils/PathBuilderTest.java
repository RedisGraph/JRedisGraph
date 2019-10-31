package com.redislabs.redisgraph.test.utils;

import com.redislabs.redisgraph.graph_entities.Edge;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PathBuilderTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testPathBuilderSizeException(){
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Path builder nodes count should be edge count + 1");
        PathBuilder builder = new PathBuilder(0);
        builder.build();
    }

    @Test
    public void testPathBuilderArgumentsException(){
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Path Builder expected Node but was Edge");
        PathBuilder builder = new PathBuilder(0);
        builder.append(new Edge());
    }
}
