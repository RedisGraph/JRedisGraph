package com.redislabs.redisgraph.exceptions;

import com.redislabs.redisgraph.RedisGraphContext;
import com.redislabs.redisgraph.RedisGraphContextGenerator;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;


public class JRedisGraphErrorTest {

    private RedisGraphContextGenerator api;

    @Before
    public void createApi(){
        api = new RedisGraph();
        Assert.assertNotNull(api.query("social", "CREATE (:person{mixed_prop: 'strval'}), (:person{mixed_prop: 50})"));
    }
    @After
    public void deleteGraph() {

        api.deleteGraph("social");
        api.close();
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testSyntaxErrorReporting() {
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Type mismatch: expected String but was Integer");

        // Issue a query that causes a compile-time error
        api.query("social", "RETURN toUpper(5)");

    }

    @Test
    public void testRuntimeErrorReporting() {
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Type mismatch: expected String but was Integer");

        // Issue a query that causes a run-time error
        api.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
    }

    @Test
    public void testExceptionFlow() {

        try {
            // Issue a query that causes a compile-time error
            api.query("social", "RETURN toUpper(5)");
        }
        catch (Exception e) {
            Assert.assertEquals(JRedisGraphException.class, e.getClass());
            Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
        }

        // On general api usage, user should get a new connection

        try {
            // Issue a query that causes a compile-time error
            api.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
        }
        catch (Exception e) {
            Assert.assertEquals(JRedisGraphException.class, e.getClass());
            Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
        }

    }


    @Test
    public void testContextSyntaxErrorReporting() {
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Type mismatch: expected String but was Integer");
        RedisGraphContext c = api.getContext();

        // Issue a query that causes a compile-time error
        c.query("social", "RETURN toUpper(5)");

    }

    @Test
    public void testMissingParametersSyntaxErrorReporting(){
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Missing parameters");
        api.query("social","RETURN $param");
    }

    @Test
    public void testMissingParametersSyntaxErrorReporting2(){
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Missing parameters");
        api.query("social","RETURN $param", new HashMap<>());
    }

    @Test
    public void testContextRuntimeErrorReporting() {
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Type mismatch: expected String but was Integer");

        RedisGraphContext c = api.getContext();
        // Issue a query that causes a run-time error
        c.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
    }


    @Test
    public void testContextExceptionFlow() {

        RedisGraphContext c = api.getContext();
        try {
            // Issue a query that causes a compile-time error
            c.query("social", "RETURN toUpper(5)");
        }
        catch (Exception e) {
            Assert.assertEquals(JRedisGraphException.class, e.getClass());
            Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
        }

        // On contexted api usage, connection should stay open

        try {
            // Issue a query that causes a compile-time error
            c.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
        }
        catch (Exception e) {
            Assert.assertEquals(JRedisGraphException.class, e.getClass());
            Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
        }

    }

    @Test
    public void timeoutExcpetion() {
        exceptionRule.expect(JRedisGraphException.class);
        exceptionRule.expectMessage("Query timed out");

        api.query("social", "UNWIND range(0,100000) AS x WITH x AS x WHERE x = 10000 RETURN x", 1L);
    }
}
