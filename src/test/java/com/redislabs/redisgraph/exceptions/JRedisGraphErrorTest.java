package com.redislabs.redisgraph.exceptions;

import com.redislabs.redisgraph.RedisGraphContext;
import com.redislabs.redisgraph.RedisGraphContextGenerator;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

  @Test
  public void testExceptions() {
    try { 
      throw new JRedisGraphCompileTimeException("Test message 1");
    }catch (JRedisGraphCompileTimeException e) {
      Assert.assertEquals( "Test message 1", e.getMessage());
    }

    Exception cause = new IndexOutOfBoundsException("Index Error");
    try { 
      throw new JRedisGraphCompileTimeException("Test message 2", cause);
    }catch (JRedisGraphCompileTimeException e) {
      Assert.assertEquals( "Test message 2", e.getMessage());
      Assert.assertEquals( cause, e.getCause());
    }

    try {
      throw new JRedisGraphCompileTimeException(cause);
    }catch (JRedisGraphCompileTimeException e) {
      Assert.assertEquals( "java.lang.IndexOutOfBoundsException: Index Error", e.getMessage());
      Assert.assertEquals( cause, e.getCause());
    }
    
    try { 
      throw new JRedisGraphRunTimeException("Test message 3");
    }catch (JRedisGraphRunTimeException e) {
      Assert.assertEquals( "Test message 3", e.getMessage());
    }

    try { 
      throw new JRedisGraphRunTimeException("Test message 4", cause);
    }catch (JRedisGraphRunTimeException e) {
      Assert.assertEquals( "Test message 4", e.getMessage());
      Assert.assertEquals( cause, e.getCause());
    }

    try {
      throw new JRedisGraphRunTimeException(cause);
    }catch (JRedisGraphRunTimeException e) {
      Assert.assertEquals( "java.lang.IndexOutOfBoundsException: Index Error", e.getMessage());
      Assert.assertEquals( cause, e.getCause());
    }
  }

  @Test
  public void testSyntaxErrorReporting() {
    try {
      // Issue a query that causes a compile-time error
      api.query("social", "RETURN toUpper(5)");
    } catch (Exception e) {
      Assert.assertEquals(JRedisGraphCompileTimeException.class, e.getClass());
      Assert.assertEquals( "redis.clients.jedis.exceptions.JedisDataException: Type mismatch: expected String but was Integer", e.getMessage());
    }

  }

  @Test
  public void testRuntimeErrorReporting() {
    try {
      // Issue a query that causes a run-time error
      api.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
    } catch (Exception e) {
      Assert.assertEquals(JRedisGraphRunTimeException.class, e.getClass());
      Assert.assertEquals( "redis.clients.jedis.exceptions.JedisDataException: Type mismatch: expected String but was Integer", e.getMessage());
    }
  }


  @Test
  public void testExceptionFlow() {

    try {
      // Issue a query that causes a compile-time error
      api.query("social", "RETURN toUpper(5)");
    }
    catch (Exception e) {
      Assert.assertEquals(JRedisGraphCompileTimeException.class, e.getClass());
      Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
    }

    // On general api usage, user should get a new connection

    try {
      // Issue a query that causes a compile-time error
      api.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
    }
    catch (Exception e) {
      Assert.assertEquals(JRedisGraphRunTimeException.class, e.getClass());
      Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
    }

  }


  @Test
  public void testContextSyntaxErrorReporting() {
    RedisGraphContext c = api.getContext();
    try {
      // Issue a query that causes a compile-time error
      c.query("social", "RETURN toUpper(5)");
    } catch (Exception e) {
      Assert.assertEquals(JRedisGraphCompileTimeException.class, e.getClass());
      Assert.assertEquals( "redis.clients.jedis.exceptions.JedisDataException: Type mismatch: expected String but was Integer", e.getMessage());
    }
  }

  @Test
  public void testMissingParametersSyntaxErrorReporting(){
    try {
      api.query("social","RETURN $param");
    } catch (Exception e) {
      Assert.assertEquals(JRedisGraphRunTimeException.class, e.getClass());
      Assert.assertEquals( "redis.clients.jedis.exceptions.JedisDataException: Missing parameters", e.getMessage());
    }
  }

  @Test
  public void testMissingParametersSyntaxErrorReporting2(){
    try {
      api.query("social","RETURN $param", new HashMap<>());
    } catch (Exception e) {
      Assert.assertEquals(JRedisGraphRunTimeException.class, e.getClass());
      Assert.assertEquals( "redis.clients.jedis.exceptions.JedisDataException: Missing parameters", e.getMessage());
    }
  }

  @Test
  public void testContextRuntimeErrorReporting() {

    RedisGraphContext c = api.getContext();
    try {
      // Issue a query that causes a run-time error
      c.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
    } catch (Exception e) {
      Assert.assertEquals(JRedisGraphRunTimeException.class, e.getClass());
      Assert.assertEquals( "redis.clients.jedis.exceptions.JedisDataException: Type mismatch: expected String but was Integer", e.getMessage());
    }
  }


  @Test
  public void testContextExceptionFlow() {

    RedisGraphContext c = api.getContext();
    try {
      // Issue a query that causes a compile-time error
      c.query("social", "RETURN toUpper(5)");
    }
    catch (Exception e) {
      Assert.assertEquals(JRedisGraphCompileTimeException.class, e.getClass());
      Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
    }

    // On contexted api usage, connection should stay open

    try {
      // Issue a query that causes a compile-time error
      c.query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
    }
    catch (Exception e) {
      Assert.assertEquals(JRedisGraphRunTimeException.class, e.getClass());
      Assert.assertTrue( e.getMessage().contains("Type mismatch: expected String but was Integer"));
    }

  }
}
