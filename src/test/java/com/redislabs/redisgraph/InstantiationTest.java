package com.redislabs.redisgraph;

import org.junit.After;
import org.junit.Assert;

import com.redislabs.redisgraph.impl.api.RedisGraph;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class InstantiationTest {
    private RedisGraphContextGenerator client;

    public void createDefaultClient() {
        client = new RedisGraph();
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }
    
    public void createClientWithHostAndPort() {
        client = new RedisGraph("localhost", 6379);
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }
    
    public void createClientWithJedisInstance() {
        client = new RedisGraph(new Jedis());
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }
    
    public void createClientWithJedisPool() {
        client = new RedisGraph(new JedisPool());
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }

    @After
    public void closeClient() {
        if (client != null) {
        	client.deleteGraph("g");
        	client.close();
        }
    }
}
