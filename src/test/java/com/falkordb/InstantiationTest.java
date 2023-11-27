package com.falkordb;

import org.junit.After;
import org.junit.Assert;

import com.falkordb.impl.api.Graph;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class InstantiationTest {
    private GraphContextGenerator client;

    public void createDefaultClient() {
        client = new Graph();
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }
    
    public void createClientWithHostAndPort() {
        client = new Graph("localhost", 6379);
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }
    
    public void createClientWithJedisInstance() {
        client = new Graph(new Jedis());
        ResultSet resultSet = client.query("g", "CREATE ({name:'bsb'})");
        Assert.assertEquals(1, resultSet.getStatistics().nodesCreated());
    }
    
    public void createClientWithJedisPool() {
        client = new Graph(new JedisPool());
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
