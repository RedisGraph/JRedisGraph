[![CircleCI](https://circleci.com/gh/RedisLabs/JRedisGraphh/tree/master.svg?style=svg)](https://circleci.com/gh/RedisLabs/JRedisGraph/tree/master)

# JRedisGraph
RedisGraph Java client


# Example: Using the Java Client

```java
package com.redislabs.redisgraph;

public class RedisGraphExample {
	public static void main(String[] args) {

		RedisGraphAPI api = new RedisGraphAPI("social");

		api.query("CREATE (:person{name:'roi',age:32})");
		api.query("CREATE (:person{name:'amit',age:30})");

		api.query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit') CREATE (a)-[knows]->(a)");

		ResultSet resultSet = api.query("MATCH (a:person)-[knows]->(:person) RETURN a");

		while(resultSet.hasNext()){
			Record record = resultSet.next();
			System.out.println(record.getString("a.name"));
		}
	}
}

```
