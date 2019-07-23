[![license](https://img.shields.io/github/license/RedisGraph/JRedisGraph.svg)](https://github.com/RedisGraph/JRedisGraph)
[![GitHub issues](https://img.shields.io/github/release/RedisGraph/JRedisGraph.svg)](https://github.com/RedisGraph/JRedisGraph/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisGraph/JRedisGraph/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGraph/JRedisGraph/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisgraph/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisgraph)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jredisgraph.svg)](https://www.javadoc.io/doc/com.redislabs/jredisgraph)
[![Codecov](https://codecov.io/gh/RedisGraph/JRedisGraph/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGraph/JRedisGraph)

# JRedisGraph
RedisGraph Java client


### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisgraph</artifactId>
      <version>1.0.6</version>
    </dependency>
  </dependencies>

Or

  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisgraph</artifactId>
      <version>2.0.0-rc1</version>
    </dependency>
  </dependencies>
```

### Snapshots
To be used with RedisGraph 2.0 (not officially released)

```xml
  <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
  </repositories>
```

and

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisgraph</artifactId>
      <version>2.0.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

# Example: Using the Java Client
## Up to 2.0.0
```java
package com.redislabs.redisgraph;

public class RedisGraphExample {
	public static void main(String[] args) {

		RedisGraphAPI api = new RedisGraphAPI("social");

		api.query("CREATE (:person{name:'roi',age:32})");
		api.query("CREATE (:person{name:%s,age:%d})", "amit", 30);

		api.query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit') CREATE (a)-[:knows]->(b)");

		ResultSet resultSet = api.query("MATCH (a:person)-[:knows]->(b:person) RETURN a, b");

		while(resultSet.hasNext()){
			Record record = resultSet.next();
			System.out.println(record.getString("a.name"));
		}
	}
}

```
## From 2.0.0

```java
package com.redislabs.redisgraph;

import com.redislabs.redisgraph.graph_entities.Edge;
import com.redislabs.redisgraph.graph_entities.Node;
import com.redislabs.redisgraph.impl.api.RedisGraph;

import java.io.IOException;
import java.util.List;

public class RedisGraphExample {
    public static void main(String[] args) {
        // general context api. Not bound to graph key or connection
        RedisGraph graph = new RedisGraph();

        // send queries to a specific graph called "social"
        graph.query("social","CREATE (:person{name:'roi',age:32})");
        graph.query("social","CREATE (:person{name:%s,age:%d})", "amit", 30);
        graph.query("social","MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit') CREATE (a)-[:knows]->(b)");

        ResultSet resultSet = graph.query("social", "MATCH (a:person)-[r:knows]->(b:person) RETURN a, r, b");
        while(resultSet.hasNext()) {
            Record record = resultSet.next();
            // get values
            Node a = record.getValue("a");
            Edge r =  record.getValue("r");

            //print record
            System.out.println(record.toString());
        }

        // delete graph
        graph.deleteGraph("social");

        // get connection context - closable object
        try(RedisGraphContext context = graph.getContext()) {
            context.query("contextSocial","CREATE (:person{name:'roi',age:32})");
            context.query("contextSocial","CREATE (:person{name:%s,age:%d})", "amit", 30);
            context.query("contextSocial", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit') CREATE (a)-[:knows]->(b)");
            // WATCH/MULTI/EXEC
            context.watch("contextSocial");
            RedisGraphTransaction t = context.multi();
            t.query("contextSocial", "MATCH (a:person)-[r:knows]->(b:person) RETURN a, r, b");
            // support for Redis/Jedis native commands in transaction
            t.set("x", "1");
            t.get("x");
            // get multi/exec results
            List<Object> execResults =  t.exec();
            System.out.println(execResults.toString());

            context.deleteGraph("contextSocial");
        } catch (IOException e) {
            
        }

    }
}


```
