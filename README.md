[![license](https://img.shields.io/github/license/RedisGraph/JRedisGraph.svg)](https://github.com/RedisGraph/JRedisGraph/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/release/RedisGraph/JRedisGraph.svg)](https://github.com/RedisGraph/JRedisGraph/releases/latest)
[![CircleCI](https://circleci.com/gh/RedisGraph/JRedisGraph/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGraph/JRedisGraph/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisgraph/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisgraph)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jredisgraph.svg)](https://www.javadoc.io/doc/com.redislabs/jredisgraph)
[![Codecov](https://codecov.io/gh/RedisGraph/JRedisGraph/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGraph/JRedisGraph)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisGraph/JRedisGraph/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RedisGraph/JRedisGraph?targetFile=pom.xml)

# JRedisGraph
[![Forum](https://img.shields.io/badge/Forum-RedisGraph-blue)](https://forum.redislabs.com/c/modules/redisgraph)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisGraph.svg)](https://gitter.im/RedisLabs/RedisGraph?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

RedisGraph Java client


### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisgraph</artifactId>
      <version>2.0.2</version>
    </dependency>
  </dependencies>
```

### Snapshots
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
      <version>2.0.3-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

# Example: Using the Java Client
```java
package com.redislabs.redisgraph;

import com.redislabs.redisgraph.graph_entities.Edge;
import com.redislabs.redisgraph.graph_entities.Node;
import com.redislabs.redisgraph.graph_entities.Path;
import com.redislabs.redisgraph.impl.api.RedisGraph;

import java.util.List;

public class RedisGraphExample {
    public static void main(String[] args) {
        // general context api. Not bound to graph key or connection
        RedisGraph graph = new RedisGraph();

        Map<String, Object> params = new HashMap<>();
           params.put("age", 30);
           params.put("name", "amit");
       
           // send queries to a specific graph called "social"
           graph.query("social","CREATE (:person{name:'roi',age:32})");
           graph.query("social","CREATE (:person{name:$name,age:$age})", params);
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

        resultSet = graph.query("social", "MATCH p = (:person)-[:knows]->(:person) RETURN p");
        while(resultSet.hasNext()) {
            Record record = resultSet.next();
            Path p = record.getValue("p");

            // More path API at Javadoc.
            System.out.println(p.nodeCount());
        }

        // delete graph
        graph.deleteGraph("social");

        // get connection context - closable object
        try(RedisGraphContext context = graph.getContext()) {
            context.query("contextSocial","CREATE (:person{name:'roi',age:32})");
            context.query("social","CREATE (:person{name:$name,age:$age})", params);
            context.query("contextSocial", "MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit') CREATE (a)-[:knows]->(b)");
            // WATCH/MULTI/EXEC
            context.watch("contextSocial");
            RedisGraphTransaction t = context.multi();
            t.query("contextSocial", "MATCH (a:person)-[r:knows]->(b:person{name:$name,age:$age}) RETURN a, r, b", params);
            // support for Redis/Jedis native commands in transaction
            t.set("x", "1");
            t.get("x");
            // get multi/exec results
            List<Object> execResults =  t.exec();
            System.out.println(execResults.toString());

            context.deleteGraph("contextSocial");
        }
    }
}

```

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FRedisGraph%2FJRedisGraph.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2FRedisGraph%2FJRedisGraph?ref=badge_large)
