[![license](https://img.shields.io/github/license/RedisGraph/JRedisGraph.svg)](https://github.com/RedisGraph/JRedisGraph)
[![CircleCI](https://circleci.com/gh/RedisGraph/JRedisGraph/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGraph/JRedisGraph/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisgraph/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisgraph)
[![GitHub issues](https://img.shields.io/github/release/RedisGraph/JRedisGraph.svg)](https://github.com/RedisGraph/JRedisGraph/releases/latest)
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
      <version>1.0.4</version>
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
      <version>1.0.5-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

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
