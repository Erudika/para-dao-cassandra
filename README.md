![Logo](https://s3-eu-west-1.amazonaws.com/org.paraio/para.png)
============================

> ### Apache Cassandra DAO plugin for Para

[![Maven Central Version](https://img.shields.io/maven-central/v/com.erudika/para-dao-cassandra)](https://central.sonatype.com/artifact/com.erudika/para-dao-cassandra)
[![Join the chat at https://gitter.im/Erudika/para](https://badges.gitter.im/Erudika/para.svg)](https://gitter.im/Erudika/para?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## What is this?

**Para** was designed as a simple and modular back-end framework for object persistence and retrieval.
It enables your application to store objects directly to a data store (NoSQL) or any relational database (RDBMS)
and it also automatically indexes those objects and makes them searchable.

This plugin allows Para to store data in a Cassandra database.

## Documentation

### [Read the Docs](https://paraio.org/docs)

## Getting started

The plugin is on Maven Central. Here's the Maven snippet to include in your `pom.xml`:

```xml
<dependency>
  <groupId>com.erudika</groupId>
  <artifactId>para-dao-cassandra</artifactId>
  <version>{see_green_version_badge_above}</version>
</dependency>
```

Alternatively you can download the JAR from the "Releases" tab above put it in a `lib` folder alongside the server
WAR file `para-x.y.z.war`. Para will look for plugins inside `lib` and pick up the Cassandra plugin.

### Configuration

Here are all the configuration properties for this plugin (these go inside your `application.conf`):
```ini
para.cassandra.hosts = "localhost,remotehost"
para.cassandra.port = 9042
para.cassandra.keyspace = "myapp"
para.cassandra.user = "user"
para.cassandra.password = "pass"
para.cassandra.replication_factor = 1

# SSL configuration
para.cassandra.ssl_enabled = false
para.cassandra.ssl_protocols = ""
para.cassandra.ssl_keystore = ""
para.cassandra.ssl_keystore_password = ""
para.cassandra.ssl_truststore = ""
para.cassandra.ssl_truststore_password = ""
```

Finally, set the config property:
```
para.dao = "CassandraDAO"
```
This could be a Java system property or part of a `application.conf` file on the classpath.
This tells Para to use the Cassandra Data Access Object (DAO) implementation instead of the default.

### Schema

**BREAKING CHANGE:** The schema has changed in v1.30.0 - column `json_updates` was added.
**Execute the following statement before switching to the new version:**
```sql
ALTER TABLE {app_identifier} ADD json_updates NVARCHAR;
```
This is not required for tables created after v1.30.0.

Here's the schema for each table created by Para:
```sql
CREATE TABLE {app_identifier} (
    id            text PRIMARY KEY,
    json          text,
    json_updates  text
)
```

### Requirements

- Cassandra Java Driver by DataStax
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
