---
title: "Amazon DSQL"
weight: 4
type: docs
aliases:
- /connectors/pipeline-connectors/dsql.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Amazon DSQL Connector

The Amazon DSQL connector allows for reading data from and writing data to Amazon DSQL (Distributed SQL) databases. This document describes how to set up the Amazon DSQL connector to run SQL queries against Amazon DSQL databases.

## What is Amazon DSQL

Amazon DSQL is a distributed SQL database service that provides high availability, scalability, and performance for modern applications. It supports standard SQL queries and provides built-in security features including IAM authentication.

## Supported Versions

The Amazon DSQL connector supports:
- Amazon DSQL (all versions)
- IAM authentication
- Connection pooling with automatic token refresh

## Dependencies

In order to set up the Amazon DSQL connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cdc-pipeline-connector-dsql</artifactId>
  <version>{{< param Version >}}</version>
</dependency>
```

### SQL Client JAR

Download [flink-cdc-pipeline-connector-dsql-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-dsql/{{< param Version >}}/flink-cdc-pipeline-connector-dsql-{{< param Version >}}.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** Refer to [flink-cdc-pipeline-connector-dsql](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-dsql), more released versions will be available in the Maven central warehouse.

## Setup Amazon DSQL

Follow the steps below to setup a Amazon DSQL database:

1. Create an Amazon DSQL cluster in your AWS account
2. Configure IAM roles and permissions for DSQL access
3. Note down the cluster endpoint and region
4. Ensure your application has appropriate AWS credentials configured

## How to create an Amazon DSQL table

Create a table in your Amazon DSQL database:

```sql
CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  description TEXT,
  weight DECIMAL(10,3)
);
```

## How to create an Amazon DSQL sink connector

The Amazon DSQL sink connector can be defined as follows:

```yaml
sink:
  type: dsql
  host: my-dsql-cluster.amazonaws.com
  port: 5432
  database: my_database
  use-iam-auth: true
  region: us-west-2
  max-pool-size: 10
  min-pool-size: 2
  connection-max-lifetime-ms: 3540000  # 59 minutes
  connection-idle-timeout-ms: 300000   # 5 minutes
  enable-full-load: true
  parallelism: 4
```

## Connector Options

<div class="highlight">
<table class="colwidths-auto docutils">
<thead>
<tr class="row-odd">
<th class="text-left" style="width: 25%">Option</th>
<th class="text-left" style="width: 8%">Required</th>
<th class="text-left" style="width: 7%">Default</th>
<th class="text-left" style="width: 10%">Type</th>
<th class="text-left" style="width: 50%">Description</th>
</tr>
</thead>
<tbody>
<tr class="row-even">
<td>type</td>
<td>required</td>
<td style="word-wrap: break-word;">(none)</td>
<td>String</td>
<td>Specify what connector to use, here should be <code>'dsql'</code>.</td>
</tr>
<tr class="row-odd">
<td>host</td>
<td>required</td>
<td style="word-wrap: break-word;">(none)</td>
<td>String</td>
<td>IP address or hostname of the Amazon DSQL cluster.</td>
</tr>
<tr class="row-even">
<td>port</td>
<td>optional</td>
<td style="word-wrap: break-word;">5432</td>
<td>Integer</td>
<td>Integer port number of the Amazon DSQL cluster.</td>
</tr>
<tr class="row-odd">
<td>database</td>
<td>required</td>
<td style="word-wrap: break-word;">(none)</td>
<td>String</td>
<td>Database name of the Amazon DSQL cluster to connect to.</td>
</tr>
<tr class="row-even">
<td>use-iam-auth</td>
<td>optional</td>
<td style="word-wrap: break-word;">true</td>
<td>Boolean</td>
<td>Whether to use IAM authentication. When true, AWS credentials will be used to generate authentication tokens.</td>
</tr>
<tr class="row-odd">
<td>region</td>
<td>required when use-iam-auth is true</td>
<td style="word-wrap: break-word;">(none)</td>
<td>String</td>
<td>AWS region where the DSQL cluster is located.</td>
</tr>
<tr class="row-even">
<td>username</td>
<td>required when use-iam-auth is false</td>
<td style="word-wrap: break-word;">(none)</td>
<td>String</td>
<td>Username for database authentication (only used when IAM auth is disabled).</td>
</tr>
<tr class="row-odd">
<td>password</td>
<td>required when use-iam-auth is false</td>
<td style="word-wrap: break-word;">(none)</td>
<td>String</td>
<td>Password for database authentication (only used when IAM auth is disabled).</td>
</tr>
<tr class="row-even">
<td>max-pool-size</td>
<td>optional</td>
<td style="word-wrap: break-word;">10</td>
<td>Integer</td>
<td>Maximum number of connections in the connection pool.</td>
</tr>
<tr class="row-odd">
<td>min-pool-size</td>
<td>optional</td>
<td style="word-wrap: break-word;">2</td>
<td>Integer</td>
<td>Minimum number of connections in the connection pool.</td>
</tr>
<tr class="row-even">
<td>connection-max-lifetime-ms</td>
<td>optional</td>
<td style="word-wrap: break-word;">3540000</td>
<td>Long</td>
<td>Maximum lifetime of a connection in milliseconds (default: 59 minutes to handle DSQL's 60-minute limit).</td>
</tr>
<tr class="row-odd">
<td>connection-idle-timeout-ms</td>
<td>optional</td>
<td style="word-wrap: break-word;">300000</td>
<td>Long</td>
<td>Maximum time a connection can remain idle in milliseconds (default: 5 minutes).</td>
</tr>
<tr class="row-even">
<td>enable-full-load</td>
<td>optional</td>
<td style="word-wrap: break-word;">false</td>
<td>Boolean</td>
<td>Whether to enable full load mode for initial data synchronization.</td>
</tr>
<tr class="row-odd">
<td>parallelism</td>
<td>optional</td>
<td style="word-wrap: break-word;">1</td>
<td>Integer</td>
<td>Parallelism degree for full load operations.</td>
</tr>
</tbody>
</table>
</div>

## Authentication

### IAM Authentication (Recommended)

The Amazon DSQL connector supports IAM authentication, which is the recommended approach for production environments. When `use-iam-auth` is set to `true`, the connector will:

1. Use AWS credentials from the environment (environment variables, instance profile, etc.)
2. Generate authentication tokens using the Amazon DSQL SDK
3. Automatically refresh tokens before they expire
4. Handle token rotation transparently

### Username/Password Authentication

For development or testing purposes, you can use traditional username/password authentication by setting `use-iam-auth` to `false` and providing `username` and `password` options.

## Connection Management

The Amazon DSQL connector implements sophisticated connection management to handle DSQL's 60-minute maximum connection duration:

- **Connection Pooling**: Uses HikariCP for efficient connection pooling
- **Automatic Refresh**: Connections are automatically refreshed before the 60-minute limit
- **Retry Logic**: Implements exponential backoff for connection failures
- **Health Checks**: Regular connection health checks to ensure reliability

## Full Load Support

The connector supports full load mode for initial data synchronization:

- **Parallel Loading**: Configurable parallelism for large table loads
- **Position Tracking**: Records source positions (binlog/WAL) for seamless CDC transition
- **Schema Mapping**: Automatic schema mapping between source and target
- **Error Recovery**: Comprehensive error handling and recovery options

## Data Type Mapping

The Amazon DSQL connector automatically maps data types between source databases and DSQL:

| Source Type (MySQL/PostgreSQL) | DSQL Type | Notes |
|--------------------------------|-----------|-------|
| TINYINT, SMALLINT, INT | INTEGER | |
| BIGINT | BIGINT | |
| FLOAT, REAL | REAL | |
| DOUBLE | DOUBLE PRECISION | |
| DECIMAL, NUMERIC | DECIMAL | Precision and scale preserved |
| CHAR, VARCHAR | VARCHAR | Length preserved |
| TEXT | TEXT | |
| DATE | DATE | |
| TIME | TIME | |
| TIMESTAMP | TIMESTAMP | |
| BOOLEAN | BOOLEAN | |

## Monitoring and Metrics

The connector exposes various metrics for monitoring:

- **Connection Pool Metrics**: Active connections, idle connections, acquisition time
- **Authentication Metrics**: Token refresh count, authentication failures
- **Full Load Metrics**: Records loaded, load progress, throughput
- **CDC Metrics**: Events processed, processing latency, commit latency
- **Error Metrics**: Error count by category, retry attempts

## Example Pipeline Configuration

Here's a complete example of a pipeline configuration that syncs data from PostgreSQL to Amazon DSQL:

```yaml
################################################################################
# Description: Sync tables from PostgreSQL to Amazon DSQL
################################################################################

source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: postgres
  database-name: postgres
  schema-name: public
  table-name: orders,products,customers

sink:
  type: dsql
  host: my-dsql-cluster.amazonaws.com
  port: 5432
  database: my_database
  use-iam-auth: true
  region: us-west-2
  max-pool-size: 10
  min-pool-size: 2
  connection-max-lifetime-ms: 3540000  # 59 minutes
  connection-idle-timeout-ms: 300000   # 5 minutes
  enable-full-load: true
  parallelism: 4

pipeline:
  name: PostgreSQL to DSQL Pipeline
  parallelism: 2
```

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify AWS credentials are properly configured
   - Check IAM role permissions for DSQL access
   - Ensure the region is correct

2. **Connection Timeouts**
   - Verify network connectivity to DSQL cluster
   - Check security groups and firewall rules
   - Adjust connection timeout settings

3. **Schema Errors**
   - Verify table schemas are compatible
   - Check data type mappings
   - Ensure primary keys are properly defined

4. **Performance Issues**
   - Adjust connection pool settings
   - Tune parallelism for full load operations
   - Monitor connection pool utilization

### Error Messages

The connector provides detailed error messages with context and troubleshooting suggestions. Check the logs for specific error categories and recommended actions.

## Limitations

- Amazon DSQL has a 60-minute maximum connection duration, which is automatically handled by the connector
- Some advanced PostgreSQL/MySQL features may not be supported in DSQL
- Schema evolution support is limited (manual intervention may be required for complex schema changes)

## FAQ

**Q: Can I use the DSQL connector with other source databases besides MySQL and PostgreSQL?**

A: The connector is designed to work with any Flink CDC source, but has been specifically tested with MySQL and PostgreSQL sources.

**Q: How does the connector handle DSQL's 60-minute connection limit?**

A: The connector automatically manages connection lifecycles, refreshing connections before they reach the 60-minute limit to ensure uninterrupted operation.

**Q: Can I customize the data type mappings?**

A: Currently, the connector uses automatic data type mapping. Custom mappings may be added in future versions.

**Q: Is the connector suitable for production use?**

A: Yes, the connector includes comprehensive error handling, monitoring, retry logic, and connection management suitable for production environments.