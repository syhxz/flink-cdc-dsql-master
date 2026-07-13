# PostgreSQL Pipeline Connector

The PostgreSQL Pipeline Connector allows you to use PostgreSQL as a source in Flink CDC pipelines. It captures changes from PostgreSQL databases using Change Data Capture (CDC) and streams them to various sinks.

## Features

- **Full Load Support**: Perform initial data synchronization before starting incremental CDC
- **Multiple Startup Modes**: Support for snapshot, latest-offset, specific-offset, and timestamp startup modes
- **Schema Discovery**: Automatic discovery of table schemas and metadata
- **Table Name Formatting**: Proper handling of PostgreSQL schema.table naming conventions
- **Comprehensive Validation**: Configuration validation with clear error messages
- **Error Handling**: Robust error handling and recovery mechanisms

## Configuration Options

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `hostname` | String | PostgreSQL server hostname |
| `username` | String | PostgreSQL username |
| `password` | String | PostgreSQL password |
| `database-name` | String | PostgreSQL database name |
| `tables` | String | Comma-separated list of table names to monitor |

### Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | Integer | 5432 | PostgreSQL server port |
| `schema-name` | String | "public" | Default schema name |
| `slot.name` | String | "flink_cdc_slot" | PostgreSQL replication slot name |
| `scan.startup.mode` | String | "snapshot" | Startup mode (snapshot, latest-offset, specific-offset, timestamp) |
| `scan.startup.specific-offset` | String | - | Specific offset for startup (required when using specific-offset mode) |
| `scan.startup.timestamp-millis` | Long | - | Timestamp in milliseconds for startup (required when using timestamp mode) |

## Usage Examples

### Basic Configuration

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: password
  database-name: mydb
  schema-name: public
  tables: "users,orders,products"
  slot.name: flink_cdc_slot
```

### Multiple Schemas

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: password
  database-name: mydb
  schema-name: public
  tables: "public.users,inventory.products,sales.orders"
  slot.name: flink_cdc_slot
```

### Snapshot Mode (Full Load)

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: password
  database-name: mydb
  tables: "users,orders"
  scan.startup.mode: snapshot
```

### Latest Offset Mode (Incremental Only)

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: password
  database-name: mydb
  tables: "users,orders"
  scan.startup.mode: latest-offset
```

### Timestamp Mode

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: password
  database-name: mydb
  tables: "users,orders"
  scan.startup.mode: timestamp
  scan.startup.timestamp-millis: 1640995200000
```

### Specific Offset Mode

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: password
  database-name: mydb
  tables: "users,orders"
  scan.startup.mode: specific-offset
  scan.startup.specific-offset: "0/1234567"
```

## Startup Modes

### Snapshot Mode
- Performs a full load of existing data before starting incremental CDC
- Recommended for initial data synchronization
- Ensures all existing data is captured

### Latest Offset Mode
- Starts CDC from the latest available offset
- Only captures new changes after the connector starts
- Faster startup but may miss existing data

### Specific Offset Mode
- Starts CDC from a specific PostgreSQL LSN (Log Sequence Number)
- Useful for resuming from a known position
- Requires knowledge of PostgreSQL WAL positions

### Timestamp Mode
- Starts CDC from a specific timestamp
- Captures all changes after the specified time
- Useful for point-in-time recovery scenarios

## Table Name Conventions

The PostgreSQL connector supports flexible table naming:

1. **Simple table names**: `users` → `public.users` (uses default schema)
2. **Schema-qualified names**: `inventory.products` → `inventory.products` (uses specified schema)
3. **Mixed naming**: `users,inventory.products` → `public.users,inventory.products`

## Data Type Mapping

The connector automatically maps PostgreSQL data types to Flink data types:

| PostgreSQL Type | Flink Type |
|-----------------|------------|
| BOOLEAN, BIT | BOOLEAN |
| SMALLINT | SMALLINT |
| INTEGER | INT |
| BIGINT | BIGINT |
| REAL, FLOAT | FLOAT |
| DOUBLE PRECISION | DOUBLE |
| NUMERIC, DECIMAL | DECIMAL |
| CHAR | CHAR |
| VARCHAR, TEXT | STRING |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| BINARY, BYTEA | BYTES |
| UUID | CHAR(36) |
| JSON, JSONB | STRING |
| Arrays | STRING |

## Prerequisites

1. **PostgreSQL Version**: PostgreSQL 9.6 or later
2. **Logical Replication**: Must be enabled in PostgreSQL configuration
3. **Replication Slot**: The connector will create a replication slot
4. **Permissions**: User must have replication permissions

### PostgreSQL Configuration

Add the following to your `postgresql.conf`:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Grant replication permissions to your user:

```sql
ALTER USER your_user REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;
```

## Error Handling

The connector provides comprehensive error handling:

### Configuration Errors
- Missing required parameters
- Invalid port ranges
- Invalid table names
- Invalid startup mode configurations

### Connection Errors
- Database connectivity issues
- Authentication failures
- Network timeouts

### Runtime Errors
- Table not found
- Permission denied
- Replication slot conflicts
- Data type conversion errors

## Monitoring

The connector provides logging for:
- Connection establishment and termination
- Table discovery progress
- Schema discovery results
- Configuration validation
- Error conditions

## Troubleshooting

### Common Issues

1. **Table not found**
   - Verify table names and schema
   - Check user permissions
   - Ensure tables exist in the database

2. **Connection refused**
   - Verify hostname and port
   - Check PostgreSQL server status
   - Verify network connectivity

3. **Authentication failed**
   - Verify username and password
   - Check PostgreSQL authentication configuration
   - Ensure user has required permissions

4. **Replication slot conflicts**
   - Use unique slot names for different connectors
   - Clean up unused replication slots
   - Monitor replication slot usage

### Debug Logging

Enable debug logging to get detailed information:

```yaml
# In your Flink configuration
rootLogger.level = DEBUG
logger.postgres.name = org.apache.flink.cdc.connectors.postgres
logger.postgres.level = DEBUG
```

## Performance Considerations

1. **Replication Slot Management**: Monitor and clean up unused replication slots
2. **Network Bandwidth**: Consider network capacity for large data volumes
3. **PostgreSQL Load**: Monitor impact on source database performance
4. **Parallelism**: Configure appropriate parallelism for your workload

## Limitations

1. **Schema Changes**: Limited support for DDL changes during runtime
2. **Large Objects**: BLOB/CLOB types may have size limitations
3. **Complex Types**: Some PostgreSQL-specific types are mapped to STRING
4. **Specific Offset**: Full implementation of specific offset mode is pending

## Examples

See the `examples/` directory for complete pipeline configuration examples:
- `postgres-to-kafka.yaml`: Stream PostgreSQL changes to Kafka
- `postgres-to-elasticsearch.yaml`: Stream PostgreSQL changes to Elasticsearch
- `postgres-full-load.yaml`: Perform full load with incremental CDC