# Amazon DSQL Connector Developer Documentation

This document provides detailed information for developers working on the Amazon DSQL connector for Flink CDC.

## Architecture Overview

The Amazon DSQL connector is implemented as a sink connector that writes change data capture (CDC) events from MySQL or PostgreSQL sources to Amazon DSQL databases. The connector is designed with the following key architectural principles:

- **Modular Design**: Each component has a single responsibility and can be tested independently
- **IAM Authentication**: Secure authentication using AWS IAM tokens with automatic refresh
- **Connection Management**: Efficient connection pooling with automatic lifecycle management
- **Full Load Support**: Parallel full load capabilities with seamless transition to CDC
- **Error Resilience**: Comprehensive error handling with retry mechanisms

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DsqlSinkProvider                             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   DsqlSink                              │    │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐   │    │
│  │  │    IAM      │ │ Connection  │ │   Full Load     │   │    │
│  │  │Authenticator│ │Pool Manager │ │  Coordinator    │   │    │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘   │    │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐   │    │
│  │  │   Schema    │ │   Error     │ │    Metrics      │   │    │
│  │  │   Mapper    │ │  Reporter   │ │   Manager       │   │    │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. DsqlSinkProvider

**Location**: `org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkProvider`

The entry point for the DSQL connector that implements the `SinkProvider` interface.

**Responsibilities**:
- Parse and validate configuration options
- Create and configure the DsqlSink instance
- Handle connector lifecycle management

**Key Methods**:
- `createSink(SinkContext context)`: Creates a new DsqlSink instance

### 2. DsqlSink

**Location**: `org.apache.flink.cdc.connectors.dsql.sink.DsqlSink`

The main sink implementation that processes CDC events and writes them to DSQL.

**Responsibilities**:
- Coordinate between all sub-components
- Process CDC events (INSERT, UPDATE, DELETE)
- Handle schema evolution events
- Manage transaction boundaries

**Key Methods**:
- `write(Event event)`: Process a single CDC event
- `flush()`: Flush pending changes to DSQL
- `close()`: Clean up resources

### 3. DsqlIamAuthenticator

**Location**: `org.apache.flink.cdc.connectors.dsql.auth.DsqlIamAuthenticator`

Handles IAM authentication for DSQL connections using the Amazon DSQL SDK.

**Responsibilities**:
- Generate authentication tokens using AWS credentials
- Cache tokens and track expiration
- Automatically refresh tokens before expiry
- Handle authentication errors

**Key Methods**:
- `getAuthToken()`: Get current valid authentication token
- `refreshToken()`: Force token refresh

**Token Management**:
- Tokens are cached for 15 minutes (default DSQL token lifetime)
- Refresh occurs 5 minutes before expiration to ensure availability
- Thread-safe implementation for concurrent access

### 4. DsqlConnectionPoolManager

**Location**: `org.apache.flink.cdc.connectors.dsql.utils.DsqlConnectionPoolManager`

Manages DSQL database connections using HikariCP connection pooling.

**Responsibilities**:
- Create and configure connection pools
- Handle DSQL's 60-minute connection lifetime limit
- Provide connection acquisition and release
- Monitor connection health

**Key Methods**:
- `getConnection()`: Acquire a connection from the pool
- `close()`: Close the connection pool

**Connection Lifecycle**:
- Maximum connection lifetime: 59 minutes (to handle DSQL's 60-minute limit)
- Automatic connection refresh before expiration
- Connection validation on acquisition

### 5. DsqlFullLoadCoordinator

**Location**: `org.apache.flink.cdc.connectors.dsql.utils.DsqlFullLoadCoordinator`

Coordinates full load operations for initial data synchronization.

**Responsibilities**:
- Create target tables in DSQL
- Split source tables into chunks for parallel processing
- Execute parallel data loading
- Track source positions for CDC transition

**Key Methods**:
- `executeFullLoad(SourceTable, TargetTable, SourcePosition)`: Execute full load
- `splitTableIntoChunks(SourceTable, int)`: Split table for parallel processing

**Parallel Processing**:
- Configurable parallelism degree
- Chunk-based processing for large tables
- Progress tracking and error recovery

### 6. DsqlSchemaMapper

**Location**: `org.apache.flink.cdc.connectors.dsql.utils.DsqlSchemaMapper`

Handles schema mapping between source databases and DSQL.

**Responsibilities**:
- Map source data types to DSQL data types
- Generate DDL statements for DSQL
- Handle schema evolution events

**Key Methods**:
- `mapSchema(SourceTable)`: Map source schema to DSQL schema
- `generateCreateTableStatement(TargetTable)`: Generate CREATE TABLE DDL
- `generateInsertStatement(TargetTable)`: Generate INSERT DML

## Configuration System

### DsqlSinkConfig

**Location**: `org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkConfig`

Central configuration class that holds all connector settings.

**Configuration Categories**:

1. **Connection Settings**:
   - `host`: DSQL cluster endpoint
   - `port`: Connection port (default: 5432)
   - `database`: Target database name

2. **Authentication Settings**:
   - `use-iam-auth`: Enable IAM authentication
   - `region`: AWS region
   - `iam-role`: IAM role ARN (optional)
   - `admin-user`: Use admin privileges

3. **Connection Pool Settings**:
   - `max-pool-size`: Maximum connections
   - `min-pool-size`: Minimum connections
   - `connection-max-lifetime-ms`: Connection lifetime
   - `connection-idle-timeout-ms`: Idle timeout

4. **Full Load Settings**:
   - `enable-full-load`: Enable full load mode
   - `parallelism`: Parallel processing degree

### DsqlSinkConfigFactory

**Location**: `org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkConfigFactory`

Factory class for creating and validating configuration instances.

**Responsibilities**:
- Parse configuration from YAML/properties
- Validate configuration values
- Provide default values
- Handle configuration errors

## Data Flow

### 1. Initialization Flow

```
1. DsqlSinkProvider.createSink()
2. Parse and validate configuration
3. Create DsqlIamAuthenticator (if IAM auth enabled)
4. Create DsqlConnectionPoolManager
5. Create DsqlSchemaMapper
6. Create DsqlFullLoadCoordinator (if full load enabled)
7. Initialize DsqlSink with all components
```

### 2. Full Load Flow

```
1. DsqlSink receives schema event
2. DsqlSchemaMapper maps source schema to DSQL schema
3. DsqlFullLoadCoordinator creates target table
4. Source table is split into chunks
5. Parallel workers load chunks to DSQL
6. Source position is recorded for CDC transition
7. Transition to CDC mode
```

### 3. CDC Event Flow

```
1. DsqlSink receives CDC event (INSERT/UPDATE/DELETE)
2. Event is validated and transformed
3. DsqlSchemaMapper generates appropriate SQL statement
4. Statement is executed using connection from pool
5. Transaction is committed
6. Metrics are updated
```

## Error Handling Strategy

### 1. Connection Errors

- **Retry Logic**: Exponential backoff with configurable max retries
- **Connection Pool**: Automatic connection replacement on failure
- **Health Checks**: Regular connection validation

### 2. Authentication Errors

- **Token Refresh**: Automatic retry with fresh token
- **Credential Validation**: Early validation of AWS credentials
- **Error Reporting**: Detailed error messages for troubleshooting

### 3. Data Errors

- **Type Conversion**: Graceful handling of unsupported data types
- **Constraint Violations**: Detailed error reporting with context
- **Transaction Rollback**: Automatic rollback on errors

### 4. Schema Evolution Errors

- **DDL Validation**: Pre-validation of schema changes
- **Rollback Support**: Ability to rollback failed schema changes
- **Compatibility Checks**: Validation of schema compatibility

## Testing Strategy

### 1. Unit Tests

**Location**: `src/test/java/org/apache/flink/cdc/connectors/dsql/`

- **Component Tests**: Each component is tested in isolation
- **Mock Dependencies**: External dependencies are mocked
- **Edge Cases**: Comprehensive coverage of error scenarios

**Key Test Classes**:
- `DsqlIamAuthenticatorTest`: Authentication logic testing
- `DsqlConnectionPoolManagerTest`: Connection pool testing
- `DsqlSchemaMapperTest`: Schema mapping testing
- `DsqlFullLoadCoordinatorTest`: Full load logic testing

### 2. Integration Tests

**Location**: `src/test/java/org/apache/flink/cdc/connectors/dsql/DsqlE2eITCase.java`

- **End-to-End Testing**: Complete pipeline testing
- **Real DSQL Instance**: Testing against actual DSQL clusters
- **Multiple Sources**: Testing with MySQL and PostgreSQL sources

**Test Scenarios**:
- Full load from MySQL to DSQL
- Full load from PostgreSQL to DSQL
- Incremental CDC from MySQL to DSQL
- Incremental CDC from PostgreSQL to DSQL
- Error scenarios and recovery

### 3. Test Environment Setup

**Docker Compose**: `src/test/resources/docker-compose.yml`

The test environment includes:
- MySQL database with test data
- PostgreSQL database with test data
- DSQL cluster (or LocalStack for local testing)

**Test Data**: `src/test/resources/ddl/`
- `mysql_test_data.sql`: MySQL test schema and data
- `postgres_test_data.sql`: PostgreSQL test schema and data

## Extension Points

### 1. Custom Authentication

To implement custom authentication mechanisms:

1. Implement the `AuthenticationProvider` interface
2. Register the provider in `DsqlSinkConfigFactory`
3. Add configuration options for the custom provider

```java
public interface AuthenticationProvider {
    String getAuthToken();
    void refreshToken();
    boolean isTokenExpired();
}
```

### 2. Custom Schema Mapping

To implement custom schema mapping logic:

1. Extend the `DsqlSchemaMapper` class
2. Override the mapping methods for specific data types
3. Register the custom mapper in configuration

```java
public class CustomDsqlSchemaMapper extends DsqlSchemaMapper {
    @Override
    public TargetColumn mapColumn(SourceColumn sourceColumn) {
        // Custom mapping logic
    }
}
```

### 3. Custom Error Handling

To implement custom error handling:

1. Implement the `ErrorHandler` interface
2. Register the handler in `DsqlSink`
3. Configure error handling policies

```java
public interface ErrorHandler {
    void handleConnectionError(SQLException e);
    void handleDataError(DataException e);
    void handleSchemaError(SchemaException e);
}
```

## Metrics and Monitoring

### 1. Connection Metrics

- `dsql.connection.pool.active`: Active connections count
- `dsql.connection.pool.idle`: Idle connections count
- `dsql.connection.acquisition.time`: Connection acquisition time

### 2. Authentication Metrics

- `dsql.auth.token.refresh.count`: Token refresh count
- `dsql.auth.token.refresh.failures`: Token refresh failures

### 3. Processing Metrics

- `dsql.records.processed`: Total records processed
- `dsql.processing.latency`: Processing latency
- `dsql.errors.count`: Error count by type

### 4. Full Load Metrics

- `dsql.fullload.progress`: Full load progress percentage
- `dsql.fullload.records.loaded`: Records loaded count
- `dsql.fullload.tables.completed`: Completed tables count

## Performance Tuning

### 1. Connection Pool Tuning

```yaml
# High throughput configuration
max-pool-size: 20
min-pool-size: 5
connection-max-lifetime-ms: 3540000  # 59 minutes
connection-idle-timeout-ms: 300000   # 5 minutes
```

### 2. Batch Processing

```yaml
# Optimize for throughput
batch-size: 5000
flush-interval-ms: 1000
```

### 3. Full Load Optimization

```yaml
# Parallel full load
enable-full-load: true
parallelism: 8
```

## Debugging and Troubleshooting

### 1. Enable Debug Logging

Add to `log4j2.properties`:

```properties
logger.dsql.name = org.apache.flink.cdc.connectors.dsql
logger.dsql.level = DEBUG
```

### 2. Common Issues

**Authentication Failures**:
- Check AWS credentials configuration
- Verify IAM role permissions
- Check DSQL cluster accessibility

**Connection Issues**:
- Verify network connectivity
- Check security group settings
- Monitor connection pool metrics

**Performance Issues**:
- Tune connection pool settings
- Adjust batch sizes
- Monitor processing metrics

### 3. Diagnostic Tools

**Connection Pool Monitoring**:
```java
HikariPoolMXBean poolBean = connectionPool.getHikariPoolMXBean();
int activeConnections = poolBean.getActiveConnections();
int idleConnections = poolBean.getIdleConnections();
```

**Authentication Token Monitoring**:
```java
long tokenExpirationTime = authenticator.getTokenExpirationTime();
boolean isExpired = authenticator.isTokenExpired();
```

## Contributing

### 1. Development Setup

1. Clone the repository
2. Set up AWS credentials for testing
3. Run `mvn clean install` to build the project
4. Run tests with `mvn test`

### 2. Code Style

- Follow existing code formatting
- Add comprehensive JavaDoc comments
- Include unit tests for new functionality
- Update documentation for API changes

### 3. Pull Request Process

1. Create feature branch from main
2. Implement changes with tests
3. Update documentation
4. Submit pull request with detailed description
5. Address review feedback

## Future Enhancements

### 1. Planned Features

- **Schema Evolution**: Enhanced schema change handling
- **Multi-Region Support**: Cross-region replication
- **Advanced Monitoring**: Enhanced metrics and alerting
- **Performance Optimizations**: Further throughput improvements

### 2. Extension Opportunities

- **Custom Data Transformations**: Plugin system for data transformation
- **Advanced Error Recovery**: Sophisticated error recovery mechanisms
- **Compression Support**: Data compression for network efficiency
- **Encryption**: End-to-end encryption support

## References

- [Amazon DSQL Documentation](https://docs.aws.amazon.com/dsql/)
- [Flink CDC Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [HikariCP Documentation](https://github.com/brettwooldridge/HikariCP)
- [AWS SDK for Java Documentation](https://docs.aws.amazon.com/sdk-for-java/)