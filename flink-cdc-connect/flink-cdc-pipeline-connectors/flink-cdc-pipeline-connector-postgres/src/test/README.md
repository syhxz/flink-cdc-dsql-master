# PostgreSQL Pipeline Connector Integration Tests

This directory contains comprehensive integration tests for the PostgreSQL pipeline connector implementation.

## Test Structure

The integration tests are organized into the following test classes:

### 1. PostgresConnectorIntegrationTest
- **Purpose**: Tests basic connector functionality and overall integration
- **Coverage**: 
  - Basic connector creation and configuration
  - Table discovery and schema retrieval
  - Multiple startup modes (snapshot, latest-offset, timestamp)
  - Multiple table configurations
  - Schema.table naming conventions
  - Basic error scenarios

### 2. PostgresSchemaDiscoveryIntegrationTest
- **Purpose**: Tests PostgreSQL schema discovery functionality
- **Coverage**:
  - Table discovery from database metadata
  - Data type mapping from PostgreSQL to Flink types
  - Primary key discovery (single and composite keys)
  - Nullable column detection
  - PostgreSQL-specific data types (UUID, JSON, TEXT, etc.)
  - Connection error handling

### 3. PostgresTableNameFormatterIntegrationTest
- **Purpose**: Tests table name formatting and parsing utilities
- **Coverage**:
  - Basic table name formatting (adding schema prefixes)
  - Explicit schema handling
  - Mixed table name formats
  - Whitespace handling
  - TableId parsing and validation
  - Special character handling
  - Case-sensitive table names
  - Identifier escaping
  - Multi-schema configurations

### 4. PostgresStartupOptionsIntegrationTest
- **Purpose**: Tests different startup mode configurations
- **Coverage**:
  - Snapshot startup mode
  - Latest-offset startup mode
  - Timestamp startup mode
  - Specific-offset startup mode
  - Default startup mode behavior
  - Invalid startup mode handling
  - Case-insensitive startup mode parsing
  - Startup mode validation

### 5. PostgresErrorScenariosIntegrationTest
- **Purpose**: Tests error handling and edge cases
- **Coverage**:
  - Missing required parameters
  - Invalid parameter values
  - Connection failures (wrong host, port, credentials)
  - Non-existent tables
  - Invalid table name formats
  - Invalid schema names
  - Resource cleanup
  - Concurrent connection attempts
  - Invalid startup mode configurations
  - Database permission issues

### 6. PostgresConnectorIntegrationTestSuite
- **Purpose**: Test suite runner for all integration tests
- **Usage**: Provides a convenient way to run all integration tests together

## Prerequisites

### Required Dependencies
- Docker (for TestContainers)
- Maven 3.6+
- Java 8+

### TestContainers
The integration tests use [TestContainers](https://www.testcontainers.org/) to spin up real PostgreSQL database instances for testing. This ensures that tests run against actual PostgreSQL databases rather than mocks.

**PostgreSQL Container Configuration:**
- Image: `postgres:13`
- Database: `testdb`
- Username: `testuser`
- Password: `testpass`
- WAL Level: `logical` (for CDC functionality)

## Running the Tests

### Run All Integration Tests
```bash
# From the postgres connector directory
mvn test -Dtest=PostgresConnectorIntegrationTestSuite

# Or run all tests in the test directory
mvn test
```

### Run Individual Test Classes
```bash
# Basic connector functionality
mvn test -Dtest=PostgresConnectorIntegrationTest

# Schema discovery tests
mvn test -Dtest=PostgresSchemaDiscoveryIntegrationTest

# Table name formatting tests
mvn test -Dtest=PostgresTableNameFormatterIntegrationTest

# Startup options tests
mvn test -Dtest=PostgresStartupOptionsIntegrationTest

# Error scenarios tests
mvn test -Dtest=PostgresErrorScenariosIntegrationTest
```

### Run Specific Test Methods
```bash
# Run a specific test method
mvn test -Dtest=PostgresConnectorIntegrationTest#testBasicConnectorFunctionality

# Run multiple specific test methods
mvn test -Dtest=PostgresConnectorIntegrationTest#testBasicConnectorFunctionality+testTableDiscoveryAndSchemaRetrieval
```

### Skip Integration Tests
```bash
# Skip all tests
mvn compile -DskipTests

# Skip only integration tests (if you have separate unit tests)
mvn test -DskipITs
```

## Test Database Schema

The integration tests automatically set up test databases with the following schema:

### Tables Created
1. **users**
   - `id` (SERIAL PRIMARY KEY)
   - `name` (VARCHAR(100) NOT NULL)
   - `email` (VARCHAR(255) UNIQUE)
   - `description` (TEXT, nullable)
   - `created_at` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

2. **orders**
   - `id` (INTEGER NOT NULL)
   - `user_id` (INTEGER NOT NULL)
   - `product_name` (VARCHAR(255) NOT NULL)
   - `quantity` (INTEGER NOT NULL)
   - `price` (DECIMAL(10,2) NOT NULL)
   - `order_date` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
   - Primary Key: `(id, user_id)` (composite key)

3. **products**
   - `id` (SERIAL PRIMARY KEY)
   - `name` (VARCHAR(255) NOT NULL)
   - `description` (TEXT)
   - `price` (DECIMAL(10,2) NOT NULL)
   - `in_stock` (BOOLEAN DEFAULT true)

4. **test_types** (for data type testing)
   - `id` (INTEGER PRIMARY KEY)
   - `bigint_col` (BIGINT)
   - `varchar_col` (VARCHAR(255))
   - `text_col` (TEXT)
   - `decimal_col` (DECIMAL(10,2))
   - `boolean_col` (BOOLEAN)
   - `timestamp_col` (TIMESTAMP)
   - `uuid_col` (UUID)
   - `json_col` (JSON)

### Multiple Schemas
Some tests create additional schemas:
- `inventory` schema with `test_products` table
- `analytics` schema with `test_reports` table

## Test Configuration

### Environment Variables
The tests use TestContainers which automatically manages PostgreSQL containers. No manual configuration is required.

### Test Properties
Tests use the following default configuration:
- Host: Automatically assigned by TestContainers
- Port: Automatically assigned by TestContainers
- Database: `testdb`
- Username: `testuser`
- Password: `testpass`
- Schema: `public`
- Slot Name: `test_slot`

## Troubleshooting

### Common Issues

1. **Docker Not Running**
   ```
   Error: Could not find a valid Docker environment
   ```
   **Solution**: Ensure Docker is installed and running

2. **Port Conflicts**
   ```
   Error: Port already in use
   ```
   **Solution**: TestContainers automatically assigns free ports, but if you have many PostgreSQL instances running, restart Docker

3. **Memory Issues**
   ```
   Error: OutOfMemoryError
   ```
   **Solution**: Increase JVM heap size: `export MAVEN_OPTS="-Xmx2g"`

4. **Test Timeouts**
   ```
   Error: Test timed out
   ```
   **Solution**: Increase test timeout or check if Docker is running slowly

### Debug Mode
To run tests with debug logging:
```bash
mvn test -Dtest=PostgresConnectorIntegrationTest -Dlogback.configurationFile=logback-test.xml -X
```

### Test Reports
Test reports are generated in:
- `target/surefire-reports/` - XML and TXT reports
- `target/site/surefire-report.html` - HTML report (after `mvn site`)

## Contributing

When adding new integration tests:

1. **Follow Naming Conventions**: Use descriptive test method names that clearly indicate what is being tested
2. **Use TestContainers**: All integration tests should use TestContainers for database setup
3. **Clean Up Resources**: Ensure proper resource cleanup in `@AfterAll` methods
4. **Test Error Cases**: Include both positive and negative test cases
5. **Document Test Purpose**: Add JavaDoc comments explaining what each test verifies
6. **Update Test Suite**: Add new test classes to `PostgresConnectorIntegrationTestSuite`

## Requirements Coverage

These integration tests cover all requirements from the PostgreSQL pipeline connector specification:

- ✅ **Requirement 1**: PostgreSQL as source in Flink CDC pipelines
- ✅ **Requirement 2**: Standard PostgreSQL CDC configuration options
- ✅ **Requirement 3**: Integration with Flink CDC pipeline framework
- ✅ **Requirement 4**: Table name formatting with schema.table convention
- ✅ **Requirement 5**: Full load functionality with startup options

All acceptance criteria from the requirements document are tested through these integration tests.