/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSchemaDiscovery;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.utils.TestCategories;
import org.apache.flink.cdc.connectors.postgres.utils.TestConfiguration;
import org.apache.flink.cdc.connectors.postgres.utils.TestContainerManager;
import org.apache.flink.cdc.connectors.postgres.utils.TestDataManager;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarCharType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Integration tests for PostgreSQL schema discovery functionality. */
@TestCategories.IntegrationTest
@TestCategories.DockerRequired
@TestCategories.SchemaTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresSchemaDiscoveryIntegrationTest {

    private TestContainerManager containerManager;
    private PostgresSourceConfig sourceConfig;
    private String jdbcUrl;
    private Properties connectionProps;

    @BeforeAll
    void setUp() throws Exception {
        containerManager = new TestContainerManager();
        
        // This will gracefully skip the test if Docker is not available
        PostgreSQLContainer<?> container = containerManager.getOrCreateContainer();
        
        jdbcUrl = containerManager.getJdbcUrl();
        connectionProps = new Properties();
        connectionProps.setProperty("user", containerManager.getUsername());
        connectionProps.setProperty("password", containerManager.getPassword());

        // Set up test database schema and data using TestDataManager
        setupTestDatabase();
        
        // Create source config using TestConfiguration
        TestConfiguration testConfig = TestConfiguration.fromContainerManager(containerManager)
                .toBuilder()
                .tables("public.test_types", "public.users", "public.orders")
                .build();
        
        sourceConfig = new PostgresSourceConfig(
                testConfig.getHostname(),
                testConfig.getPort(),
                testConfig.getUsername(),
                testConfig.getPassword(),
                testConfig.getDatabaseName(),
                testConfig.getSchemaName(),
                Arrays.asList("public.test_types", "public.users", "public.orders"),
                testConfig.getSlotName(),
                StartupOptions.snapshot()
        );
    }

    @AfterAll
    void tearDown() {
        if (containerManager != null) {
            containerManager.cleanup();
        }
    }

    @Test
    void testTableDiscovery() throws Exception {
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(sourceConfig)) {
            List<TableId> tables = discovery.discoverTables();
            
            assertEquals(3, tables.size());
            assertTrue(tables.stream().anyMatch(t -> "test_types".equals(t.getTableName())));
            assertTrue(tables.stream().anyMatch(t -> "users".equals(t.getTableName())));
            assertTrue(tables.stream().anyMatch(t -> "orders".equals(t.getTableName())));
            
            // All tables should be in public schema
            for (TableId tableId : tables) {
                assertEquals("public", tableId.getSchemaName());
            }
        }
    }

    @Test
    void testDataTypeMapping() throws Exception {
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(sourceConfig)) {
            TableId testTypesTable = TableId.tableId("public", "test_types");
            Schema schema = discovery.getTableSchema(testTypesTable);
            
            assertNotNull(schema);
            List<Column> columns = schema.getColumns();
            assertFalse(columns.isEmpty());
            
            // Verify specific data type mappings
            Column idColumn = findColumn(columns, "id");
            assertNotNull(idColumn);
            assertTrue(idColumn.getType() instanceof IntType);
            
            Column bigintColumn = findColumn(columns, "bigint_col");
            assertNotNull(bigintColumn);
            assertTrue(bigintColumn.getType() instanceof BigIntType);
            
            Column varcharColumn = findColumn(columns, "varchar_col");
            assertNotNull(varcharColumn);
            assertTrue(varcharColumn.getType() instanceof VarCharType);
            
            Column decimalColumn = findColumn(columns, "decimal_col");
            assertNotNull(decimalColumn);
            assertTrue(decimalColumn.getType() instanceof DecimalType);
            
            Column booleanColumn = findColumn(columns, "boolean_col");
            assertNotNull(booleanColumn);
            assertTrue(booleanColumn.getType() instanceof BooleanType);
            
            Column timestampColumn = findColumn(columns, "timestamp_col");
            assertNotNull(timestampColumn);
            assertTrue(timestampColumn.getType() instanceof TimestampType);
        }
    }

    @Test
    void testPrimaryKeyDiscovery() throws Exception {
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(sourceConfig)) {
            // Test single primary key
            TableId usersTable = TableId.tableId("public", "users");
            Schema usersSchema = discovery.getTableSchema(usersTable);
            
            List<String> usersPrimaryKeys = usersSchema.primaryKeys();
            assertEquals(1, usersPrimaryKeys.size());
            assertEquals("id", usersPrimaryKeys.get(0));
            
            // Test composite primary key
            TableId ordersTable = TableId.tableId("public", "orders");
            Schema ordersSchema = discovery.getTableSchema(ordersTable);
            
            List<String> ordersPrimaryKeys = ordersSchema.primaryKeys();
            assertEquals(2, ordersPrimaryKeys.size());
            assertTrue(ordersPrimaryKeys.contains("id"));
            assertTrue(ordersPrimaryKeys.contains("user_id"));
        }
    }

    @Test
    void testNullableColumns() throws Exception {
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(sourceConfig)) {
            TableId usersTable = TableId.tableId("public", "users");
            Schema schema = discovery.getTableSchema(usersTable);
            
            List<Column> columns = schema.getColumns();
            
            // id column should not be nullable (PRIMARY KEY)
            Column idColumn = findColumn(columns, "id");
            assertNotNull(idColumn);
            assertFalse(idColumn.getType().isNullable());
            
            // name column should not be nullable (NOT NULL)
            Column nameColumn = findColumn(columns, "name");
            assertNotNull(nameColumn);
            assertFalse(nameColumn.getType().isNullable());
            
            // description column should be nullable
            Column descColumn = findColumn(columns, "description");
            if (descColumn != null) { // Only if the column exists
                assertTrue(descColumn.getType().isNullable());
            }
        }
    }

    @Test
    void testSchemaForNonExistentTable() throws Exception {
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(sourceConfig)) {
            TableId nonExistentTable = TableId.tableId("public", "non_existent_table");
            
            assertThrows(IllegalArgumentException.class, () -> {
                discovery.getTableSchema(nonExistentTable);
            });
        }
    }

    @Test
    void testPostgresSpecificTypes() throws Exception {
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(sourceConfig)) {
            TableId testTypesTable = TableId.tableId("public", "test_types");
            Schema schema = discovery.getTableSchema(testTypesTable);
            
            List<Column> columns = schema.getColumns();
            
            // Test UUID type
            Column uuidColumn = findColumn(columns, "uuid_col");
            if (uuidColumn != null) {
                DataType dataType = uuidColumn.getType();
                assertNotNull(dataType);
                // UUID should be mapped to CHAR(36)
            }
            
            // Test JSON type
            Column jsonColumn = findColumn(columns, "json_col");
            if (jsonColumn != null) {
                DataType dataType = jsonColumn.getType();
                assertNotNull(dataType);
                // JSON should be mapped to VARCHAR
                assertTrue(dataType instanceof VarCharType);
            }
            
            // Test TEXT type
            Column textColumn = findColumn(columns, "text_col");
            if (textColumn != null) {
                DataType dataType = textColumn.getType();
                assertNotNull(dataType);
                assertTrue(dataType instanceof VarCharType);
            }
        }
    }

    @Test
    void testConnectionErrorHandling() {
        // Create config with invalid connection parameters
        PostgresSourceConfig invalidConfig = new PostgresSourceConfig(
                "invalid-host",
                5432,
                "invalid-user",
                "invalid-pass",
                "invalid-db",
                "public",
                Arrays.asList("public.users"),
                "test_slot",
                StartupOptions.snapshot()
        );
        
        try (PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery(invalidConfig)) {
            assertThrows(RuntimeException.class, () -> {
                discovery.discoverTables();
            });
        } catch (Exception e) {
            // Expected - connection should fail
        }
    }

    private Column findColumn(List<Column> columns, String columnName) {
        return columns.stream()
                .filter(col -> columnName.equals(col.getName()))
                .findFirst()
                .orElse(null);
    }

    private void setupTestDatabase() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            TestDataManager dataManager = new TestDataManager(conn);
            
            // Set up standard test schema
            dataManager.setupTestSchema();
            
            // Create additional test_types table for schema discovery testing
            dataManager.createCustomTable("test_types", 
                "id INTEGER PRIMARY KEY, " +
                "bigint_col BIGINT, " +
                "varchar_col VARCHAR(255), " +
                "text_col TEXT, " +
                "decimal_col DECIMAL(10,2), " +
                "boolean_col BOOLEAN, " +
                "timestamp_col TIMESTAMP, " +
                "uuid_col UUID, " +
                "json_col JSON");
            
            // Create orders table with composite primary key for testing
            dataManager.createCustomTable("orders", 
                "id INTEGER NOT NULL, " +
                "user_id INTEGER NOT NULL, " +
                "product_name VARCHAR(255) NOT NULL, " +
                "quantity INTEGER NOT NULL, " +
                "price DECIMAL(10,2) NOT NULL, " +
                "order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "PRIMARY KEY (id, user_id)");
            
            // Insert test data
            dataManager.insertTestData();
            
            // Insert specific test data for schema discovery
            dataManager.insertCustomData("test_types", 
                "id, bigint_col, varchar_col, text_col, decimal_col, boolean_col, timestamp_col",
                "(1, 9223372036854775807, 'test string', 'long text content', 123.45, true, CURRENT_TIMESTAMP)");
            
            dataManager.insertCustomData("orders", 
                "id, user_id, product_name, quantity, price",
                "(1, 1, 'Laptop', 1, 999.99), (2, 1, 'Mouse', 2, 29.99)");
        }
    }
}
