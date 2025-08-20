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
import org.apache.flink.cdc.connectors.postgres.utils.PostgresTableNameFormatter;
import org.apache.flink.cdc.connectors.postgres.utils.TestCategories;
import org.apache.flink.cdc.connectors.postgres.utils.TestContainerManager;
import org.apache.flink.cdc.connectors.postgres.utils.TestDataManager;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Integration tests for PostgreSQL table name formatting with real database. */
@TestCategories.IntegrationTest
@TestCategories.DockerRequired
@TestCategories.TableNamingTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresTableNameFormatterIntegrationTest {

    private TestContainerManager containerManager;
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

        // Set up test database with various schema and table names
        setupTestDatabase();
    }

    @AfterAll
    void tearDown() {
        if (containerManager != null) {
            containerManager.cleanup();
        }
    }

    @Test
    void testBasicTableNameFormatting() {
        // Test simple table names without schema
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", "users,orders");
        
        assertEquals(2, formatted.length);
        assertEquals("public.users", formatted[0]);
        assertEquals("public.orders", formatted[1]);
    }

    @Test
    void testTableNamesWithExplicitSchema() {
        // Test table names with explicit schema
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", "public.users,inventory.products");
        
        assertEquals(2, formatted.length);
        assertEquals("public.users", formatted[0]);
        assertEquals("inventory.products", formatted[1]);
    }

    @Test
    void testMixedTableNameFormats() {
        // Test mixed formats (some with schema, some without)
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", "users,inventory.products,public.orders");
        
        assertEquals(3, formatted.length);
        assertEquals("public.users", formatted[0]);
        assertEquals("inventory.products", formatted[1]);
        assertEquals("public.orders", formatted[2]);
    }

    @Test
    void testTableNameFormattingWithWhitespace() {
        // Test table names with various whitespace
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", " users , orders , products ");
        
        assertEquals(3, formatted.length);
        assertEquals("public.users", formatted[0]);
        assertEquals("public.orders", formatted[1]);
        assertEquals("public.products", formatted[2]);
    }

    @Test
    void testTableIdParsing() {
        // Test parsing formatted table names to TableId objects
        String[] formattedTables = {"public.users", "inventory.products", "analytics.reports"};
        List<TableId> tableIds = PostgresTableNameFormatter.parseTableIds(formattedTables);
        
        assertEquals(3, tableIds.size());
        
        TableId usersTable = tableIds.get(0);
        assertEquals("public", usersTable.getSchemaName());
        assertEquals("users", usersTable.getTableName());
        
        TableId productsTable = tableIds.get(1);
        assertEquals("inventory", productsTable.getSchemaName());
        assertEquals("products", productsTable.getTableName());
        
        TableId reportsTable = tableIds.get(2);
        assertEquals("analytics", reportsTable.getSchemaName());
        assertEquals("reports", reportsTable.getTableName());
    }

    @Test
    void testSpecialCharacterHandling() {
        // Test table names with special characters (that need escaping)
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", "user_data,order-history");
        
        assertEquals(2, formatted.length);
        assertEquals("public.user_data", formatted[0]);
        assertEquals("public.order-history", formatted[1]);
    }

    @Test
    void testCaseSensitiveTableNames() {
        // Test case-sensitive table names
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", "Users,ORDERS,MixedCase");
        
        assertEquals(3, formatted.length);
        assertEquals("public.Users", formatted[0]);
        assertEquals("public.ORDERS", formatted[1]);
        assertEquals("public.MixedCase", formatted[2]);
    }

    @Test
    void testTableNameValidation() {
        // Test valid table names
        assertTrue(PostgresTableNameFormatter.isValidTableName("users"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("user_data"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("_private"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("table123"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("table$data"));
        
        // Test invalid table names
        assertFalse(PostgresTableNameFormatter.isValidTableName("123table")); // starts with number
        assertFalse(PostgresTableNameFormatter.isValidTableName("table-name")); // contains hyphen
        assertFalse(PostgresTableNameFormatter.isValidTableName("table name")); // contains space
        assertFalse(PostgresTableNameFormatter.isValidTableName("")); // empty
        assertFalse(PostgresTableNameFormatter.isValidTableName(null)); // null
    }

    @Test
    void testSchemaNameValidation() {
        // Test valid schema names
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("public"));
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("inventory"));
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("_private"));
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("schema123"));
        
        // Test invalid schema names
        assertFalse(PostgresTableNameFormatter.isValidSchemaName("123schema"));
        assertFalse(PostgresTableNameFormatter.isValidSchemaName("schema-name"));
        assertFalse(PostgresTableNameFormatter.isValidSchemaName(""));
        assertFalse(PostgresTableNameFormatter.isValidSchemaName(null));
    }

    @Test
    void testIdentifierEscaping() {
        // Test identifiers that don't need escaping
        assertEquals("users", PostgresTableNameFormatter.escapeIdentifier("users"));
        assertEquals("user_data", PostgresTableNameFormatter.escapeIdentifier("user_data"));
        
        // Test identifiers that need escaping
        assertEquals("\"user-data\"", PostgresTableNameFormatter.escapeIdentifier("user-data"));
        assertEquals("\"user name\"", PostgresTableNameFormatter.escapeIdentifier("user name"));
        assertEquals("\"123table\"", PostgresTableNameFormatter.escapeIdentifier("123table"));
        
        // Test reserved keywords
        assertEquals("\"select\"", PostgresTableNameFormatter.escapeIdentifier("select"));
        assertEquals("\"from\"", PostgresTableNameFormatter.escapeIdentifier("from"));
        assertEquals("\"table\"", PostgresTableNameFormatter.escapeIdentifier("table"));
        
        // Test already escaped identifiers
        assertEquals("\"already_escaped\"", PostgresTableNameFormatter.escapeIdentifier("\"already_escaped\""));
    }

    @Test
    void testErrorHandling() {
        // Test empty table list
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.formatTableList("public", "");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.formatTableList("public", null);
        });
        
        // Test empty schema name
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.formatTableList("", "users");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.formatTableList(null, "users");
        });
        
        // Test invalid table name format
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.parseTableId("invalid_format");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.parseTableId("schema.table.extra");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            PostgresTableNameFormatter.parseTableId("");
        });
    }

    @Test
    void testRealDatabaseTableNames() throws Exception {
        // Test with actual database tables to ensure formatting works with real PostgreSQL
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
             Statement stmt = conn.createStatement()) {
            
            // Test that our formatted table names work with actual database queries
            String[] formatted = PostgresTableNameFormatter.formatTableList("public", "test_users,test_orders");
            
            for (String tableName : formatted) {
                TableId tableId = PostgresTableNameFormatter.parseTableId(tableName);
                
                // Verify we can query the table (this tests that our formatting is correct)
                String query = String.format("SELECT COUNT(*) FROM %s.%s", 
                        PostgresTableNameFormatter.escapeIdentifier(tableId.getSchemaName()),
                        PostgresTableNameFormatter.escapeIdentifier(tableId.getTableName()));
                
                // This should not throw an exception if the table exists and formatting is correct
                stmt.executeQuery(query);
            }
        }
    }

    @Test
    void testMultiSchemaFormatting() throws Exception {
        // Test formatting with multiple schemas
        String[] formatted = PostgresTableNameFormatter.formatTableList("public", 
                "test_users,inventory.test_products,analytics.test_reports");
        
        assertEquals(3, formatted.length);
        assertEquals("public.test_users", formatted[0]);
        assertEquals("inventory.test_products", formatted[1]);
        assertEquals("analytics.test_reports", formatted[2]);
        
        // Verify each table can be parsed correctly
        for (String tableName : formatted) {
            TableId tableId = PostgresTableNameFormatter.parseTableId(tableName);
            assertNotNull(tableId.getSchemaName());
            assertNotNull(tableId.getTableName());
            assertFalse(tableId.getSchemaName().isEmpty());
            assertFalse(tableId.getTableName().isEmpty());
        }
    }

    private void setupTestDatabase() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
             Statement stmt = conn.createStatement()) {
            
            // Create additional schemas for testing
            stmt.execute("CREATE SCHEMA IF NOT EXISTS inventory");
            stmt.execute("CREATE SCHEMA IF NOT EXISTS analytics");
            
            // Create test tables in different schemas
            stmt.execute("CREATE TABLE IF NOT EXISTS public.test_users (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name VARCHAR(100) NOT NULL)");
            
            stmt.execute("CREATE TABLE IF NOT EXISTS public.test_orders (" +
                    "id SERIAL PRIMARY KEY, " +
                    "user_id INTEGER REFERENCES public.test_users(id))");
            
            stmt.execute("CREATE TABLE IF NOT EXISTS inventory.test_products (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name VARCHAR(255) NOT NULL, " +
                    "price DECIMAL(10,2))");
            
            stmt.execute("CREATE TABLE IF NOT EXISTS analytics.test_reports (" +
                    "id SERIAL PRIMARY KEY, " +
                    "report_name VARCHAR(255) NOT NULL, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            
            // Insert some test data
            stmt.execute("INSERT INTO public.test_users (name) VALUES ('Test User 1'), ('Test User 2')");
            stmt.execute("INSERT INTO public.test_orders (user_id) VALUES (1), (2)");
            stmt.execute("INSERT INTO inventory.test_products (name, price) VALUES ('Product 1', 99.99), ('Product 2', 149.99)");
            stmt.execute("INSERT INTO analytics.test_reports (report_name) VALUES ('Monthly Report'), ('Weekly Report')");
        }
    }
}
