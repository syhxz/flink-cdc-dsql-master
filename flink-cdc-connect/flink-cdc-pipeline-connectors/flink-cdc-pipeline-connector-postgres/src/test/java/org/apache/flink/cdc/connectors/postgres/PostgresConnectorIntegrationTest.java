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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.utils.TestCategories;
import org.apache.flink.cdc.connectors.postgres.utils.TestConfiguration;
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

/** Integration tests for PostgreSQL pipeline connector. */
@TestCategories.IntegrationTest
@TestCategories.DockerRequired
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresConnectorIntegrationTest {

    private TestContainerManager containerManager;
    private PostgresDataSourceFactory factory;
    private String jdbcUrl;
    private Properties connectionProps;

    @BeforeAll
    void setUp() throws Exception {
        containerManager = new TestContainerManager();
        
        // This will gracefully skip the test if Docker is not available
        PostgreSQLContainer<?> container = containerManager.getOrCreateContainer();
        
        factory = new PostgresDataSourceFactory();
        jdbcUrl = containerManager.getJdbcUrl();
        connectionProps = new Properties();
        connectionProps.setProperty("user", containerManager.getUsername());
        connectionProps.setProperty("password", containerManager.getPassword());

        // Set up test database schema and data using TestDataManager
        setupTestDatabase();
    }

    @AfterAll
    void tearDown() {
        if (containerManager != null) {
            containerManager.cleanup();
        }
    }

    @Test
    void testBasicConnectorFunctionality() throws Exception {
        Configuration config = createBasicConfiguration();
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // Test table discovery
        List<TableId> tables = dataSource.getMetadataAccessor().listTables(null, null);
        assertFalse(tables.isEmpty());
        assertTrue(tables.stream().anyMatch(t -> "users".equals(t.getTableName())));
        
        // Test schema retrieval
        TableId usersTable = tables.stream()
                .filter(t -> "users".equals(t.getTableName()))
                .findFirst()
                .orElseThrow();
        
        Schema schema = dataSource.getMetadataAccessor().getTableSchema(usersTable);
        assertNotNull(schema);
        assertFalse(schema.getColumns().isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testTableDiscoveryAndSchemaRetrieval() throws Exception {
        Configuration config = createBasicConfiguration();
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        // Test table discovery
        List<TableId> tables = dataSource.getMetadataAccessor().listTables(null, null);
        assertEquals(3, tables.size()); // users, orders, products
        
        // Verify all expected tables are discovered
        assertTrue(tables.stream().anyMatch(t -> "users".equals(t.getTableName())));
        assertTrue(tables.stream().anyMatch(t -> "orders".equals(t.getTableName())));
        assertTrue(tables.stream().anyMatch(t -> "products".equals(t.getTableName())));
        
        // Test schema retrieval for each table
        for (TableId tableId : tables) {
            Schema schema = dataSource.getMetadataAccessor().getTableSchema(tableId);
            assertNotNull(schema);
            assertFalse(schema.getColumns().isEmpty());
            
            if ("users".equals(tableId.getTableName())) {
                assertEquals(4, schema.getColumns().size()); // id, name, email, created_at
                assertTrue(schema.primaryKeys().contains("id"));
            }
        }
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testDifferentStartupModes() throws Exception {
        // Test snapshot mode
        Configuration snapshotConfig = createBasicConfiguration();
        snapshotConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        
        DataSourceFactory.Context context = createContext(snapshotConfig);
        DataSource dataSource = factory.createDataSource(context);
        assertNotNull(dataSource);
        // DataSource doesn't have close method - resources are managed by the framework
        
        // Test latest-offset mode
        Configuration latestConfig = createBasicConfiguration();
        latestConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "latest-offset");
        
        context = createContext(latestConfig);
        dataSource = factory.createDataSource(context);
        assertNotNull(dataSource);
        // DataSource doesn't have close method - resources are managed by the framework
        
        // Test timestamp mode
        Configuration timestampConfig = createBasicConfiguration();
        timestampConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        timestampConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, System.currentTimeMillis());
        
        context = createContext(timestampConfig);
        dataSource = factory.createDataSource(context);
        assertNotNull(dataSource);
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testMultipleTableConfigurations() throws Exception {
        // Test single table
        Configuration singleTableConfig = createBasicConfiguration();
        singleTableConfig.set(PostgresDataSourceFactory.TABLES, "users");
        
        DataSourceFactory.Context context = createContext(singleTableConfig);
        DataSource dataSource = factory.createDataSource(context);
        
        List<TableId> tables = dataSource.getMetadataAccessor().listTables(null, null);
        assertEquals(1, tables.size());
        assertEquals("users", tables.get(0).getTableName());
        // DataSource doesn't have close method - resources are managed by the framework
        
        // Test multiple tables
        Configuration multiTableConfig = createBasicConfiguration();
        multiTableConfig.set(PostgresDataSourceFactory.TABLES, "users,orders");
        
        context = createContext(multiTableConfig);
        dataSource = factory.createDataSource(context);
        
        tables = dataSource.getMetadataAccessor().listTables(null, null);
        assertEquals(2, tables.size());
        assertTrue(tables.stream().anyMatch(t -> "users".equals(t.getTableName())));
        assertTrue(tables.stream().anyMatch(t -> "orders".equals(t.getTableName())));
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testSchemaTableNamingConventions() throws Exception {
        // Test with explicit schema
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.TABLES, "public.users,public.orders");
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        List<TableId> tables = dataSource.getMetadataAccessor().listTables(null, null);
        assertEquals(2, tables.size());
        
        for (TableId tableId : tables) {
            assertEquals("public", tableId.getSchemaName());
        }
        
        // DataSource doesn't have close method - resources are managed by the framework
        
        // Test with mixed naming (some with schema, some without)
        config.set(PostgresDataSourceFactory.TABLES, "users,public.orders");
        
        context = createContext(config);
        dataSource = factory.createDataSource(context);
        
        tables = dataSource.getMetadataAccessor().listTables(null, null);
        assertEquals(2, tables.size());
        
        // All should have public schema
        for (TableId tableId : tables) {
            assertEquals("public", tableId.getSchemaName());
        }
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testErrorScenarios() {
        // Test missing table
        Configuration missingTableConfig = createBasicConfiguration();
        missingTableConfig.set(PostgresDataSourceFactory.TABLES, "nonexistent_table");
        
        final DataSourceFactory.Context missingTableContext = createContext(missingTableConfig);
        
        assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(missingTableContext);
        });
        
        // Test invalid connection parameters
        Configuration invalidConfig = createBasicConfiguration();
        invalidConfig.set(PostgresDataSourceFactory.HOSTNAME, "invalid-host");
        
        final DataSourceFactory.Context invalidContext = createContext(invalidConfig);
        
        assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(invalidContext);
        });
        
        // Test invalid startup mode configuration
        Configuration invalidStartupConfig = createBasicConfiguration();
        invalidStartupConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "specific-offset");
        // Missing scan.startup.specific-offset
        
        final DataSourceFactory.Context invalidStartupContext = createContext(invalidStartupConfig);
        
        assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(invalidStartupContext);
        });
    }

    @Test
    void testConnectionFailureHandling() {
        // Test with wrong port
        Configuration portConfig = createBasicConfiguration();
        portConfig.set(PostgresDataSourceFactory.PORT, 9999);
        
        final DataSourceFactory.Context portContext = createContext(portConfig);
        
        assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(portContext);
        });
        
        // Test with wrong credentials
        Configuration credentialsConfig = createBasicConfiguration();
        credentialsConfig.set(PostgresDataSourceFactory.USERNAME, "wronguser");
        credentialsConfig.set(PostgresDataSourceFactory.PASSWORD, "wrongpass");
        
        final DataSourceFactory.Context credentialsContext = createContext(credentialsConfig);
        
        assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(credentialsContext);
        });
    }

    private Configuration createBasicConfiguration() {
        TestConfiguration testConfig = TestConfiguration.fromContainerManager(containerManager);
        return testConfig.toFlinkConfiguration();
    }

    private DataSourceFactory.Context createContext(Configuration config) {
        return new DataSourceFactory.Context() {
            @Override
            public Configuration getFactoryConfiguration() {
                return config;
            }

            @Override
            public Configuration getPipelineConfiguration() {
                return new Configuration();
            }

            @Override
            public ClassLoader getClassLoader() {
                return Thread.currentThread().getContextClassLoader();
            }
        };
    }

    private void setupTestDatabase() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            TestDataManager dataManager = new TestDataManager(conn);
            
            // Set up standard test schema and insert test data
            dataManager.setupTestSchema();
            dataManager.insertTestData();
        }
    }
}

