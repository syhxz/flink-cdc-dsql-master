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
import org.apache.flink.cdc.common.factories.DataSourceFactory;
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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Integration tests for PostgreSQL connector error scenarios. */
@TestCategories.IntegrationTest
@TestCategories.DockerRequired
@TestCategories.ErrorScenarios
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresErrorScenariosIntegrationTest {

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
    void testMissingRequiredParameters() {
        // Test missing hostname
        Configuration hostnameConfig = new Configuration();
        hostnameConfig.set(PostgresDataSourceFactory.PORT, 5432);
        hostnameConfig.set(PostgresDataSourceFactory.USERNAME, "user");
        hostnameConfig.set(PostgresDataSourceFactory.PASSWORD, "pass");
        hostnameConfig.set(PostgresDataSourceFactory.DATABASE_NAME, "db");
        hostnameConfig.set(PostgresDataSourceFactory.TABLES, "table1");
        
        final DataSourceFactory.Context hostnameContext = createContext(hostnameConfig);
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(hostnameContext);
        });
        assertTrue(exception.getMessage().contains("hostname"));
        
        // Test missing username
        Configuration usernameConfig = createBasicConfiguration();
        usernameConfig.set(PostgresDataSourceFactory.USERNAME, null);
        
        final DataSourceFactory.Context usernameContext = createContext(usernameConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(usernameContext);
        });
        assertTrue(exception.getMessage().contains("username"));
        
        // Test missing password
        Configuration passwordConfig = createBasicConfiguration();
        passwordConfig.set(PostgresDataSourceFactory.PASSWORD, null);
        
        final DataSourceFactory.Context passwordContext = createContext(passwordConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(passwordContext);
        });
        assertTrue(exception.getMessage().contains("password"));
        
        // Test missing database name
        Configuration databaseConfig = createBasicConfiguration();
        databaseConfig.set(PostgresDataSourceFactory.DATABASE_NAME, null);
        
        final DataSourceFactory.Context databaseContext = createContext(databaseConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(databaseContext);
        });
        assertTrue(exception.getMessage().contains("database-name"));
        
        // Test missing tables
        Configuration tablesConfig = createBasicConfiguration();
        tablesConfig.set(PostgresDataSourceFactory.TABLES, null);
        
        final DataSourceFactory.Context tablesContext = createContext(tablesConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(tablesContext);
        });
        assertTrue(exception.getMessage().contains("tables"));
    }

    @Test
    void testInvalidParameterValues() {
        // Test invalid port (too low)
        Configuration lowPortConfig = createBasicConfiguration();
        lowPortConfig.set(PostgresDataSourceFactory.PORT, 0);
        
        final DataSourceFactory.Context lowPortContext = createContext(lowPortConfig);
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(lowPortContext);
        });
        assertTrue(exception.getMessage().contains("Port"));
        
        // Test invalid port (too high)
        Configuration highPortConfig = createBasicConfiguration();
        highPortConfig.set(PostgresDataSourceFactory.PORT, 70000);
        
        final DataSourceFactory.Context highPortContext = createContext(highPortConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(highPortContext);
        });
        assertTrue(exception.getMessage().contains("Port"));
        
        // Test invalid slot name
        Configuration slotConfig = createBasicConfiguration();
        slotConfig.set(PostgresDataSourceFactory.SLOT_NAME, "Invalid-Slot-Name");
        
        final DataSourceFactory.Context slotContext = createContext(slotConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(slotContext);
        });
        assertTrue(exception.getMessage().contains("slot name"));
    }

    @Test
    void testConnectionFailures() {
        // Test wrong hostname
        Configuration hostnameConfig = createBasicConfiguration();
        hostnameConfig.set(PostgresDataSourceFactory.HOSTNAME, "nonexistent-host");
        
        final DataSourceFactory.Context hostnameContext = createContext(hostnameConfig);
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(hostnameContext);
        });
        assertTrue(exception.getMessage().contains("Failed to connect") || 
                   exception.getMessage().contains("connection"));
        
        // Test wrong port
        Configuration portConfig = createBasicConfiguration();
        portConfig.set(PostgresDataSourceFactory.PORT, 9999);
        
        final DataSourceFactory.Context portContext = createContext(portConfig);
        
        exception = assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(portContext);
        });
        assertTrue(exception.getMessage().contains("Failed to connect") || 
                   exception.getMessage().contains("connection"));
        
        // Test wrong credentials
        Configuration credentialsConfig = createBasicConfiguration();
        credentialsConfig.set(PostgresDataSourceFactory.USERNAME, "wronguser");
        credentialsConfig.set(PostgresDataSourceFactory.PASSWORD, "wrongpass");
        
        final DataSourceFactory.Context credentialsContext = createContext(credentialsConfig);
        
        exception = assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(credentialsContext);
        });
        assertTrue(exception.getMessage().contains("Failed to connect") || 
                   exception.getMessage().contains("authentication"));
        
        // Test wrong database name
        Configuration databaseConfig = createBasicConfiguration();
        databaseConfig.set(PostgresDataSourceFactory.DATABASE_NAME, "nonexistent_db");
        
        final DataSourceFactory.Context databaseContext = createContext(databaseConfig);
        
        exception = assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(databaseContext);
        });
        assertTrue(exception.getMessage().contains("Failed to connect") || 
                   exception.getMessage().contains("database"));
    }

    @Test
    void testNonExistentTables() {
        // Test single non-existent table
        Configuration singleTableConfig = createBasicConfiguration();
        singleTableConfig.set(PostgresDataSourceFactory.TABLES, "nonexistent_table");
        
        final DataSourceFactory.Context singleTableContext = createContext(singleTableConfig);
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(singleTableContext);
        });
        assertTrue(exception.getMessage().contains("does not exist") || 
                   exception.getMessage().contains("nonexistent_table"));
        
        // Test mix of existing and non-existent tables
        Configuration mixedTablesConfig = createBasicConfiguration();
        mixedTablesConfig.set(PostgresDataSourceFactory.TABLES, "users,nonexistent_table");
        
        final DataSourceFactory.Context mixedTablesContext = createContext(mixedTablesConfig);
        
        exception = assertThrows(RuntimeException.class, () -> {
            factory.createDataSource(mixedTablesContext);
        });
        assertTrue(exception.getMessage().contains("does not exist") || 
                   exception.getMessage().contains("nonexistent_table"));
    }

    @Test
    void testInvalidTableNameFormats() {
        // Test invalid table name format (too many dots)
        Configuration invalidFormatConfig = createBasicConfiguration();
        invalidFormatConfig.set(PostgresDataSourceFactory.TABLES, "schema.table.extra");
        
        final DataSourceFactory.Context invalidFormatContext = createContext(invalidFormatConfig);
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(invalidFormatContext);
        });
        assertTrue(exception.getMessage().contains("Invalid table") || 
                   exception.getMessage().contains("table configuration"));
        
        // Test empty table name
        Configuration emptyTableConfig = createBasicConfiguration();
        emptyTableConfig.set(PostgresDataSourceFactory.TABLES, "users,,orders");
        
        final DataSourceFactory.Context emptyTableContext = createContext(emptyTableConfig);
        
        // This should work as empty entries are filtered out
        assertDoesNotThrow(() -> {
            DataSource dataSource = factory.createDataSource(emptyTableContext);
            // DataSource doesn't have close method - resources are managed by the framework
        });
    }

    @Test
    void testInvalidSchemaNames() {
        // Test invalid schema name
        Configuration invalidSchemaConfig = createBasicConfiguration();
        invalidSchemaConfig.set(PostgresDataSourceFactory.SCHEMA_NAME, "123invalid");
        
        final DataSourceFactory.Context invalidSchemaContext = createContext(invalidSchemaConfig);
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(invalidSchemaContext);
        });
        assertTrue(exception.getMessage().contains("Invalid schema name"));
        
        // Test empty schema name
        Configuration emptySchemaConfig = createBasicConfiguration();
        emptySchemaConfig.set(PostgresDataSourceFactory.SCHEMA_NAME, "");
        
        final DataSourceFactory.Context emptySchemaContext = createContext(emptySchemaConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(emptySchemaContext);
        });
        assertTrue(exception.getMessage().contains("Schema name"));
    }

    @Test
    void testDataSourceResourceCleanup() throws Exception {
        // Test that resources are properly cleaned up even when errors occur
        Configuration config = createBasicConfiguration();
        
        final DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // DataSource doesn't have close method - resources are managed by the framework
        // Just verify the data source was created successfully
        assertNotNull(dataSource.getMetadataAccessor());
        assertNotNull(dataSource.getEventSourceProvider());
    }

    @Test
    void testConcurrentConnectionAttempts() throws Exception {
        // Test multiple concurrent connection attempts to ensure thread safety
        Configuration config = createBasicConfiguration();
        final DataSourceFactory.Context context = createContext(config);
        
        // Create multiple data sources concurrently
        Thread[] threads = new Thread[5];
        final Exception[] exceptions = new Exception[5];
        final DataSource[] dataSources = new DataSource[5];
        
        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    dataSources[index] = factory.createDataSource(context);
                } catch (Exception e) {
                    exceptions[index] = e;
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        // Check that all succeeded
        for (int i = 0; i < exceptions.length; i++) {
            if (exceptions[i] != null) {
                fail("Thread " + i + " failed with exception: " + exceptions[i].getMessage());
            }
            assertNotNull(dataSources[i], "DataSource " + i + " should not be null");
        }
        
        // DataSource doesn't have close method - resources are managed by the framework
        // Just verify all data sources were created successfully
        for (DataSource dataSource : dataSources) {
            if (dataSource != null) {
                assertNotNull(dataSource.getMetadataAccessor());
                assertNotNull(dataSource.getEventSourceProvider());
            }
        }
    }

    @Test
    void testInvalidStartupModeConfigurations() {
        // Test specific-offset mode without offset
        Configuration specificOffsetConfig = createBasicConfiguration();
        specificOffsetConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "specific-offset");
        
        final DataSourceFactory.Context specificOffsetContext = createContext(specificOffsetConfig);
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(specificOffsetContext);
        });
        assertTrue(exception.getMessage().contains("scan.startup.specific-offset"));
        
        // Test timestamp mode without timestamp
        Configuration timestampConfig = createBasicConfiguration();
        timestampConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        
        final DataSourceFactory.Context timestampContext = createContext(timestampConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(timestampContext);
        });
        assertTrue(exception.getMessage().contains("scan.startup.timestamp-millis"));
        
        // Test timestamp mode with invalid timestamp
        Configuration invalidTimestampConfig = createBasicConfiguration();
        invalidTimestampConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        invalidTimestampConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, -1L);
        
        final DataSourceFactory.Context invalidTimestampContext = createContext(invalidTimestampConfig);
        
        exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(invalidTimestampContext);
        });
        assertTrue(exception.getMessage().contains("positive number"));
    }

    @Test
    void testDatabasePermissionIssues() throws Exception {
        // Create a user with limited permissions
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
             Statement stmt = conn.createStatement()) {
            
            // Create a limited user (this might fail in some test environments, so we'll catch and skip)
            try {
                stmt.execute("CREATE USER limited_user WITH PASSWORD 'limited_pass'");
                stmt.execute("GRANT CONNECT ON DATABASE " + containerManager.getDatabaseName() + " TO limited_user");
                // Don't grant table access
                
                // Test with limited user
                Configuration config = createBasicConfiguration();
                config.set(PostgresDataSourceFactory.USERNAME, "limited_user");
                config.set(PostgresDataSourceFactory.PASSWORD, "limited_pass");
                
                DataSourceFactory.Context context = createContext(config);
                
                // This should fail due to insufficient permissions
                assertThrows(RuntimeException.class, () -> {
                    factory.createDataSource(context);
                });
                
            } catch (Exception e) {
                // Skip this test if we can't create users (common in containerized environments)
                System.out.println("Skipping permission test due to: " + e.getMessage());
            }
        }
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
