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

/** Integration tests for PostgreSQL startup options functionality. */
@TestCategories.IntegrationTest
@TestCategories.DockerRequired
@TestCategories.StartupOptionsTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresStartupOptionsIntegrationTest {

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
    void testSnapshotStartupMode() throws Exception {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // Verify that the data source was created successfully with snapshot mode
        assertFalse(dataSource.getMetadataAccessor().listTables(null, null).isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testLatestOffsetStartupMode() throws Exception {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "latest-offset");
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // Verify that the data source was created successfully with latest-offset mode
        assertFalse(dataSource.getMetadataAccessor().listTables(null, null).isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testTimestampStartupMode() throws Exception {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, System.currentTimeMillis() - 3600000L); // 1 hour ago
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // Verify that the data source was created successfully with timestamp mode
        assertFalse(dataSource.getMetadataAccessor().listTables(null, null).isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testSpecificOffsetStartupMode() throws Exception {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "specific-offset");
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_SPECIFIC_OFFSET, "0/0"); // PostgreSQL LSN format
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // Verify that the data source was created successfully with specific-offset mode
        assertFalse(dataSource.getMetadataAccessor().listTables(null, null).isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testDefaultStartupMode() throws Exception {
        Configuration config = createBasicConfiguration();
        // Don't set startup mode - should default to snapshot
        
        DataSourceFactory.Context context = createContext(config);
        DataSource dataSource = factory.createDataSource(context);
        
        assertNotNull(dataSource);
        
        // Verify that the data source was created successfully with default mode
        assertFalse(dataSource.getMetadataAccessor().listTables(null, null).isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testInvalidStartupMode() {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "invalid-mode");
        
        DataSourceFactory.Context context = createContext(config);
        
        assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(context);
        });
    }

    @Test
    void testSpecificOffsetWithoutOffset() {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "specific-offset");
        // Don't set the specific offset - should fail
        
        DataSourceFactory.Context context = createContext(config);
        
        assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(context);
        });
    }

    @Test
    void testTimestampWithoutTimestamp() {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        // Don't set the timestamp - should fail
        
        DataSourceFactory.Context context = createContext(config);
        
        assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(context);
        });
    }

    @Test
    void testTimestampWithInvalidTimestamp() {
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, -1L); // Invalid negative timestamp
        
        DataSourceFactory.Context context = createContext(config);
        
        assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(context);
        });
    }

    @Test
    void testCaseInsensitiveStartupModes() throws Exception {
        // Test uppercase
        Configuration config1 = createBasicConfiguration();
        config1.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "SNAPSHOT");
        
        DataSourceFactory.Context context1 = createContext(config1);
        DataSource dataSource1 = factory.createDataSource(context1);
        assertNotNull(dataSource1);
        // DataSource doesn't have close method - resources are managed by the framework
        
        // Test mixed case
        Configuration config2 = createBasicConfiguration();
        config2.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "Latest-Offset");
        
        DataSourceFactory.Context context2 = createContext(config2);
        DataSource dataSource2 = factory.createDataSource(context2);
        assertNotNull(dataSource2);
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testStartupModeWithDifferentTableConfigurations() throws Exception {
        // Test snapshot mode with single table
        Configuration config1 = createBasicConfiguration();
        config1.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        config1.set(PostgresDataSourceFactory.TABLES, "users");
        
        DataSourceFactory.Context context1 = createContext(config1);
        DataSource dataSource1 = factory.createDataSource(context1);
        assertNotNull(dataSource1);
        assertEquals(1, dataSource1.getMetadataAccessor().listTables(null, null).size());
        // DataSource doesn't have close method - resources are managed by the framework
        
        // Test latest-offset mode with multiple tables
        Configuration config2 = createBasicConfiguration();
        config2.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "latest-offset");
        config2.set(PostgresDataSourceFactory.TABLES, "users,orders");
        
        DataSourceFactory.Context context2 = createContext(config2);
        DataSource dataSource2 = factory.createDataSource(context2);
        assertNotNull(dataSource2);
        assertEquals(2, dataSource2.getMetadataAccessor().listTables(null, null).size());
        // DataSource doesn't have close method - resources are managed by the framework
    }

    @Test
    void testStartupModeValidationBeforeConnection() {
        // Test that startup mode validation happens before attempting database connection
        Configuration config = createBasicConfiguration();
        config.set(PostgresDataSourceFactory.HOSTNAME, "invalid-host"); // This would cause connection failure
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "invalid-mode"); // But this should fail first
        
        DataSourceFactory.Context context = createContext(config);
        
        // Should fail with IllegalArgumentException for invalid startup mode,
        // not with connection error
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            factory.createDataSource(context);
        });
        
        assertTrue(exception.getMessage().contains("startup mode") || 
                   exception.getMessage().contains("Unsupported startup mode"));
    }

    @Test
    void testMultipleStartupModeCreations() throws Exception {
        // Test creating multiple data sources with different startup modes
        // to ensure no state is shared between creations
        
        Configuration snapshotConfig = createBasicConfiguration();
        snapshotConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        
        Configuration latestConfig = createBasicConfiguration();
        latestConfig.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "latest-offset");
        
        // Create first data source
        DataSourceFactory.Context context1 = createContext(snapshotConfig);
        DataSource dataSource1 = factory.createDataSource(context1);
        assertNotNull(dataSource1);
        
        // Create second data source with different startup mode
        DataSourceFactory.Context context2 = createContext(latestConfig);
        DataSource dataSource2 = factory.createDataSource(context2);
        assertNotNull(dataSource2);
        
        // Both should work independently
        assertFalse(dataSource1.getMetadataAccessor().listTables(null, null).isEmpty());
        assertFalse(dataSource2.getMetadataAccessor().listTables(null, null).isEmpty());
        
        // DataSource doesn't have close method - resources are managed by the framework
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
