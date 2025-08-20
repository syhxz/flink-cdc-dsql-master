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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Configuration model for PostgreSQL connector tests.
 * Provides builders for different test scenarios and easy conversion to Flink Configuration.
 */
public class TestConfiguration {
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final String databaseName;
    private final String schemaName;
    private final List<String> tables;
    private final String slotName;
    private final String startupMode;
    private final Long timestampMillis;
    private final String specificOffset;
    
    private TestConfiguration(Builder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
        this.databaseName = builder.databaseName;
        this.schemaName = builder.schemaName;
        this.tables = builder.tables;
        this.slotName = builder.slotName;
        this.startupMode = builder.startupMode;
        this.timestampMillis = builder.timestampMillis;
        this.specificOffset = builder.specificOffset;
    }
    
    /**
     * Creates a default test configuration with standard values.
     * 
     * @return default test configuration
     */
    public static TestConfiguration defaultConfig() {
        return new Builder()
                .hostname("localhost")
                .port(5432)
                .username("testuser")
                .password("testpass")
                .databaseName("testdb")
                .schemaName("public")
                .tables(Arrays.asList("users", "orders", "products"))
                .slotName("test_slot")
                .startupMode("snapshot")
                .build();
    }
    
    /**
     * Creates a test configuration from a running PostgreSQL container.
     * 
     * @param container the PostgreSQL container
     * @return test configuration based on container settings
     */
    public static TestConfiguration fromContainer(PostgreSQLContainer<?> container) {
        return new Builder()
                .hostname(container.getHost())
                .port(container.getMappedPort(5432))
                .username(container.getUsername())
                .password(container.getPassword())
                .databaseName(container.getDatabaseName())
                .schemaName("public")
                .tables(Arrays.asList("users", "orders", "products"))
                .slotName("test_slot")
                .startupMode("snapshot")
                .build();
    }
    
    /**
     * Creates a test configuration from a TestContainerManager.
     * 
     * @param containerManager the container manager
     * @return test configuration based on container manager settings
     */
    public static TestConfiguration fromContainerManager(TestContainerManager containerManager) {
        return new Builder()
                .hostname(containerManager.getHost())
                .port(containerManager.getMappedPort())
                .username(containerManager.getUsername())
                .password(containerManager.getPassword())
                .databaseName(containerManager.getDatabaseName())
                .schemaName("public")
                .tables(Arrays.asList("users", "orders", "products"))
                .slotName("test_slot")
                .startupMode("snapshot")
                .build();
    }
    
    /**
     * Creates a minimal test configuration for faster unit tests.
     * 
     * @return minimal test configuration
     */
    public static TestConfiguration minimalConfig() {
        return new Builder()
                .hostname("localhost")
                .port(5432)
                .username("testuser")
                .password("testpass")
                .databaseName("testdb")
                .schemaName("public")
                .tables(Arrays.asList("users"))
                .slotName("test_slot")
                .startupMode("snapshot")
                .build();
    }
    
    /**
     * Converts this test configuration to a Flink Configuration object.
     * 
     * @return Flink Configuration with all settings applied
     */
    public Configuration toFlinkConfiguration() {
        Configuration config = new Configuration();
        config.set(PostgresDataSourceFactory.HOSTNAME, hostname);
        config.set(PostgresDataSourceFactory.PORT, port);
        config.set(PostgresDataSourceFactory.USERNAME, username);
        config.set(PostgresDataSourceFactory.PASSWORD, password);
        config.set(PostgresDataSourceFactory.DATABASE_NAME, databaseName);
        config.set(PostgresDataSourceFactory.SCHEMA_NAME, schemaName);
        config.set(PostgresDataSourceFactory.TABLES, String.join(",", tables));
        config.set(PostgresDataSourceFactory.SLOT_NAME, slotName);
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, startupMode);
        
        if (timestampMillis != null) {
            config.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, timestampMillis);
        }
        
        if (specificOffset != null) {
            config.set(PostgresDataSourceFactory.SCAN_STARTUP_SPECIFIC_OFFSET, specificOffset);
        }
        
        return config;
    }
    
    /**
     * Creates a new builder for constructing test configurations.
     * 
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Creates a builder initialized with values from this configuration.
     * 
     * @return builder with current configuration values
     */
    public Builder toBuilder() {
        return new Builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseName(databaseName)
                .schemaName(schemaName)
                .tables(tables)
                .slotName(slotName)
                .startupMode(startupMode)
                .timestampMillis(timestampMillis)
                .specificOffset(specificOffset);
    }
    
    // Getters
    public String getHostname() { return hostname; }
    public int getPort() { return port; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public List<String> getTables() { return tables; }
    public String getSlotName() { return slotName; }
    public String getStartupMode() { return startupMode; }
    public Long getTimestampMillis() { return timestampMillis; }
    public String getSpecificOffset() { return specificOffset; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestConfiguration that = (TestConfiguration) o;
        return port == that.port &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password) &&
                Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tables, that.tables) &&
                Objects.equals(slotName, that.slotName) &&
                Objects.equals(startupMode, that.startupMode) &&
                Objects.equals(timestampMillis, that.timestampMillis) &&
                Objects.equals(specificOffset, that.specificOffset);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(hostname, port, username, password, databaseName, 
                          schemaName, tables, slotName, startupMode, timestampMillis, specificOffset);
    }
    
    @Override
    public String toString() {
        return "TestConfiguration{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", schemaName='" + schemaName + '\'' +
                ", tables=" + tables +
                ", slotName='" + slotName + '\'' +
                ", startupMode='" + startupMode + '\'' +
                ", timestampMillis=" + timestampMillis +
                ", specificOffset='" + specificOffset + '\'' +
                '}';
    }
    
    /**
     * Builder class for constructing TestConfiguration instances.
     */
    public static class Builder {
        private String hostname = "localhost";
        private int port = 5432;
        private String username = "testuser";
        private String password = "testpass";
        private String databaseName = "testdb";
        private String schemaName = "public";
        private List<String> tables = Arrays.asList("users", "orders", "products");
        private String slotName = "test_slot";
        private String startupMode = "snapshot";
        private Long timestampMillis;
        private String specificOffset;
        
        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }
        
        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }
        
        public Builder tables(List<String> tables) {
            this.tables = tables;
            return this;
        }
        
        public Builder tables(String... tables) {
            this.tables = Arrays.asList(tables);
            return this;
        }
        
        public Builder slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }
        
        public Builder startupMode(String startupMode) {
            this.startupMode = startupMode;
            return this;
        }
        
        public Builder timestampMillis(Long timestampMillis) {
            this.timestampMillis = timestampMillis;
            return this;
        }
        
        public Builder specificOffset(String specificOffset) {
            this.specificOffset = specificOffset;
            return this;
        }
        
        /**
         * Configures the builder for snapshot startup mode.
         * 
         * @return this builder
         */
        public Builder snapshotMode() {
            this.startupMode = "snapshot";
            this.timestampMillis = null;
            this.specificOffset = null;
            return this;
        }
        
        /**
         * Configures the builder for latest-offset startup mode.
         * 
         * @return this builder
         */
        public Builder latestOffsetMode() {
            this.startupMode = "latest-offset";
            this.timestampMillis = null;
            this.specificOffset = null;
            return this;
        }
        
        /**
         * Configures the builder for timestamp startup mode.
         * 
         * @param timestampMillis the timestamp in milliseconds
         * @return this builder
         */
        public Builder timestampMode(long timestampMillis) {
            this.startupMode = "timestamp";
            this.timestampMillis = timestampMillis;
            this.specificOffset = null;
            return this;
        }
        
        /**
         * Configures the builder for specific-offset startup mode.
         * 
         * @param specificOffset the specific offset
         * @return this builder
         */
        public Builder specificOffsetMode(String specificOffset) {
            this.startupMode = "specific-offset";
            this.specificOffset = specificOffset;
            this.timestampMillis = null;
            return this;
        }
        
        /**
         * Builds the TestConfiguration instance.
         * 
         * @return new TestConfiguration instance
         */
        public TestConfiguration build() {
            return new TestConfiguration(this);
        }
    }
}