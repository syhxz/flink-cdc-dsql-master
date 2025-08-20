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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** Utility class for PostgreSQL configuration validation. */
@Internal
public class PostgresValidationUtils {

    /**
     * Validate PostgreSQL configuration parameters.
     *
     * @param config the configuration to validate
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateConfiguration(Configuration config) {
        // Validate required parameters
        validateRequiredParameter(config.get(PostgresDataSourceFactory.HOSTNAME), "hostname");
        validateRequiredParameter(config.get(PostgresDataSourceFactory.USERNAME), "username");
        validateRequiredParameter(config.get(PostgresDataSourceFactory.PASSWORD), "password");
        validateRequiredParameter(config.get(PostgresDataSourceFactory.DATABASE_NAME), "database-name");
        validateRequiredParameter(config.get(PostgresDataSourceFactory.TABLES), "tables");

        // Validate port range
        int port = config.get(PostgresDataSourceFactory.PORT);
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535, but was: " + port);
        }

        // Validate schema name
        String schemaName = config.get(PostgresDataSourceFactory.SCHEMA_NAME);
        if (!PostgresTableNameFormatter.isValidSchemaName(schemaName)) {
            throw new IllegalArgumentException(
                    "Invalid schema name: " + schemaName +
                    ". Schema name must follow PostgreSQL identifier rules.");
        }

        // Validate table names
        String tables = config.get(PostgresDataSourceFactory.TABLES);
        validateTableNames(schemaName, tables);

        // Validate slot name
        String slotName = config.get(PostgresDataSourceFactory.SLOT_NAME);
        if (!isValidSlotName(slotName)) {
            throw new IllegalArgumentException(
                    "Invalid slot name: " + slotName +
                    ". Slot name must contain only lowercase letters, numbers, and underscores.");
        }

        // Validate startup mode configuration
        validateStartupModeConfiguration(config);
    }

    /**
     * Test PostgreSQL database connectivity.
     *
     * @param config the configuration to test
     * @throws SQLException if connection fails
     */
    public static void testConnection(Configuration config) throws SQLException {
        String hostname = config.get(PostgresDataSourceFactory.HOSTNAME);
        int port = config.get(PostgresDataSourceFactory.PORT);
        String username = config.get(PostgresDataSourceFactory.USERNAME);
        String password = config.get(PostgresDataSourceFactory.PASSWORD);
        String databaseName = config.get(PostgresDataSourceFactory.DATABASE_NAME);

        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("connectTimeout", "10"); // 10 seconds timeout

        String jdbcUrl = String.format(
                "jdbc:postgresql://%s:%d/%s",
                hostname, port, databaseName);

        // Explicitly register the PostgreSQL JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC driver not found in classpath", e);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
            // Test basic connectivity
            if (!connection.isValid(5)) {
                throw new SQLException("Connection is not valid");
            }
        } catch (SQLException e) {
            throw new SQLException(
                    "Failed to connect to PostgreSQL database at " + hostname + ":" + port + 
                    "/" + databaseName + ". Error: " + e.getMessage(), e);
        }
    }

    private static void validateRequiredParameter(String value, String parameterName) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            throw new IllegalArgumentException(
                    "Required parameter '" + parameterName + "' is missing or empty");
        }
    }

    private static void validateTableNames(String schemaName, String tables) {
        try {
            String[] tableList = PostgresTableNameFormatter.formatTableList(schemaName, tables);
            
            for (String table : tableList) {
                // Parse and validate each table name
                PostgresTableNameFormatter.parseTableId(table);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid table configuration: " + tables + ". Error: " + e.getMessage(), e);
        }
    }

    private static boolean isValidSlotName(String slotName) {
        if (StringUtils.isNullOrWhitespaceOnly(slotName)) {
            return false;
        }

        // PostgreSQL slot names must contain only lowercase letters, numbers, and underscores
        return slotName.matches("^[a-z0-9_]+$");
    }

    private static void validateStartupModeConfiguration(Configuration config) {
        String startupMode = config.get(PostgresDataSourceFactory.SCAN_STARTUP_MODE);
        
        switch (startupMode.toLowerCase()) {
            case "snapshot":
            case "latest-offset":
                // No additional validation needed
                break;
            case "specific-offset":
                String offset = config.get(PostgresDataSourceFactory.SCAN_STARTUP_SPECIFIC_OFFSET);
                if (StringUtils.isNullOrWhitespaceOnly(offset)) {
                    throw new IllegalArgumentException(
                            "scan.startup.specific-offset must be specified when using specific-offset mode");
                }
                break;
            case "timestamp":
                Long timestamp = config.get(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS);
                if (timestamp == null || timestamp <= 0) {
                    throw new IllegalArgumentException(
                            "scan.startup.timestamp-millis must be a positive number when using timestamp mode");
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode: " + startupMode + 
                        ". Supported modes are: snapshot, latest-offset, specific-offset, timestamp");
        }
    }
}