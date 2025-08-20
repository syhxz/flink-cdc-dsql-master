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

package org.apache.flink.cdc.connectors.dsql.sink;

import org.apache.flink.cdc.common.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.DATABASE;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.HOST;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.PORT;

/**
 * Provider for Amazon DSQL sink connections.
 * Handles basic JDBC connection management for DSQL.
 */
public class DsqlSinkProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlSinkProvider.class);

    private final Configuration config;
    private final String jdbcUrl;
    private final Properties connectionProperties;

    public DsqlSinkProvider(Configuration config) {
        this.config = config;
        this.jdbcUrl = buildJdbcUrl();
        this.connectionProperties = buildConnectionProperties();

        LOG.info("Initialized DSQL sink provider with URL: {}", jdbcUrl);
    }

    /**
     * Creates a new connection to the DSQL database.
     *
     * @return A new database connection
     * @throws SQLException if connection fails
     */
    public Connection createConnection() throws SQLException {
        try {
            // Load PostgreSQL driver (DSQL is PostgreSQL-compatible)
            Class.forName("org.postgresql.Driver");

            Connection connection = DriverManager.getConnection(jdbcUrl, connectionProperties);

            // Set connection properties for DSQL
            connection.setAutoCommit(false); // Use transactions

            LOG.debug("Successfully created DSQL connection");
            return connection;

        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC driver not found", e);
        } catch (SQLException e) {
            LOG.error("Failed to create DSQL connection: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Tests the connection to ensure it's working.
     *
     * @return true if connection is successful
     */
    public boolean testConnection() {
        try (Connection connection = createConnection()) {
            // Simple test query
            return connection.isValid(5); // 5 second timeout
        } catch (SQLException e) {
            LOG.error("Connection test failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Builds the JDBC URL for DSQL connection.
     */
    private String buildJdbcUrl() {
        String host = config.get(HOST);
        Integer port = config.get(PORT);
        String database = config.get(DATABASE);

        return String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    }

    /**
     * Builds connection properties for DSQL.
     */
    private Properties buildConnectionProperties() {
        Properties props = new Properties();

        // Configure authentication using the authenticator
        org.apache.flink.cdc.connectors.dsql.auth.DsqlAuthenticator authenticator =
            new org.apache.flink.cdc.connectors.dsql.auth.DsqlAuthenticator(config);
        authenticator.configureAuthentication(props);

        // DSQL-specific properties
        props.setProperty("ssl", "true");
        props.setProperty("sslmode", "require");

        // Connection timeout settings
        props.setProperty("connectTimeout", "30");
        props.setProperty("socketTimeout", "60");

        return props;
    }

    /**
     * Gets the configuration for this sink provider.
     */
    public Configuration getConfig() {
        return config;
    }
}
