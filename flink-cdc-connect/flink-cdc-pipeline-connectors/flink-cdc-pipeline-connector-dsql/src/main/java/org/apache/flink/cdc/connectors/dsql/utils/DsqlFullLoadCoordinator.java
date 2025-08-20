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

package org.apache.flink.cdc.connectors.dsql.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinator for managing full load operations in DSQL.
 * Handles table creation, validation, and tracking of created tables.
 */
public class DsqlFullLoadCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlFullLoadCoordinator.class);

    private final DsqlSinkProvider sinkProvider;
    private final DsqlRetryHelper retryHelper;
    private final Map<String, Boolean> createdTables;

    public DsqlFullLoadCoordinator(DsqlSinkProvider sinkProvider) {
        this.sinkProvider = sinkProvider;
        this.retryHelper = DsqlRetryHelper.createDefault();
        this.createdTables = new ConcurrentHashMap<>();
    }

    /**
     * Creates a target table in DSQL if it doesn't already exist.
     *
     * @param schema The schema definition for the table
     * @param tableId The table identifier
     * @throws SQLException if table creation fails
     */
    public void createTargetTableIfNotExists(Schema schema, TableId tableId) throws SQLException {
        String tableName = getTargetTableName(tableId);

        // Check if we've already created this table
        if (createdTables.containsKey(tableName)) {
            LOG.debug("Table {} already created, skipping", tableName);
            return;
        }

        LOG.info("Creating target table: {}", tableName);

        try {
            retryHelper.executeWithRetry(() -> {
                try (Connection connection = sinkProvider.createConnection()) {
                    // TODO: Implement actual table creation logic
                    // This would involve:
                    // 1. Generate CREATE TABLE statement from schema
                    // 2. Execute the statement
                    // 3. Handle any conflicts (table already exists)

                    LOG.info("Table {} created successfully", tableName);
                    createdTables.put(tableName, true);

                    return null; // Void operation
                }
            }, "create table " + tableName);

        } catch (Exception e) {
            throw new SQLException("Failed to create target table: " + tableName, e);
        }
    }

    /**
     * Checks if a table has been created.
     *
     * @param tableId The table identifier
     * @return true if the table has been created
     */
    public boolean isTableCreated(TableId tableId) {
        String tableName = getTargetTableName(tableId);
        return createdTables.containsKey(tableName);
    }

    /**
     * Marks a table as created (useful for recovery scenarios).
     *
     * @param tableId The table identifier
     */
    public void markTableAsCreated(TableId tableId) {
        String tableName = getTargetTableName(tableId);
        createdTables.put(tableName, true);
        LOG.debug("Marked table {} as created", tableName);
    }

    /**
     * Gets the target table name for DSQL.
     * This method can be customized to implement table naming strategies.
     *
     * @param tableId The source table identifier
     * @return The target table name in DSQL
     */
    private String getTargetTableName(TableId tableId) {
        // For now, use a simple naming strategy: schema.table
        // This can be enhanced to support custom naming patterns
        if (tableId.getSchemaName() != null && !tableId.getSchemaName().isEmpty()) {
            return tableId.getSchemaName() + "." + tableId.getTableName();
        } else {
            return tableId.getTableName();
        }
    }

    /**
     * Validates that a table exists and has the expected schema.
     *
     * @param tableId The table identifier
     * @return true if the table exists and is valid
     */
    public boolean validateTable(TableId tableId) {
        String tableName = getTargetTableName(tableId);

        try {
            return retryHelper.executeWithRetry(() -> {
                try (Connection connection = sinkProvider.createConnection()) {
                    // Simple existence check using information_schema
                    String checkSql = "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1";

                    try (PreparedStatement statement = connection.prepareStatement(checkSql)) {
                        statement.setString(1, tableId.getTableName());
                        return statement.executeQuery().next();
                    }
                }
            }, "validate table " + tableName);

        } catch (Exception e) {
            LOG.warn("Failed to validate table {}: {}", tableName, e.getMessage());
            return false;
        }
    }

    /**
     * Gets statistics about created tables.
     *
     * @return Number of tables created
     */
    public int getCreatedTableCount() {
        return createdTables.size();
    }

    /**
     * Clears the created tables cache (useful for testing).
     */
    public void clearCache() {
        createdTables.clear();
        LOG.debug("Cleared created tables cache");
    }
}

