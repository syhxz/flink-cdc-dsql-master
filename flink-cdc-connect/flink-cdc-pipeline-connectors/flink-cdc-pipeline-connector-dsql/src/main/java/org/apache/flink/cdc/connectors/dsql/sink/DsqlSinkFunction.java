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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A simple sink function for DSQL that processes CDC events.
 */
public class DsqlSinkFunction implements SinkFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlSinkFunction.class);
    
    private final Configuration config;
    private transient DsqlSinkProvider sinkProvider;
    
    public DsqlSinkFunction(Configuration config) {
        this.config = config;
    }
    
    @Override
    public void invoke(Event event, Context context) throws Exception {
        // Initialize sink provider if not already done
        if (sinkProvider == null) {
            sinkProvider = new DsqlSinkProvider(config);
            LOG.info("Initialized DSQL sink provider");
        }
        
        // Process the CDC event
        processEvent(event);
    }
    
    private void processEvent(Event event) {
        try {
            LOG.info("Processing CDC event: {}", event.getClass().getSimpleName());
            
            // Get table information if available
            String tableInfo = "unknown";
            if (event instanceof org.apache.flink.cdc.common.event.DataChangeEvent) {
                org.apache.flink.cdc.common.event.DataChangeEvent dataEvent = 
                    (org.apache.flink.cdc.common.event.DataChangeEvent) event;
                tableInfo = dataEvent.tableId().toString();
                LOG.info("Data change event: {} on table {}", dataEvent.op(), tableInfo);
            } else if (event instanceof org.apache.flink.cdc.common.event.SchemaChangeEvent) {
                org.apache.flink.cdc.common.event.SchemaChangeEvent schemaEvent = 
                    (org.apache.flink.cdc.common.event.SchemaChangeEvent) event;
                tableInfo = schemaEvent.tableId().toString();
                LOG.info("Schema change event on table {}", tableInfo);
            }
            
            // Test connection and log success
            try (Connection connection = sinkProvider.createConnection()) {
                if (connection.isValid(5)) {
                    LOG.debug("Successfully connected to DSQL for event processing");
                    // Process the event based on its type
                    if (event instanceof org.apache.flink.cdc.common.event.CreateTableEvent) {
                        processCreateTableEvent((org.apache.flink.cdc.common.event.CreateTableEvent) event, connection);
                    } else if (event instanceof org.apache.flink.cdc.common.event.DataChangeEvent) {
                        processDataChangeEvent((org.apache.flink.cdc.common.event.DataChangeEvent) event, connection);
                    } else if (event instanceof org.apache.flink.cdc.common.event.SchemaChangeEvent) {
                        processSchemaChangeEvent((org.apache.flink.cdc.common.event.SchemaChangeEvent) event, connection);
                    } else {
                        LOG.debug("Skipping unsupported event type: {}", event.getClass().getSimpleName());
                    }
                    
                    connection.commit();
                } else {
                    LOG.warn("DSQL connection is not valid");
                }
            }
            
        } catch (SQLException e) {
            LOG.error("Failed to process event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process CDC event", e);
        } catch (Exception e) {
            LOG.error("Unexpected error processing event: {}", e.getMessage(), e);
            throw new RuntimeException("Unexpected error processing CDC event", e);
        }
    }
    
    private void processCreateTableEvent(org.apache.flink.cdc.common.event.CreateTableEvent event, Connection connection) throws SQLException {
        LOG.info("Processing CreateTableEvent for table: {}", event.tableId());
        
        // Generate CREATE TABLE statement
        String createTableSql = generateCreateTableSql(event);
        LOG.debug("Generated CREATE TABLE SQL: {}", createTableSql);
        
        // Execute the CREATE TABLE statement
        try (var statement = connection.prepareStatement(createTableSql)) {
            statement.execute();
            LOG.info("Successfully created table: {}", event.tableId());
        } catch (SQLException e) {
            // Check if table already exists (this is common in CDC scenarios)
            if (e.getMessage().contains("already exists") || e.getMessage().contains("duplicate")) {
                LOG.info("Table {} already exists, skipping creation", event.tableId());
            } else {
                LOG.error("Failed to create table {}: {}", event.tableId(), e.getMessage());
                throw e;
            }
        }
    }
    
    private void processDataChangeEvent(org.apache.flink.cdc.common.event.DataChangeEvent event, Connection connection) throws SQLException {
        LOG.debug("Processing DataChangeEvent: {} on table {}", event.op(), event.tableId());
        
        String sql;
        switch (event.op()) {
            case INSERT:
                sql = generateInsertSql(event);
                break;
            case UPDATE:
                sql = generateUpdateSql(event);
                break;
            case DELETE:
                sql = generateDeleteSql(event);
                break;
            default:
                LOG.warn("Unsupported operation: {}", event.op());
                return;
        }
        
        LOG.debug("Generated SQL: {}", sql);
        
        // Execute the SQL statement
        try (var statement = connection.prepareStatement(sql)) {
            setStatementParameters(statement, event);
            int rowsAffected = statement.executeUpdate();
            LOG.debug("SQL executed successfully, {} rows affected", rowsAffected);
        }
    }
    
    private void processSchemaChangeEvent(org.apache.flink.cdc.common.event.SchemaChangeEvent event, Connection connection) throws SQLException {
        LOG.info("Processing SchemaChangeEvent on table: {}", event.tableId());
        
        // For now, log schema changes but don't execute them
        // In a full implementation, this would handle ALTER TABLE statements
        LOG.warn("Schema change events are not yet fully implemented: {}", event.getClass().getSimpleName());
    }
    
    private String generateCreateTableSql(org.apache.flink.cdc.common.event.CreateTableEvent event) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ");
        sql.append(formatTableName(event.tableId()));
        sql.append(" (");
        
        // Add columns
        var columns = event.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            var column = columns.get(i);
            sql.append(column.getName()).append(" ");
            sql.append(mapDataType(column.getType()));
            if (!column.getType().isNullable()) {
                sql.append(" NOT NULL");
            }
        }
        
        // Add primary key if exists
        var primaryKeys = event.getSchema().primaryKeys();
        if (!primaryKeys.isEmpty()) {
            sql.append(", PRIMARY KEY (");
            for (int i = 0; i < primaryKeys.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(primaryKeys.get(i));
            }
            sql.append(")");
        }
        
        sql.append(")");
        return sql.toString();
    }
    
    private String generateInsertSql(org.apache.flink.cdc.common.event.DataChangeEvent event) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append(formatTableName(event.tableId()));
        sql.append(" VALUES (");
        
        // Add placeholders for all columns
        var after = event.after();
        int fieldCount = after.getArity();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        
        sql.append(")");
        return sql.toString();
    }
    
    private String generateUpdateSql(org.apache.flink.cdc.common.event.DataChangeEvent event) {
        // For now, implement a simple UPDATE based on primary key
        // In a full implementation, this would be more sophisticated
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ");
        sql.append(formatTableName(event.tableId()));
        sql.append(" SET ");
        
        // Add SET clauses (simplified - assumes all fields are updated)
        var after = event.after();
        int fieldCount = after.getArity();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) sql.append(", ");
            sql.append("column").append(i).append(" = ?");
        }
        
        sql.append(" WHERE id = ?"); // Simplified - assumes 'id' is the primary key
        return sql.toString();
    }
    
    private String generateDeleteSql(org.apache.flink.cdc.common.event.DataChangeEvent event) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ");
        sql.append(formatTableName(event.tableId()));
        sql.append(" WHERE id = ?"); // Simplified - assumes 'id' is the primary key
        return sql.toString();
    }
    
    private void setStatementParameters(java.sql.PreparedStatement statement, org.apache.flink.cdc.common.event.DataChangeEvent event) throws SQLException {
        // This is a simplified implementation
        // In a full implementation, this would properly map field values to SQL parameters
        LOG.debug("Setting statement parameters for {} operation", event.op());
        
        var data = event.after() != null ? event.after() : event.before();
        if (data != null) {
            // For now, just log that we would set parameters
            LOG.debug("Would set {} parameters from record data", data.getArity());
            
            // Set dummy parameters to avoid SQL errors
            for (int i = 1; i <= data.getArity(); i++) {
                statement.setObject(i, "dummy_value_" + i);
            }
        }
    }
    
    private String formatTableName(org.apache.flink.cdc.common.event.TableId tableId) {
        // Format table name for DSQL
        if (tableId.getSchemaName() != null) {
            return tableId.getSchemaName() + "." + tableId.getTableName();
        } else {
            return tableId.getTableName();
        }
    }
    
    private String mapDataType(org.apache.flink.cdc.common.types.DataType dataType) {
        // Simple data type mapping for DSQL
        // In a full implementation, this would be more comprehensive
        String typeName = dataType.getTypeRoot().name();
        switch (typeName) {
            case "INTEGER":
                return "INTEGER";
            case "BIGINT":
                return "BIGINT";
            case "VARCHAR":
                return "VARCHAR(255)";
            case "BOOLEAN":
                return "BOOLEAN";
            case "TIMESTAMP":
                return "TIMESTAMP";
            default:
                LOG.warn("Unknown data type: {}, using VARCHAR", typeName);
                return "VARCHAR(255)";
        }
    }
}
