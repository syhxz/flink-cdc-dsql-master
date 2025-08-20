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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresTableNameFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Utility class for discovering PostgreSQL table schemas. */
@Internal
public class PostgresSchemaDiscovery implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaDiscovery.class);

    private final PostgresSourceConfig config;
    private Connection connection;

    public PostgresSchemaDiscovery(PostgresSourceConfig config) {
        this.config = config;
    }

    /**
     * Discover all tables specified in the configuration.
     *
     * @return list of discovered table IDs
     * @throws Exception if discovery fails
     */
    public List<TableId> discoverTables() throws Exception {
        List<TableId> tables = new ArrayList<>();

        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            for (String formattedTable : config.getTableList()) {
                try {
                    TableId tableId = PostgresTableNameFormatter.parseTableId(formattedTable);
                    
                    if (tableExists(metaData, tableId)) {
                        tables.add(tableId);
                        LOG.info("Discovered table: {}", tableId);
                    } else {
                        throw new IllegalArgumentException(
                                "Table " + formattedTable + " does not exist in database " + 
                                config.getDatabaseList().get(0) + ". Please verify the table name and schema.");
                    }
                } catch (Exception e) {
                    LOG.error("Failed to discover table: {}", formattedTable, e);
                    throw new RuntimeException(
                            "Failed to discover table " + formattedTable + ": " + e.getMessage(), e);
                }
            }
        } catch (SQLException e) {
            LOG.error("Database connection failed during table discovery", e);
            throw new RuntimeException(
                    "Failed to connect to PostgreSQL database for table discovery: " + e.getMessage(), e);
        }
        
        LOG.info("Successfully discovered {} tables in total", tables.size());
        return tables;
    }

    /**
     * Get the schema for a specific table.
     *
     * @param tableId the table ID
     * @return the table schema
     * @throws Exception if schema discovery fails
     */
    public Schema getTableSchema(TableId tableId) throws Exception {
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            // Verify table exists before getting schema
            if (!tableExists(metaData, tableId)) {
                throw new IllegalArgumentException(
                        "Table " + tableId + " does not exist in database " + config.getDatabaseList().get(0));
            }
            
            List<Column> columns = getTableColumns(metaData, tableId);
            if (columns.isEmpty()) {
                throw new RuntimeException(
                        "No columns found for table " + tableId + ". This may indicate a permissions issue.");
            }
            
            List<String> primaryKeys = getTablePrimaryKeys(metaData, tableId);
            
            Schema.Builder schemaBuilder = Schema.newBuilder()
                    .setColumns(columns);
            
            if (!primaryKeys.isEmpty()) {
                schemaBuilder.primaryKey(primaryKeys);
            }
            
            Schema schema = schemaBuilder.build();
            LOG.info("Successfully discovered schema for table {}: {} columns, primary keys: {}", 
                    tableId, columns.size(), primaryKeys);
            
            return schema;
            
        } catch (SQLException e) {
            LOG.error("Failed to get schema for table: {}", tableId, e);
            throw new RuntimeException(
                    "Failed to get schema for table " + tableId + ": " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error while getting schema for table: {}", tableId, e);
            throw new RuntimeException(
                    "Unexpected error while getting schema for table " + tableId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Check if a table exists in the database.
     */
    private boolean tableExists(DatabaseMetaData metaData, TableId tableId) throws SQLException {
        try (ResultSet rs = metaData.getTables(
                config.getDatabaseList().get(0),
                tableId.getSchemaName(),
                tableId.getTableName(),
                new String[]{"TABLE"})) {
            
            return rs.next();
        }
    }

    /**
     * Get column information for a table.
     */
    private List<Column> getTableColumns(DatabaseMetaData metaData, TableId tableId) throws SQLException {
        List<Column> columns = new ArrayList<>();
        
        try (ResultSet rs = metaData.getColumns(
                config.getDatabaseList().get(0),
                tableId.getSchemaName(),
                tableId.getTableName(),
                null)) {
            
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                String defaultValue = rs.getString("COLUMN_DEF");
                
                DataType flinkDataType = mapPostgresTypeToFlinkType(
                        dataType, typeName, columnSize, decimalDigits);
                
                Column column = Column.physicalColumn(columnName, flinkDataType, defaultValue);
                columns.add(column);
                
                LOG.debug("Discovered column: {} {} (nullable: {}, default: {})", 
                        columnName, flinkDataType, nullable, defaultValue);
            }
        }
        
        return columns;
    }

    /**
     * Get primary key information for a table.
     */
    private List<String> getTablePrimaryKeys(DatabaseMetaData metaData, TableId tableId) throws SQLException {
        List<String> primaryKeys = new ArrayList<>();
        Map<Integer, String> keyColumns = new HashMap<>();
        
        try (ResultSet rs = metaData.getPrimaryKeys(
                config.getDatabaseList().get(0),
                tableId.getSchemaName(),
                tableId.getTableName())) {
            
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int keySeq = rs.getInt("KEY_SEQ");
                keyColumns.put(keySeq, columnName);
            }
        }
        
        // Sort by key sequence to maintain order
        keyColumns.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> primaryKeys.add(entry.getValue()));
        
        return primaryKeys;
    }

    /**
     * Map PostgreSQL data types to Flink data types.
     */
    private DataType mapPostgresTypeToFlinkType(int sqlType, String typeName, int precision, int scale) {
        switch (sqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return DataTypes.BOOLEAN();
                
            case Types.TINYINT:
            case Types.SMALLINT:
                return DataTypes.SMALLINT();
                
            case Types.INTEGER:
                return DataTypes.INT();
                
            case Types.BIGINT:
                return DataTypes.BIGINT();
                
            case Types.REAL:
            case Types.FLOAT:
                return DataTypes.FLOAT();
                
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
                
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (scale > 0) {
                    return DataTypes.DECIMAL(precision, scale);
                } else {
                    return DataTypes.DECIMAL(precision, 0);
                }
                
            case Types.CHAR:
                return DataTypes.CHAR(precision);
                
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (precision > 0 && precision <= Integer.MAX_VALUE) {
                    return DataTypes.VARCHAR(precision);
                } else {
                    return DataTypes.STRING();
                }
                
            case Types.DATE:
                return DataTypes.DATE();
                
            case Types.TIME:
                return DataTypes.TIME();
                
            case Types.TIMESTAMP:
                return DataTypes.TIMESTAMP();
                
            case Types.BINARY:
                return DataTypes.BINARY(precision);
                
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (precision > 0 && precision <= Integer.MAX_VALUE) {
                    return DataTypes.VARBINARY(precision);
                } else {
                    return DataTypes.BYTES();
                }
                
            case Types.ARRAY:
                // PostgreSQL arrays - map to string for now
                LOG.warn("PostgreSQL array type {} mapped to STRING", typeName);
                return DataTypes.STRING();
                
            case Types.OTHER:
                // Handle PostgreSQL-specific types
                return handlePostgresSpecificTypes(typeName);
                
            default:
                LOG.warn("Unsupported PostgreSQL type: {} ({}), mapping to STRING", typeName, sqlType);
                return DataTypes.STRING();
        }
    }

    /**
     * Handle PostgreSQL-specific data types.
     */
    private DataType handlePostgresSpecificTypes(String typeName) {
        String lowerTypeName = typeName.toLowerCase();
        
        switch (lowerTypeName) {
            case "uuid":
                return DataTypes.CHAR(36); // UUID is 36 characters
                
            case "json":
            case "jsonb":
                return DataTypes.STRING();
                
            case "inet":
            case "cidr":
                return DataTypes.VARCHAR(43); // Max IPv6 length
                
            case "macaddr":
                return DataTypes.CHAR(17); // MAC address length
                
            case "text":
                return DataTypes.STRING();
                
            case "bytea":
                return DataTypes.BYTES();
                
            default:
                LOG.warn("Unknown PostgreSQL-specific type: {}, mapping to STRING", typeName);
                return DataTypes.STRING();
        }
    }

    /**
     * Get a database connection.
     */
    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Properties properties = new Properties();
            properties.setProperty("user", config.getUsername());
            properties.setProperty("password", config.getPassword());
            properties.setProperty("connectTimeout", "10"); // 10 seconds timeout
            
            String jdbcUrl = String.format(
                    "jdbc:postgresql://%s:%d/%s",
                    config.getHostname(),
                    config.getPort(),
                    config.getDatabaseList().get(0));
            
            try {
                LOG.debug("Connecting to PostgreSQL: {}", jdbcUrl);
                connection = DriverManager.getConnection(jdbcUrl, properties);
                
                // Test connection validity
                if (!connection.isValid(5)) {
                    throw new SQLException("Connection is not valid");
                }
                
            } catch (SQLException e) {
                LOG.error("Failed to connect to PostgreSQL database: {}", jdbcUrl, e);
                throw new SQLException(
                        "Failed to connect to PostgreSQL database at " + 
                        config.getHostname() + ":" + config.getPort() + "/" + config.getDatabaseList().get(0) + 
                        ". Error: " + e.getMessage(), e);
            }
        }
        
        return connection;
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.debug("Closed PostgreSQL connection");
        }
    }
}
