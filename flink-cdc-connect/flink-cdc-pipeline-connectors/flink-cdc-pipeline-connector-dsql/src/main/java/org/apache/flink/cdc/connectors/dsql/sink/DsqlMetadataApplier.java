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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.VarCharType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * MetadataApplier for DSQL (PostgreSQL-compatible) sink. Executes DDL statements on the target
 * database to keep the schema in sync with source schema changes.
 *
 * <p>This runs in the JobManager's SchemaRegistry coordinator thread, not in TaskManager. It
 * supports three policies: evolve (execute DDL), ignore (skip), exception (fail pipeline).
 */
public class DsqlMetadataApplier implements MetadataApplier {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DsqlMetadataApplier.class);

    /** Schema change policy options. */
    public enum SchemaChangePolicy {
        /** Automatically apply DDL changes to the target database. */
        EVOLVE,
        /** Log and ignore DDL changes, data flow continues. */
        IGNORE,
        /** Throw exception to stop the pipeline when DDL change is detected. */
        EXCEPTION
    }

    private final Configuration config;
    private final SchemaChangePolicy policy;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public DsqlMetadataApplier(Configuration config) {
        this.config = config;
        this.policy = parsePolicy(config.get(DsqlSinkOptions.SCHEMA_CHANGE_POLICY));
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        LOG.info("DsqlMetadataApplier initialized with policy: {}", this.policy);
    }

    private SchemaChangePolicy parsePolicy(String policyStr) {
        if (policyStr == null) {
            return SchemaChangePolicy.EVOLVE;
        }
        switch (policyStr.toLowerCase().trim()) {
            case "ignore":
                return SchemaChangePolicy.IGNORE;
            case "exception":
                return SchemaChangePolicy.EXCEPTION;
            case "evolve":
            default:
                return SchemaChangePolicy.EVOLVE;
        }
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        Set<SchemaChangeEventType> types = new HashSet<>();
        types.add(SchemaChangeEventType.CREATE_TABLE);
        types.add(SchemaChangeEventType.ADD_COLUMN);
        types.add(SchemaChangeEventType.DROP_COLUMN);
        types.add(SchemaChangeEventType.ALTER_COLUMN_TYPE);
        types.add(SchemaChangeEventType.RENAME_COLUMN);
        types.add(SchemaChangeEventType.DROP_TABLE);
        types.add(SchemaChangeEventType.TRUNCATE_TABLE);
        return types;
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        if (policy == SchemaChangePolicy.IGNORE) {
            return true; // Accept all but do nothing
        }
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        String eventType = event.getClass().getSimpleName();
        TableId tableId = event.tableId();

        switch (policy) {
            case IGNORE:
                LOG.info(
                        "[IGNORE] Schema change skipped for table {}: {}",
                        tableId,
                        eventType);
                return;

            case EXCEPTION:
                String msg =
                        String.format(
                                "Schema change detected (policy=exception): table=%s, event=%s. "
                                        + "Pipeline stopped. Please apply DDL manually and restart.",
                                tableId,
                                eventType);
                LOG.error(msg);
                throw new RuntimeException(msg);

            case EVOLVE:
            default:
                break;
        }

        // Policy is EVOLVE — execute DDL
        LOG.info("Applying schema change for table {}: {}", tableId, eventType);

        try (Connection connection = createConnection()) {
            if (event instanceof CreateTableEvent) {
                applyCreateTable((CreateTableEvent) event, connection);
            } else if (event instanceof AddColumnEvent) {
                applyAddColumn((AddColumnEvent) event, connection);
            } else if (event instanceof DropColumnEvent) {
                applyDropColumn((DropColumnEvent) event, connection);
            } else if (event instanceof AlterColumnTypeEvent) {
                applyAlterColumnType((AlterColumnTypeEvent) event, connection);
            } else if (event instanceof RenameColumnEvent) {
                applyRenameColumn((RenameColumnEvent) event, connection);
            } else if (event instanceof DropTableEvent) {
                applyDropTable((DropTableEvent) event, connection);
            } else if (event instanceof TruncateTableEvent) {
                applyTruncateTable((TruncateTableEvent) event, connection);
            } else {
                LOG.warn("Unhandled schema change event type: {}", eventType);
            }
            LOG.info("Successfully applied schema change for table {}", tableId);
        } catch (SQLException e) {
            String msg = e.getMessage();
            // Tolerate idempotent errors (column/table already exists or doesn't exist)
            if (msg != null
                    && (msg.contains("already exists")
                            || msg.contains("does not exist")
                            || msg.contains("duplicate column"))) {
                LOG.warn(
                        "Schema change for table {} completed with idempotent warning: {}",
                        tableId,
                        msg);
            } else {
                LOG.error(
                        "Failed to apply schema change for table {}: {}",
                        tableId,
                        msg,
                        e);
                throw new RuntimeException(
                        "Schema change failed for table " + tableId + ": " + msg, e);
            }
        }
    }

    // ======================== DDL Handlers ========================

    private void applyCreateTable(CreateTableEvent event, Connection connection)
            throws SQLException {
        TableId tableId = event.tableId();
        Schema schema = event.getSchema();

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(getTableName(tableId)).append(" (");

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(column.getName()))
                    .append(" ")
                    .append(mapDataTypeToSql(column.getType()));
        }

        List<String> primaryKeys = schema.primaryKeys();
        if (!primaryKeys.isEmpty()) {
            sql.append(", PRIMARY KEY (");
            for (int i = 0; i < primaryKeys.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quoteIdentifier(primaryKeys.get(i)));
            }
            sql.append(")");
        }
        sql.append(")");

        executeSql(connection, sql.toString());

        // If table already existed, ensure all columns from the schema are present.
        // This handles the case where the pipeline restarts and the source schema
        // has evolved since the target table was originally created.
        for (Column column : columns) {
            String addColSql =
                    "ALTER TABLE "
                            + getTableName(tableId)
                            + " ADD COLUMN IF NOT EXISTS "
                            + quoteIdentifier(column.getName())
                            + " "
                            + mapDataTypeToSql(column.getType());
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(addColSql);
            } catch (SQLException e) {
                if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                    // Column exists — fine
                } else {
                    LOG.warn(
                            "Failed to ensure column {} exists on table {}: {}",
                            column.getName(),
                            tableId,
                            e.getMessage());
                }
            }
        }
    }

    private void applyAddColumn(AddColumnEvent event, Connection connection) throws SQLException {
        TableId tableId = event.tableId();
        for (AddColumnEvent.ColumnWithPosition col : event.getAddedColumns()) {
            Column column = col.getAddColumn();
            String sql =
                    "ALTER TABLE "
                            + getTableName(tableId)
                            + " ADD COLUMN IF NOT EXISTS "
                            + quoteIdentifier(column.getName())
                            + " "
                            + mapDataTypeToSql(column.getType());
            executeSql(connection, sql);
        }
    }

    private void applyDropColumn(DropColumnEvent event, Connection connection) throws SQLException {
        TableId tableId = event.tableId();
        for (String columnName : event.getDroppedColumnNames()) {
            String sql =
                    "ALTER TABLE "
                            + getTableName(tableId)
                            + " DROP COLUMN IF EXISTS "
                            + quoteIdentifier(columnName);
            executeSql(connection, sql);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event, Connection connection)
            throws SQLException {
        TableId tableId = event.tableId();
        for (Map.Entry<String, DataType> entry : event.getTypeMapping().entrySet()) {
            String columnName = entry.getKey();
            DataType newType = entry.getValue();
            String sql =
                    "ALTER TABLE "
                            + getTableName(tableId)
                            + " ALTER COLUMN "
                            + quoteIdentifier(columnName)
                            + " TYPE "
                            + mapDataTypeToSql(newType);
            executeSql(connection, sql);
        }
    }

    private void applyRenameColumn(RenameColumnEvent event, Connection connection)
            throws SQLException {
        TableId tableId = event.tableId();
        for (Map.Entry<String, String> entry : event.getNameMapping().entrySet()) {
            String oldName = entry.getKey();
            String newName = entry.getValue();
            String sql =
                    "ALTER TABLE "
                            + getTableName(tableId)
                            + " RENAME COLUMN "
                            + quoteIdentifier(oldName)
                            + " TO "
                            + quoteIdentifier(newName);
            executeSql(connection, sql);
        }
    }

    private void applyDropTable(DropTableEvent event, Connection connection) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + getTableName(event.tableId());
        executeSql(connection, sql);
    }

    private void applyTruncateTable(TruncateTableEvent event, Connection connection)
            throws SQLException {
        String sql = "TRUNCATE TABLE " + getTableName(event.tableId());
        executeSql(connection, sql);
    }

    // ======================== Helper Methods ========================

    private void executeSql(Connection connection, String sql) throws SQLException {
        LOG.info("Executing DDL: {}", sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private Connection createConnection() throws SQLException {
        String host = config.get(DsqlSinkOptions.HOST);
        Integer port = config.get(DsqlSinkOptions.PORT);
        String database = config.get(DsqlSinkOptions.DATABASE);
        String username = config.get(DsqlSinkOptions.USERNAME);
        String password = config.get(DsqlSinkOptions.PASSWORD);

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);

        Properties props = new Properties();
        props.setProperty("user", username);
        if (password != null) {
            props.setProperty("password", password);
        }

        LOG.debug("Creating JDBC connection to {} for DDL execution", jdbcUrl);
        return DriverManager.getConnection(jdbcUrl, props);
    }

    private String getTableName(TableId tableId) {
        if (tableId.getSchemaName() != null && !tableId.getSchemaName().isEmpty()) {
            return quoteIdentifier(tableId.getSchemaName())
                    + "."
                    + quoteIdentifier(tableId.getTableName());
        }
        // Use configured schema as default
        String schema = config.get(DsqlSinkOptions.SCHEMA);
        if (schema != null && !schema.isEmpty()) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(tableId.getTableName());
        }
        return quoteIdentifier(tableId.getTableName());
    }

    private String quoteIdentifier(String identifier) {
        if (identifier == null) {
            return "\"\"";
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String mapDataTypeToSql(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                if (dataType instanceof DecimalType) {
                    DecimalType dt = (DecimalType) dataType;
                    return "DECIMAL(" + dt.getPrecision() + "," + dt.getScale() + ")";
                }
                return "DECIMAL(38,10)";
            case BOOLEAN:
                return "BOOLEAN";
            case VARCHAR:
                if (dataType instanceof VarCharType) {
                    VarCharType vt = (VarCharType) dataType;
                    if (vt.getLength() == VarCharType.MAX_LENGTH) {
                        return "TEXT";
                    }
                    return "VARCHAR(" + vt.getLength() + ")";
                }
                return "TEXT";
            case CHAR:
                if (dataType instanceof CharType) {
                    CharType ct = (CharType) dataType;
                    return "CHAR(" + ct.getLength() + ")";
                }
                return "VARCHAR(255)";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMPTZ";
            case BINARY:
            case VARBINARY:
                return "BYTEA";
            default:
                return "TEXT";
        }
    }

    @Override
    public void close() throws Exception {
        // No persistent resources to close — connections are created per-operation
    }
}
