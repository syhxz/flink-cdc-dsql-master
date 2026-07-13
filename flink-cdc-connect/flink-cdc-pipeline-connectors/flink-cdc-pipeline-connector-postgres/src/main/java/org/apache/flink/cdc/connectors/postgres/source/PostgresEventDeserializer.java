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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.util.Collector;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Event deserializer for {@link PostgresDataSource}. */
@Internal
public class PostgresEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresEventDeserializer.class);

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.postgresql.SchemaChangeKey";

    private final boolean includeSchemaChanges;

    // Track tables for which we've emitted CreateTableEvents
    private final Set<TableId> tablesWithCreateTableEvents = new HashSet<>();

    public PostgresEventDeserializer(
            boolean includeSchemaChanges, PostgresSchemaDataTypeInference schemaDataTypeInference) {
        super(schemaDataTypeInference, DebeziumChangelogMode.ALL);
        this.includeSchemaChanges = includeSchemaChanges;
        LOG.info(
                "=== PostgresEventDeserializer INITIALIZED with includeSchemaChanges: {} ===",
                includeSchemaChanges);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Event> out) throws Exception {
        LOG.info(
                "=== DESERIALIZE: topic={}, partition={}, offset={} ===",
                record.topic(),
                record.sourcePartition(),
                record.sourceOffset());

        LOG.debug(
                "Deserializing record: topic={}, key={}, value={}",
                record.topic(),
                record.key(),
                record.value());

        // Add detailed debugging for record structure
        if (record.value() instanceof Struct) {
            Struct value = (Struct) record.value();
            LOG.info("Record value schema: {}", value.schema());

            // Log the "after" field structure
            Object afterValue = value.get("after");
            if (afterValue instanceof Struct) {
                Struct afterStruct = (Struct) afterValue;
                LOG.info("After struct schema: {}", afterStruct.schema());
                LOG.info("After struct fields: {}", afterStruct.schema().fields().size());
                for (org.apache.kafka.connect.data.Field field : afterStruct.schema().fields()) {
                    Object fieldValue = afterStruct.get(field.name());
                    LOG.info(
                            "  Field '{}': type={}, value={}",
                            field.name(),
                            field.schema().type(),
                            fieldValue);
                }
            } else {
                LOG.info("After value is not a Struct: {}", afterValue);
            }
        }

        if (isSchemaChangeEvent(record)) {
            LOG.info("=== SCHEMA CHANGE EVENT DETECTED ===");
            if (includeSchemaChanges) {
                List<SchemaChangeEvent> schemaChangeEvents = deserializeSchemaChangeRecord(record);
                for (SchemaChangeEvent event : schemaChangeEvents) {
                    LOG.info("Emitting schema change event: {}", event);
                    out.collect(event);
                }
            }
        } else if (isDataChangeRecord(record)) {
            LOG.info("=== DATA CHANGE EVENT DETECTED ===");
            // Emit CreateTableEvent if this is the first time we see this table
            TableId tableId = getTableId(record);
            if (tableId != null && !tablesWithCreateTableEvents.contains(tableId)) {
                LOG.info("=== EMITTING CREATE TABLE EVENT FOR: {} ===", tableId);
                emitCreateTableEventFromRecord(record, tableId, out);
                tablesWithCreateTableEvents.add(tableId);
            }

            // Check if this is a DDL command table INSERT — convert to SchemaChangeEvent
            if (tableId != null && isDdlCommandTable(tableId)) {
                LOG.info("=== DDL COMMAND TABLE EVENT DETECTED ===");
                handleDdlCommandEvent(record, out);
                return; // Don't process as normal data change
            }

            // Add schema validation before processing
            validateRecordSchema(record);
            LOG.info("=== CALLING SUPER.DESERIALIZE FOR DATA CHANGE ===");
            super.deserialize(record, out);
            LOG.info("=== SUPER.DESERIALIZE COMPLETED ===");
        } else {
            LOG.info("=== UNKNOWN RECORD TYPE - SKIPPING: topic={} ===", record.topic());
            LOG.debug(
                    "Skipping record that is neither schema change nor data change: {}",
                    record.topic());
        }
    }

    private void validateRecordSchema(SourceRecord record) {
        if (record.valueSchema() == null) {
            LOG.warn("Record has null value schema: topic={}", record.topic());
        } else {
            LOG.debug(
                    "Record schema: topic={}, schema={}",
                    record.topic(),
                    record.valueSchema().name());
        }

        if (record.value() == null) {
            LOG.warn("Record has null value: topic={}", record.topic());
        }
    }

    @Override
    protected org.apache.flink.cdc.common.event.TableId getTableId(SourceRecord record) {
        LOG.debug("Extracting table ID from record: topic={}", record.topic());

        // Extract table ID from the source record
        if (record.topic() != null) {
            // PostgreSQL topic format is typically: serverName.schemaName.tableName
            String[] parts = record.topic().split("\\.");
            if (parts.length >= 3) {
                String database = parts[0];
                String schema = parts[1];
                String table = parts[2];
                org.apache.flink.cdc.common.event.TableId tableId =
                        org.apache.flink.cdc.common.event.TableId.tableId(database, schema, table);
                LOG.debug("Extracted table ID from topic: {}", tableId);
                return tableId;
            } else {
                LOG.warn(
                        "Topic format unexpected, expected serverName.schemaName.tableName but got: {}",
                        record.topic());
            }
        }

        // Fallback: try to extract from record key/value
        if (record.value() instanceof Struct) {
            Struct struct = (Struct) record.value();
            if (struct.schema().field("source") != null) {
                Struct source = struct.getStruct("source");
                if (source != null) {
                    String database = source.getString("db");
                    String schema = source.getString("schema");
                    String table = source.getString("table");
                    if (database != null && schema != null && table != null) {
                        org.apache.flink.cdc.common.event.TableId tableId =
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        database, schema, table);
                        LOG.debug("Extracted table ID from source struct: {}", tableId);
                        return tableId;
                    } else {
                        LOG.warn(
                                "Source struct missing required fields: db={}, schema={}, table={}",
                                database,
                                schema,
                                table);
                    }
                } else {
                    LOG.warn("Source struct is null in record value");
                }
            } else {
                LOG.warn("No 'source' field found in record value schema");
            }
        } else {
            LOG.warn(
                    "Record value is not a Struct: {}",
                    record.value() != null ? record.value().getClass() : "null");
        }

        // Default fallback
        org.apache.flink.cdc.common.event.TableId fallbackTableId =
                org.apache.flink.cdc.common.event.TableId.tableId(
                        "postgres_cdc_source", "public", "unknown");
        LOG.warn("Using fallback table ID: {}", fallbackTableId);
        return fallbackTableId;
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        // Return empty metadata for now
        // In a full implementation, you'd extract metadata from the record
        return Map.of();
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        return isSchemaChangeEvent(record);
    }

    private boolean isSchemaChangeEvent(SourceRecord sourceRecord) {
        return sourceRecord.key() != null
                && sourceRecord.keySchema() != null
                && SCHEMA_CHANGE_EVENT_KEY_NAME.equals(sourceRecord.keySchema().name());
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        try {
            Struct key = (Struct) record.key();
            String databaseName = key.getString("databaseName");

            Struct value = (Struct) record.value();
            if (value == null) {
                return null;
            }

            String ddl = value.getString("ddl");
            if (ddl == null || ddl.trim().isEmpty()) {
                return null;
            }

            // For now, we'll create a simple schema change event
            // In a full implementation, you'd parse the DDL and create appropriate events
            org.apache.flink.cdc.common.event.TableId tableId =
                    org.apache.flink.cdc.common.event.TableId.tableId(
                            databaseName, "public", "unknown");

            LOG.info("Received schema change event for database {}: {}", databaseName, ddl);

            // Return empty list for now - schema change parsing would be implemented here
            return List.of();

        } catch (Exception e) {
            LOG.warn("Failed to deserialize schema change record", e);
            return List.of();
        }
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        return record.value() != null
                && record.valueSchema() != null
                && record.valueSchema().field(Envelope.FieldName.OPERATION) != null;
    }

    /**
     * Emit a CreateTableEvent by creating a basic schema from the record structure. This ensures
     * the SchemaOperator has schema information before processing data events.
     */
    private void emitCreateTableEventFromRecord(
            SourceRecord record, TableId tableId, Collector<Event> out) {
        try {
            // Create a basic schema from the record's value schema
            Schema schema = createSchemaFromRecord(record);
            if (schema != null) {
                CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
                out.collect(createTableEvent);
                LOG.info(
                        "Emitted CreateTableEvent for table: {} with {} columns",
                        tableId,
                        schema.getColumns().size());
            } else {
                LOG.warn("Could not create schema for table: {} from record", tableId);
            }
        } catch (Exception e) {
            LOG.error("Failed to emit CreateTableEvent for table: {}", tableId, e);
        }
    }

    /**
     * Create a basic Schema from a Debezium SourceRecord. This extracts column information from the
     * record's value schema.
     */
    private Schema createSchemaFromRecord(SourceRecord record) {
        try {
            if (record.valueSchema() == null || record.value() == null) {
                return null;
            }

            // Get the "after" field schema which contains the table structure
            org.apache.kafka.connect.data.Schema valueSchema = record.valueSchema();
            org.apache.kafka.connect.data.Field afterField = valueSchema.field("after");

            if (afterField == null || afterField.schema() == null) {
                LOG.warn("No 'after' field found in record schema");
                return null;
            }

            org.apache.kafka.connect.data.Schema afterSchema = afterField.schema();
            Schema.Builder schemaBuilder = Schema.newBuilder();

            // Convert each field in the "after" schema to a column
            LOG.info(
                    "Creating schema from {} fields in 'after' schema",
                    afterSchema.fields().size());
            List<String> columnNames = new ArrayList<>();

            for (org.apache.kafka.connect.data.Field field : afterSchema.fields()) {
                String columnName = field.name();
                org.apache.flink.cdc.common.types.DataType dataType =
                        schemaDataTypeInference.infer(null, field.schema());

                LOG.info("Adding column: name='{}', type={}", columnName, dataType);
                schemaBuilder.physicalColumn(columnName, dataType);
                columnNames.add(columnName);
            }

            // Set primary key - assume first column named 'id' or first column if no 'id'
            List<String> primaryKeys = new ArrayList<>();
            if (columnNames.contains("id")) {
                primaryKeys.add("id");
                LOG.info("Setting primary key to 'id' column");
            } else if (!columnNames.isEmpty()) {
                primaryKeys.add(columnNames.get(0));
                LOG.info("Setting primary key to first column: '{}'", columnNames.get(0));
            }

            if (!primaryKeys.isEmpty()) {
                schemaBuilder.primaryKey(primaryKeys);
                LOG.info("Schema created with primary key: {}", primaryKeys);
            } else {
                LOG.warn("No primary key set for schema - this may cause UPDATE/DELETE issues");
            }

            return schemaBuilder.build();

        } catch (Exception e) {
            LOG.error("Failed to create schema from record", e);
            return null;
        }
    }

    // ======================== DDL Capture Support ========================

    private static final String DDL_COMMAND_TABLE_NAME = "flink_cdc_ddl_command";

    private boolean isDdlCommandTable(TableId tableId) {
        return DDL_COMMAND_TABLE_NAME.equals(tableId.getTableName());
    }

    private void handleDdlCommandEvent(SourceRecord record, Collector<Event> out) {
        try {
            Struct value = (Struct) record.value();
            if (value == null) return;

            String op = value.getString(Envelope.FieldName.OPERATION);
            // Only process CDC INSERT operations (op='c'), NOT snapshot reads (op='r').
            // Snapshot reads are historical DDL records that have already been applied.
            if (!"c".equals(op)) {
                return;
            }

            Object afterValue = value.get(Envelope.FieldName.AFTER);
            if (!(afterValue instanceof Struct)) return;

            Struct after = (Struct) afterValue;
            String tag = getFieldString(after, "tag");
            String ddlText = getFieldString(after, "ddl_text");
            String schemaName = getFieldString(after, "schema_name");

            if (ddlText == null || ddlText.isEmpty()) {
                LOG.debug("DDL command event with empty ddl_text, skipping");
                return;
            }

            LOG.info("Processing captured DDL: tag={}, ddl={}", tag, ddlText);

            // Parse DDL and emit corresponding SchemaChangeEvent
            List<SchemaChangeEvent> events = parseDdlToSchemaChangeEvents(tag, ddlText, schemaName);
            for (SchemaChangeEvent event : events) {
                LOG.info("Emitting DDL-derived schema change event: {}", event);
                out.collect(event);
            }
        } catch (Exception e) {
            LOG.warn("Failed to process DDL command event: {}", e.getMessage(), e);
        }
    }

    private String getFieldString(Struct struct, String fieldName) {
        try {
            if (struct.schema().field(fieldName) != null) {
                Object val = struct.get(fieldName);
                return val != null ? val.toString() : null;
            }
        } catch (Exception e) {
            // field not found
        }
        return null;
    }

    private List<SchemaChangeEvent> parseDdlToSchemaChangeEvents(
            String tag, String ddlText, String schemaName) {

        List<SchemaChangeEvent> events = new java.util.ArrayList<>();

        if (tag == null) return events;

        // Normalize
        String upperTag = tag.toUpperCase().trim();
        String upperDdl = ddlText.toUpperCase().trim();

        // Extract table name from DDL
        String tableName = extractTableName(ddlText, upperTag);
        if (tableName == null) {
            LOG.debug("Could not extract table name from DDL: {}", ddlText);
            return events;
        }

        // Build TableId - use the source prefix convention
        String namespace = "postgres_cdc_source";
        String schema = (schemaName != null && !schemaName.isEmpty()) ? schemaName : "public";
        org.apache.flink.cdc.common.event.TableId tableId =
                org.apache.flink.cdc.common.event.TableId.tableId(namespace, schema, tableName);

        switch (upperTag) {
            case "ALTER TABLE":
                if (upperDdl.contains("ADD COLUMN") || upperDdl.contains("ADD ")) {
                    // Parse ADD COLUMN
                    List<AddColumnEvent.ColumnWithPosition> addedCols = parseAddColumns(ddlText);
                    if (!addedCols.isEmpty()) {
                        events.add(new AddColumnEvent(tableId, addedCols));
                    }
                } else if (upperDdl.contains("DROP COLUMN")) {
                    List<String> droppedCols = parseDropColumns(ddlText);
                    if (!droppedCols.isEmpty()) {
                        events.add(new org.apache.flink.cdc.common.event.DropColumnEvent(
                                tableId, droppedCols));
                    }
                } else if (upperDdl.contains("RENAME COLUMN")) {
                    Map<String, String> renameMap = parseRenameColumn(ddlText);
                    if (!renameMap.isEmpty()) {
                        events.add(new org.apache.flink.cdc.common.event.RenameColumnEvent(
                                tableId, renameMap));
                    }
                } else if (upperDdl.contains("ALTER COLUMN") && upperDdl.contains("TYPE")) {
                    Map<String, org.apache.flink.cdc.common.types.DataType> typeMap =
                            parseAlterColumnType(ddlText);
                    if (!typeMap.isEmpty()) {
                        events.add(new org.apache.flink.cdc.common.event.AlterColumnTypeEvent(
                                tableId, typeMap));
                    }
                }
                break;

            case "DROP TABLE":
                events.add(new org.apache.flink.cdc.common.event.DropTableEvent(tableId));
                break;

            case "CREATE TABLE":
                // CREATE TABLE events are already handled by the normal flow
                // Skip to avoid duplicates
                LOG.debug("Skipping DDL-captured CREATE TABLE (handled by normal flow)");
                break;

            default:
                LOG.info("Unhandled DDL tag: {} - {}", tag, ddlText);
                break;
        }

        return events;
    }

    private String extractTableName(String ddl, String upperTag) {
        try {
            String upperDdl = ddl.toUpperCase();
            int tablePos = -1;

            if (upperTag.equals("ALTER TABLE")) {
                tablePos = upperDdl.indexOf("ALTER TABLE") + "ALTER TABLE".length();
            } else if (upperTag.equals("DROP TABLE")) {
                tablePos = upperDdl.indexOf("DROP TABLE") + "DROP TABLE".length();
                // Handle IF EXISTS
                String afterDrop = upperDdl.substring(tablePos).trim();
                if (afterDrop.startsWith("IF EXISTS")) {
                    tablePos = upperDdl.indexOf("IF EXISTS", tablePos) + "IF EXISTS".length();
                }
            } else if (upperTag.equals("CREATE TABLE")) {
                tablePos = upperDdl.indexOf("CREATE TABLE") + "CREATE TABLE".length();
                String afterCreate = upperDdl.substring(tablePos).trim();
                if (afterCreate.startsWith("IF NOT EXISTS")) {
                    tablePos = upperDdl.indexOf("IF NOT EXISTS", tablePos) + "IF NOT EXISTS".length();
                }
            }

            if (tablePos < 0) return null;

            // Extract table name (handle schema.table format)
            String remaining = ddl.substring(tablePos).trim();
            // Take until space or (
            int endPos = remaining.length();
            for (int i = 0; i < remaining.length(); i++) {
                char c = remaining.charAt(i);
                if (c == ' ' || c == '(' || c == ';') {
                    endPos = i;
                    break;
                }
            }
            String fullName = remaining.substring(0, endPos).trim();

            // Remove schema prefix if present (e.g., public.users -> users)
            if (fullName.contains(".")) {
                fullName = fullName.substring(fullName.lastIndexOf('.') + 1);
            }
            // Remove quotes
            fullName = fullName.replace("\"", "").replace("'", "");

            return fullName.isEmpty() ? null : fullName;
        } catch (Exception e) {
            LOG.debug("Failed to extract table name from DDL: {}", ddl);
            return null;
        }
    }

    private List<AddColumnEvent.ColumnWithPosition> parseAddColumns(String ddl) {
        List<AddColumnEvent.ColumnWithPosition> result = new java.util.ArrayList<>();
        try {
            // Pattern: ALTER TABLE xxx ADD COLUMN col_name type
            String upperDdl = ddl.toUpperCase();
            int addPos = upperDdl.indexOf("ADD COLUMN");
            if (addPos < 0) {
                addPos = upperDdl.indexOf("ADD ");
                if (addPos < 0) return result;
                addPos += 4;
            } else {
                addPos += "ADD COLUMN".length();
            }

            // Handle IF NOT EXISTS
            String remaining = ddl.substring(addPos).trim();
            if (remaining.toUpperCase().startsWith("IF NOT EXISTS")) {
                remaining = remaining.substring("IF NOT EXISTS".length()).trim();
            }

            // Extract column name and type
            String[] parts = remaining.split("\\s+", 3);
            if (parts.length >= 2) {
                String colName = parts[0].replace("\"", "");
                String typeName = parts[1];
                // Include precision if present (e.g., VARCHAR(20))
                if (parts.length > 2 && parts[1].endsWith("(")) {
                    typeName = parts[1] + parts[2].split("[,;)\\s]")[0] + ")";
                } else if (remaining.contains("(") && !typeName.contains("(")) {
                    int parenStart = remaining.indexOf('(');
                    int parenEnd = remaining.indexOf(')', parenStart);
                    if (parenEnd > parenStart) {
                        typeName = remaining.substring(parts[0].length(), parenEnd + 1).trim();
                    }
                }

                org.apache.flink.cdc.common.types.DataType dataType = sqlTypeToDataType(typeName);
                Column column = Column.physicalColumn(colName, dataType);
                result.add(AddColumnEvent.last(column));
                LOG.info("Parsed ADD COLUMN: name={}, type={}", colName, typeName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse ADD COLUMN DDL: {}", ddl, e);
        }
        return result;
    }

    private List<String> parseDropColumns(String ddl) {
        List<String> result = new java.util.ArrayList<>();
        try {
            String upperDdl = ddl.toUpperCase();
            int dropPos = upperDdl.indexOf("DROP COLUMN");
            if (dropPos < 0) return result;

            String remaining = ddl.substring(dropPos + "DROP COLUMN".length()).trim();
            if (remaining.toUpperCase().startsWith("IF EXISTS")) {
                remaining = remaining.substring("IF EXISTS".length()).trim();
            }
            String colName = remaining.split("[\\s,;]")[0].replace("\"", "");
            if (!colName.isEmpty()) {
                result.add(colName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse DROP COLUMN DDL: {}", ddl, e);
        }
        return result;
    }

    private Map<String, String> parseRenameColumn(String ddl) {
        Map<String, String> result = new java.util.HashMap<>();
        try {
            String upperDdl = ddl.toUpperCase();
            int renamePos = upperDdl.indexOf("RENAME COLUMN");
            if (renamePos < 0) return result;

            String remaining = ddl.substring(renamePos + "RENAME COLUMN".length()).trim();
            // Pattern: old_name TO new_name
            String[] parts = remaining.split("(?i)\\s+TO\\s+");
            if (parts.length == 2) {
                String oldName = parts[0].trim().replace("\"", "");
                String newName = parts[1].trim().split("[\\s;]")[0].replace("\"", "");
                result.put(oldName, newName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse RENAME COLUMN DDL: {}", ddl, e);
        }
        return result;
    }

    private Map<String, org.apache.flink.cdc.common.types.DataType> parseAlterColumnType(
            String ddl) {
        Map<String, org.apache.flink.cdc.common.types.DataType> result = new java.util.HashMap<>();
        try {
            String upperDdl = ddl.toUpperCase();
            int alterPos = upperDdl.indexOf("ALTER COLUMN");
            if (alterPos < 0) return result;

            String remaining = ddl.substring(alterPos + "ALTER COLUMN".length()).trim();
            // Pattern: col_name TYPE new_type
            String[] parts = remaining.split("(?i)\\s+TYPE\\s+", 2);
            if (parts.length == 2) {
                String colName = parts[0].trim().replace("\"", "");
                String typeName = parts[1].trim().split("[\\s;]")[0];
                result.put(colName, sqlTypeToDataType(typeName));
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse ALTER COLUMN TYPE DDL: {}", ddl, e);
        }
        return result;
    }

    private org.apache.flink.cdc.common.types.DataType sqlTypeToDataType(String sqlType) {
        String upper = sqlType.toUpperCase().trim();
        // Remove parentheses content for matching
        String baseType = upper.replaceAll("\\(.*\\)", "").trim();

        switch (baseType) {
            case "INTEGER":
            case "INT":
            case "INT4":
                return org.apache.flink.cdc.common.types.DataTypes.INT();
            case "BIGINT":
            case "INT8":
                return org.apache.flink.cdc.common.types.DataTypes.BIGINT();
            case "SMALLINT":
            case "INT2":
                return org.apache.flink.cdc.common.types.DataTypes.SMALLINT();
            case "REAL":
            case "FLOAT4":
                return org.apache.flink.cdc.common.types.DataTypes.FLOAT();
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return org.apache.flink.cdc.common.types.DataTypes.DOUBLE();
            case "BOOLEAN":
            case "BOOL":
                return org.apache.flink.cdc.common.types.DataTypes.BOOLEAN();
            case "TEXT":
                return org.apache.flink.cdc.common.types.DataTypes.STRING();
            case "VARCHAR":
            case "CHARACTER VARYING":
                // Try to extract length
                if (upper.contains("(")) {
                    try {
                        int len = Integer.parseInt(
                                upper.replaceAll(".*\\((\\d+)\\).*", "$1"));
                        return org.apache.flink.cdc.common.types.DataTypes.VARCHAR(len);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
                return org.apache.flink.cdc.common.types.DataTypes.STRING();
            case "TIMESTAMP":
            case "TIMESTAMP WITHOUT TIME ZONE":
                return org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(6);
            case "TIMESTAMPTZ":
            case "TIMESTAMP WITH TIME ZONE":
                return org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(6);
            case "DATE":
                return org.apache.flink.cdc.common.types.DataTypes.DATE();
            case "TIME":
            case "TIME WITHOUT TIME ZONE":
                return org.apache.flink.cdc.common.types.DataTypes.TIME(0);
            case "BYTEA":
                return org.apache.flink.cdc.common.types.DataTypes.BYTES();
            case "NUMERIC":
            case "DECIMAL":
                if (upper.contains("(")) {
                    try {
                        String params = upper.replaceAll(".*\\((.*)\\).*", "$1");
                        String[] pv = params.split(",");
                        int p = Integer.parseInt(pv[0].trim());
                        int s = pv.length > 1 ? Integer.parseInt(pv[1].trim()) : 0;
                        return org.apache.flink.cdc.common.types.DataTypes.DECIMAL(p, s);
                    } catch (Exception e) {
                        // ignore
                    }
                }
                return org.apache.flink.cdc.common.types.DataTypes.DECIMAL(38, 10);
            default:
                // Default to STRING for unknown types
                return org.apache.flink.cdc.common.types.DataTypes.STRING();
        }
    }
}
