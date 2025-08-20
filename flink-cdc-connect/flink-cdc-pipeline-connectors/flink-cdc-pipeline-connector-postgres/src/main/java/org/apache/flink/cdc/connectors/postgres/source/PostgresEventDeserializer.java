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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
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
            boolean includeSchemaChanges, 
            PostgresSchemaDataTypeInference schemaDataTypeInference) {
        super(schemaDataTypeInference, DebeziumChangelogMode.ALL);
        this.includeSchemaChanges = includeSchemaChanges;
        LOG.info("=== PostgresEventDeserializer INITIALIZED with includeSchemaChanges: {} ===", includeSchemaChanges);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Event> out) throws Exception {
        LOG.info("=== DESERIALIZE: topic={}, partition={}, offset={} ===", 
                record.topic(), record.sourcePartition(), record.sourceOffset());
        
        LOG.debug("Deserializing record: topic={}, key={}, value={}", 
                record.topic(), record.key(), record.value());
        
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
                    LOG.info("  Field '{}': type={}, value={}", field.name(), field.schema().type(), fieldValue);
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
            
            // Add schema validation before processing
            validateRecordSchema(record);
            LOG.info("=== CALLING SUPER.DESERIALIZE FOR DATA CHANGE ===");
            super.deserialize(record, out);
            LOG.info("=== SUPER.DESERIALIZE COMPLETED ===");
        } else {
            LOG.info("=== UNKNOWN RECORD TYPE - SKIPPING: topic={} ===", record.topic());
            LOG.debug("Skipping record that is neither schema change nor data change: {}", record.topic());
        }
    }
    

    
    private void validateRecordSchema(SourceRecord record) {
        if (record.valueSchema() == null) {
            LOG.warn("Record has null value schema: topic={}", record.topic());
        } else {
            LOG.debug("Record schema: topic={}, schema={}", record.topic(), record.valueSchema().name());
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
                org.apache.flink.cdc.common.event.TableId tableId = org.apache.flink.cdc.common.event.TableId.tableId(database, schema, table);
                LOG.debug("Extracted table ID from topic: {}", tableId);
                return tableId;
            } else {
                LOG.warn("Topic format unexpected, expected serverName.schemaName.tableName but got: {}", record.topic());
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
                        org.apache.flink.cdc.common.event.TableId tableId = org.apache.flink.cdc.common.event.TableId.tableId(database, schema, table);
                        LOG.debug("Extracted table ID from source struct: {}", tableId);
                        return tableId;
                    } else {
                        LOG.warn("Source struct missing required fields: db={}, schema={}, table={}", 
                                database, schema, table);
                    }
                } else {
                    LOG.warn("Source struct is null in record value");
                }
            } else {
                LOG.warn("No 'source' field found in record value schema");
            }
        } else {
            LOG.warn("Record value is not a Struct: {}", record.value() != null ? record.value().getClass() : "null");
        }
        
        // Default fallback
        org.apache.flink.cdc.common.event.TableId fallbackTableId = org.apache.flink.cdc.common.event.TableId.tableId("postgres_cdc_source", "public", "unknown");
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
            org.apache.flink.cdc.common.event.TableId tableId = org.apache.flink.cdc.common.event.TableId.tableId(databaseName, "public", "unknown");
            
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
     * Emit a CreateTableEvent by creating a basic schema from the record structure.
     * This ensures the SchemaOperator has schema information before processing data events.
     */
    private void emitCreateTableEventFromRecord(SourceRecord record, TableId tableId, Collector<Event> out) {
        try {
            // Create a basic schema from the record's value schema
            Schema schema = createSchemaFromRecord(record);
            if (schema != null) {
                CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
                out.collect(createTableEvent);
                LOG.info("Emitted CreateTableEvent for table: {} with {} columns", 
                        tableId, schema.getColumns().size());
            } else {
                LOG.warn("Could not create schema for table: {} from record", tableId);
            }
        } catch (Exception e) {
            LOG.error("Failed to emit CreateTableEvent for table: {}", tableId, e);
        }
    }
    
    /**
     * Create a basic Schema from a Debezium SourceRecord.
     * This extracts column information from the record's value schema.
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
            LOG.info("Creating schema from {} fields in 'after' schema", afterSchema.fields().size());
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
}