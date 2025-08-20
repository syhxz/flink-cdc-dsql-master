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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresPipelineSourceConfigFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** PostgreSQL data source implementation for Flink CDC pipeline. */
@Internal
public class PostgresDataSource implements DataSource {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataSource.class);

    private final PostgresPipelineSourceConfigFactory configFactory;
    private final String slotName;
    private final String decodingPluginName;

    public PostgresDataSource(PostgresPipelineSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        
        // Extract configuration values when creating the data source
        org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig cdcConfig = configFactory.create(0);
        
        // Extract slot name and plugin name using helper methods
        this.slotName = extractSlotName(cdcConfig);
        this.decodingPluginName = extractDecodingPluginName(cdcConfig);
        
        LOG.info("Created PostgresDataSource with slot: {}, plugin: {}", slotName, decodingPluginName);
    }
    
    private String extractSlotName(org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig cdcConfig) {
        try {
            String value = cdcConfig.getDbzConnectorConfig().getConfig().getString("slot.name");
            return value != null ? value : "flink";
        } catch (Exception e) {
            return "flink"; // fallback to default
        }
    }
    
    private String extractDecodingPluginName(org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig cdcConfig) {
        try {
            String value = cdcConfig.getDbzConnectorConfig().getConfig().getString("plugin.name");
            return value != null ? value : "decoderbufs";
        } catch (Exception e) {
            return "decoderbufs"; // fallback to default
        }
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        // Create PostgreSQL source configuration
        org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig sourceConfig = configFactory.create(0);
        
        // Add extensive debug logging
        LOG.info("=== POSTGRES DATA SOURCE CONFIGURATION ===");
        LOG.info("Hostname: {}", sourceConfig.getHostname());
        LOG.info("Port: {}", sourceConfig.getPort());
        LOG.info("Username: {}", sourceConfig.getUsername());
        LOG.info("Database: {}", sourceConfig.getDatabaseList());
        LOG.info("Slot Name: {}", this.slotName);
        LOG.info("Decoding Plugin: {}", this.decodingPluginName);
        LOG.info("Startup Options: {}", sourceConfig.getStartupOptions());
        LOG.info("Table List: {}", sourceConfig.getTableList());
        
        // Check if startup options are properly configured for snapshot
        if (sourceConfig.getStartupOptions() != null) {
            LOG.info("Startup Mode: {}", sourceConfig.getStartupOptions().startupMode);
        } else {
            LOG.warn("WARNING: Startup options are NULL!");
        }
        
        // Create the event deserializer
        PostgresEventDeserializer deserializer = new PostgresEventDeserializer(
                true, // include schema changes
                new PostgresSchemaDataTypeInference()
        );

        // Use the standard PostgreSQL source from the CDC connector
        org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource<Event> source =
                org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource.<Event>builder()
                        .hostname(sourceConfig.getHostname())
                        .port(sourceConfig.getPort())
                        .username(sourceConfig.getUsername())
                        .password(sourceConfig.getPassword())
                        .database(sourceConfig.getDatabaseList().get(0))
                        .tableList(sourceConfig.getTableList().toArray(new String[0])) // CRITICAL: Add table list
                        .slotName(this.slotName)
                        .decodingPluginName(this.decodingPluginName)
                        .deserializer(deserializer)
                        .startupOptions(sourceConfig.getStartupOptions())
                        .build();

        LOG.info("=== POSTGRES SOURCE CREATED SUCCESSFULLY ===");
        return FlinkSourceProvider.of(source);
    }
    




    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new PostgresMetadataAccessor(configFactory);
    }

    /** Metadata accessor for PostgreSQL. */
    private static class PostgresMetadataAccessor implements MetadataAccessor {
        
        private final PostgresPipelineSourceConfigFactory configFactory;
        private final PostgresSchemaDiscovery schemaDiscovery;

        public PostgresMetadataAccessor(PostgresPipelineSourceConfigFactory configFactory) {
            this.configFactory = configFactory;
            // Create a config instance for schema discovery
            org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig config = configFactory.create(0);
            this.schemaDiscovery = createSchemaDiscovery(config);
        }

        @Override
        public List<String> listNamespaces() {
            throw new UnsupportedOperationException("List namespace is not supported by PostgreSQL.");
        }

        @Override
        public List<String> listSchemas(@Nullable String namespace) {
            // For PostgreSQL, we'll extract schema information from table filters
            // For now, return a default schema
            return java.util.Collections.singletonList("public");
        }

        @Override
        public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
            try {
                return schemaDiscovery.discoverTables();
            } catch (Exception e) {
                throw new RuntimeException("Failed to list tables", e);
            }
        }

        @Override
        public Schema getTableSchema(TableId tableId) {
            try {
                return schemaDiscovery.getTableSchema(tableId);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get table schema for " + tableId, e);
            }
        }
        
        private PostgresSchemaDiscovery createSchemaDiscovery(org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig config) {
            return new PostgresSchemaDiscovery(config);
        }
    }
}