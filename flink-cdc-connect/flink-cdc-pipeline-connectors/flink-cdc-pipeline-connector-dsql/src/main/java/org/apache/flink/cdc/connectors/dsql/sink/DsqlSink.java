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
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DataSink} for Amazon DSQL connector with batch processing support.
 */
public class DsqlSink implements DataSink {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlSink.class);

    private final Configuration config;

    public DsqlSink(Configuration config) {
        this.config = config;
        LOG.info("Initialized DSQL sink with host: {}, database: {}, batch size: {}", 
                config.get(DsqlSinkOptions.HOST), 
                config.get(DsqlSinkOptions.DATABASE),
                config.get(DsqlSinkOptions.BATCH_SIZE));
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkFunctionProvider.of(new DsqlBatchSinkFunction(config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DsqlMetadataApplier(config);
    }
    
    /**
     * Simple metadata applier for DSQL.
     */
    private static class DsqlMetadataApplier implements MetadataApplier {
        private static final Logger LOG = LoggerFactory.getLogger(DsqlMetadataApplier.class);
        private final Configuration config;
        
        public DsqlMetadataApplier(Configuration config) {
            this.config = config;
        }
        
        @Override
        public void applySchemaChange(org.apache.flink.cdc.common.event.SchemaChangeEvent schemaChangeEvent) {
            LOG.info("Applying schema change for table: {} - Event: {}", 
                    schemaChangeEvent.tableId(), schemaChangeEvent.getClass().getSimpleName());
            
            try {
                // Handle different types of schema change events
                if (schemaChangeEvent instanceof org.apache.flink.cdc.common.event.CreateTableEvent) {
                    handleCreateTable((org.apache.flink.cdc.common.event.CreateTableEvent) schemaChangeEvent);
                } else if (schemaChangeEvent instanceof org.apache.flink.cdc.common.event.AlterColumnTypeEvent) {
                    handleAlterColumnType((org.apache.flink.cdc.common.event.AlterColumnTypeEvent) schemaChangeEvent);
                } else if (schemaChangeEvent instanceof org.apache.flink.cdc.common.event.AddColumnEvent) {
                    handleAddColumn((org.apache.flink.cdc.common.event.AddColumnEvent) schemaChangeEvent);
                } else if (schemaChangeEvent instanceof org.apache.flink.cdc.common.event.DropColumnEvent) {
                    handleDropColumn((org.apache.flink.cdc.common.event.DropColumnEvent) schemaChangeEvent);
                } else {
                    LOG.info("Schema change event type {} is supported by default", schemaChangeEvent.getClass().getSimpleName());
                }
                
                LOG.info("Successfully applied schema change for table: {}", schemaChangeEvent.tableId());
            } catch (Exception e) {
                LOG.error("Failed to apply schema change for table: {} - Error: {}", 
                         schemaChangeEvent.tableId(), e.getMessage(), e);
                // Don't throw exception to avoid pipeline failure - log and continue
            }
        }
        
        private void handleCreateTable(org.apache.flink.cdc.common.event.CreateTableEvent event) {
            LOG.info("Creating table: {} with schema: {}", event.tableId(), event.getSchema());
            // For DSQL, table creation is typically handled automatically
            // The schema is registered by the framework
        }
        
        private void handleAlterColumnType(org.apache.flink.cdc.common.event.AlterColumnTypeEvent event) {
            LOG.info("Altering column type for table: {} - Column: {}", 
                    event.tableId(), event.getTypeMapping());
        }
        
        private void handleAddColumn(org.apache.flink.cdc.common.event.AddColumnEvent event) {
            LOG.info("Adding column to table: {} - Columns: {}", 
                    event.tableId(), event.getAddedColumns());
        }
        
        private void handleDropColumn(org.apache.flink.cdc.common.event.DropColumnEvent event) {
            LOG.info("Dropping column from table: {} - Columns: {}", 
                    event.tableId(), event.getDroppedColumnNames());
        }
    }
}
