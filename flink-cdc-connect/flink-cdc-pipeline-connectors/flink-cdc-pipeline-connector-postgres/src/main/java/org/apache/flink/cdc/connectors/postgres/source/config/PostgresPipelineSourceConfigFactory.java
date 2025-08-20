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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.apache.flink.cdc.common.annotation.Internal;

/** Factory for creating PostgreSQL source configuration for pipeline connector. */
@Internal
public class PostgresPipelineSourceConfigFactory {
    private final org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory cdcFactory;
    private String databaseName;
    private String schemaName;
    private String slotName;
    private StartupOptions pipelineStartupOptions;
    
    public PostgresPipelineSourceConfigFactory() {
        this.cdcFactory = new org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory();
    }

    public PostgresPipelineSourceConfigFactory hostname(String hostname) {
        cdcFactory.hostname(hostname);
        return this;
    }

    public PostgresPipelineSourceConfigFactory port(int port) {
        cdcFactory.port(port);
        return this;
    }

    public PostgresPipelineSourceConfigFactory username(String username) {
        cdcFactory.username(username);
        return this;
    }

    public PostgresPipelineSourceConfigFactory password(String password) {
        cdcFactory.password(password);
        return this;
    }

    public PostgresPipelineSourceConfigFactory databaseList(String databaseName) {
        this.databaseName = databaseName;
        cdcFactory.database(databaseName);
        return this;
    }

    public PostgresPipelineSourceConfigFactory schemaName(String schemaName) {
        this.schemaName = schemaName;
        if (schemaName != null && !schemaName.isEmpty()) {
            cdcFactory.schemaList(new String[]{schemaName});
        }
        return this;
    }

    public PostgresPipelineSourceConfigFactory tableList(String... tableList) {
        cdcFactory.tableList(tableList);
        return this;
    }

    public PostgresPipelineSourceConfigFactory slotName(String slotName) {
        this.slotName = slotName;
        System.out.println("DEBUG: PostgresPipelineSourceConfigFactory.slotName() called with: " + slotName);
        cdcFactory.slotName(slotName);
        return this;
    }

    public PostgresPipelineSourceConfigFactory decodingPluginName(String decodingPluginName) {
        System.out.println("DEBUG: PostgresPipelineSourceConfigFactory.decodingPluginName() called with: " + decodingPluginName);
        cdcFactory.decodingPluginName(decodingPluginName);
        return this;
    }

    public PostgresPipelineSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        this.pipelineStartupOptions = startupOptions;
        // Convert pipeline startup options to CDC startup options
        cdcFactory.startupOptions(convertStartupOptions(startupOptions));
        return this;
    }

    /**
     * Convert pipeline connector's StartupOptions to CDC connector's StartupOptions.
     */
    private org.apache.flink.cdc.connectors.base.options.StartupOptions convertStartupOptions(StartupOptions startupOptions) {
        switch (startupOptions.getMode()) {
            case SNAPSHOT:
                return org.apache.flink.cdc.connectors.base.options.StartupOptions.initial();
            case LATEST_OFFSET:
                return org.apache.flink.cdc.connectors.base.options.StartupOptions.latest();
            case SPECIFIC_OFFSET:
                // For PostgreSQL, specific offset is typically an LSN (Log Sequence Number)
                // The CDC connector expects both file and position, but PostgreSQL LSN is a single value
                // We'll use the LSN as the file parameter and 0 as position
                return org.apache.flink.cdc.connectors.base.options.StartupOptions.specificOffset(startupOptions.getSpecificOffset(), 0);
            case TIMESTAMP:
                return org.apache.flink.cdc.connectors.base.options.StartupOptions.timestamp(startupOptions.getTimestamp());
            default:
                throw new IllegalArgumentException("Unsupported startup mode: " + startupOptions.getMode());
        }
    }

    public org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig create(int subtaskId) {
        return cdcFactory.create(subtaskId);
    }
}