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

package org.apache.flink.cdc.connectors.postgres.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDataSource;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresPipelineSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresTableNameFormatter;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresValidationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** A {@link DataSourceFactory} to create PostgreSQL data source. */
@Internal
public class PostgresDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataSourceFactory.class);

    public static final String IDENTIFIER = "postgres";

    // Configuration options
    public static final ConfigOption<String> HOSTNAME =
            key("hostname").stringType().noDefaultValue().withDescription("PostgreSQL hostname");

    public static final ConfigOption<Integer> PORT =
            key("port").intType().defaultValue(5432).withDescription("PostgreSQL port");

    public static final ConfigOption<String> USERNAME =
            key("username").stringType().noDefaultValue().withDescription("PostgreSQL username");

    public static final ConfigOption<String> PASSWORD =
            key("password").stringType().noDefaultValue().withDescription("PostgreSQL password");

    public static final ConfigOption<String> DATABASE_NAME =
            key("database-name").stringType().noDefaultValue().withDescription("PostgreSQL database name");

    public static final ConfigOption<String> SCHEMA_NAME =
            key("schema-name").stringType().defaultValue("public").withDescription("PostgreSQL schema name");

    public static final ConfigOption<String> TABLES =
            key("tables").stringType().noDefaultValue().withDescription("PostgreSQL table names");

    public static final ConfigOption<String> SLOT_NAME =
            key("slot.name").stringType().defaultValue("flink_cdc_slot").withDescription("PostgreSQL replication slot name");

    // Startup mode configuration options
    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            key("scan.startup.mode")
                    .stringType()
                    .defaultValue("snapshot")
                    .withDescription("Startup mode for PostgreSQL CDC source (snapshot, latest-offset, specific-offset, timestamp)");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET =
            key("scan.startup.specific-offset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specific offset for startup when using specific-offset mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Timestamp in milliseconds for startup when using timestamp mode");

    public static final ConfigOption<String> DECODING_PLUGIN_NAME =
            key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("pgoutput")
                    .withDescription("PostgreSQL logical decoding plugin name (pgoutput, wal2json, etc.)");

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();

        final Configuration config = context.getFactoryConfiguration();
        
        try {
            // Validate configuration parameters
            LOG.info("Validating PostgreSQL configuration");
            PostgresValidationUtils.validateConfiguration(config);
            
            // Test database connectivity
            LOG.info("Testing PostgreSQL database connectivity");
            PostgresValidationUtils.testConnection(config);
            
        } catch (Exception e) {
            LOG.error("PostgreSQL configuration validation failed", e);
            throw new IllegalArgumentException("PostgreSQL configuration validation failed: " + e.getMessage(), e);
        }
        
        // Extract configuration
        String hostname = config.get(HOSTNAME);
        int port = config.get(PORT);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tables = config.get(TABLES);
        String slotName = config.get(SLOT_NAME);
        String decodingPluginName = config.get(DECODING_PLUGIN_NAME);

        LOG.info("PostgreSQL configuration - hostname: {}, port: {}, database: {}, schema: {}, tables: {}, slot: {}, decoding plugin: {}", 
                hostname, port, databaseName, schemaName, tables, slotName, decodingPluginName);
        
        // Debug: Log the raw configuration to see what we're actually getting
        LOG.info("Raw configuration values from config object:");
        LOG.info("  HOSTNAME: {}", config.get(HOSTNAME));
        LOG.info("  PORT: {}", config.get(PORT));
        LOG.info("  DATABASE_NAME: {}", config.get(DATABASE_NAME));
        LOG.info("  SLOT_NAME: {}", config.get(SLOT_NAME));
        LOG.info("  DECODING_PLUGIN_NAME: {}", config.get(DECODING_PLUGIN_NAME));

        try {
            // Parse startup options
            StartupOptions startupOptions = parseStartupOptions(config);

            // Format table list using utility class
            String[] tableList = PostgresTableNameFormatter.formatTableList(schemaName, tables);
            LOG.info("Formatted table list: {}", Arrays.toString(tableList));

            LOG.info("Successfully created PostgreSQL source for database: {}, tables: {}", 
                    databaseName, Arrays.toString(tableList));
            
            // Create config factory for the pipeline connector
            PostgresPipelineSourceConfigFactory configFactory = new PostgresPipelineSourceConfigFactory();
            
            // Configure the factory with our parameters
            configFactory.hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseName)
                        .schemaName(schemaName)
                        .tableList(tableList)
                        .slotName(slotName)
                        .decodingPluginName(decodingPluginName)
                        .startupOptions(startupOptions);
            
            return new PostgresDataSource(configFactory);
            
        } catch (Exception e) {
            LOG.error("Failed to create PostgresDataSource for database: {}", databaseName, e);
            throw new RuntimeException("Failed to create PostgresDataSource: " + e.getMessage(), e);
        }
    }

    /**
     * Parse startup options from configuration.
     */
    private StartupOptions parseStartupOptions(Configuration config) {
        String startupMode = config.get(SCAN_STARTUP_MODE);
        
        switch (startupMode.toLowerCase()) {
            case "snapshot":
                return StartupOptions.snapshot();
            case "latest-offset":
                return StartupOptions.latestOffset();
            case "specific-offset":
                String offset = config.get(SCAN_STARTUP_SPECIFIC_OFFSET);
                if (offset == null) {
                    throw new IllegalArgumentException(
                            "scan.startup.specific-offset must be specified when using specific-offset mode");
                }
                // For PostgreSQL, the offset is typically an LSN (Log Sequence Number)
                // We'll use the offset as-is since our method only takes a single string parameter
                return StartupOptions.specificOffset(offset);
            case "timestamp":
                Long timestamp = config.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
                if (timestamp == null) {
                    throw new IllegalArgumentException(
                            "scan.startup.timestamp-millis must be specified when using timestamp mode");
                }
                return StartupOptions.timestamp(timestamp);
            default:
                throw new IllegalArgumentException("Unsupported startup mode: " + startupMode);
        }
    }



    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SCHEMA_NAME);
        options.add(SLOT_NAME);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(DECODING_PLUGIN_NAME);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}