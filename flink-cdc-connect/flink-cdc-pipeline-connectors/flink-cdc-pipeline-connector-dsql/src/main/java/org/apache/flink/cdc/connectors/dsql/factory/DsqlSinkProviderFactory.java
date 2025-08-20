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

package org.apache.flink.cdc.connectors.dsql.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.dsql.sink.DsqlSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.CONNECTION_IDLE_TIMEOUT_MS;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.CONNECTION_MAX_LIFETIME_MS;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.DATABASE;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.ENABLE_FULL_LOAD;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.HOST;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.IAM_ROLE;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.MAX_POOL_SIZE;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.MIN_POOL_SIZE;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.PARALLELISM;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.PORT;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.REGION;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.USE_IAM_AUTH;

/**
 * Factory for creating DSQL data sinks.
 * Handles configuration validation and creates DsqlSink instances.
 */
public class DsqlSinkProviderFactory implements DataSinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlSinkProviderFactory.class);

    @Override
    public DataSink createDataSink(Context context) {
        Configuration config = context.getFactoryConfiguration();
        
        // Validate required options
        validateConfiguration(config);

        LOG.info("Creating DSQL data sink with host: {}, database: {}",
                config.get(HOST), config.get(DATABASE));

        return new DsqlSink(config);
    }

    @Override
    public String identifier() {
        return "dsql";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(DATABASE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(USE_IAM_AUTH);
        options.add(IAM_ROLE);
        options.add(REGION);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(MAX_POOL_SIZE);
        options.add(MIN_POOL_SIZE);
        options.add(CONNECTION_MAX_LIFETIME_MS);
        options.add(CONNECTION_IDLE_TIMEOUT_MS);
        options.add(ENABLE_FULL_LOAD);
        options.add(PARALLELISM);
        return options;
    }

    /**
     * Validates the configuration for required and logical consistency.
     */
    private void validateConfiguration(Configuration config) {
        // Check required options
        if (config.get(HOST) == null || config.get(HOST).trim().isEmpty()) {
            throw new IllegalArgumentException("DSQL host is required");
        }

        if (config.get(DATABASE) == null || config.get(DATABASE).trim().isEmpty()) {
            throw new IllegalArgumentException("DSQL database name is required");
        }

        // Validate authentication configuration
        Boolean useIamAuth = config.get(USE_IAM_AUTH);
        if (useIamAuth != null && !useIamAuth) {
            // If not using IAM auth, username and password are required
            if (config.get(USERNAME) == null || config.get(PASSWORD) == null) {
                throw new IllegalArgumentException(
                    "Username and password are required when IAM authentication is disabled");
            }
        }

        // Validate pool size settings
        Integer maxPoolSize = config.get(MAX_POOL_SIZE);
        Integer minPoolSize = config.get(MIN_POOL_SIZE);
        if (maxPoolSize != null && minPoolSize != null && minPoolSize > maxPoolSize) {
            throw new IllegalArgumentException(
                "Minimum pool size cannot be greater than maximum pool size");
        }

        LOG.debug("Configuration validation passed");
    }
}
