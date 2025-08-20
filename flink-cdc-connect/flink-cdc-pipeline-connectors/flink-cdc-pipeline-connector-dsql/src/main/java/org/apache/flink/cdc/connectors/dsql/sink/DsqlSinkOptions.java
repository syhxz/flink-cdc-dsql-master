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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/**
 * Configuration options for Amazon DSQL sink connector.
 */
public class DsqlSinkOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname of the Amazon DSQL cluster endpoint.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Port number of the Amazon DSQL cluster.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the Amazon DSQL database.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username for database authentication.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for database authentication.");

    public static final ConfigOption<Boolean> USE_IAM_AUTH =
            ConfigOptions.key("use-iam-auth")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to use IAM authentication for connecting to DSQL.");

    public static final ConfigOption<String> REGION =
            ConfigOptions.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region where the DSQL cluster is located.");

    public static final ConfigOption<String> IAM_ROLE =
            ConfigOptions.key("iam-role")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IAM role ARN to assume for DSQL access.");

    public static final ConfigOption<Integer> MAX_POOL_SIZE =
            ConfigOptions.key("max-pool-size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Maximum number of connections in the connection pool.");

    public static final ConfigOption<Integer> MIN_POOL_SIZE =
            ConfigOptions.key("min-pool-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Minimum number of connections in the connection pool.");

    public static final ConfigOption<Long> CONNECTION_MAX_LIFETIME_MS =
            ConfigOptions.key("connection-max-lifetime-ms")
                    .longType()
                    .defaultValue(3540000L) // 59 minutes
                    .withDescription("Maximum lifetime of a connection in milliseconds.");

    public static final ConfigOption<Long> CONNECTION_IDLE_TIMEOUT_MS =
            ConfigOptions.key("connection-idle-timeout-ms")
                    .longType()
                    .defaultValue(300000L) // 5 minutes
                    .withDescription("Maximum idle time for a connection in milliseconds.");

    // Batch processing configuration options
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of events to batch before committing to DSQL.");

    public static final ConfigOption<String> BATCH_TIMEOUT =
            ConfigOptions.key("batch-timeout")
                    .stringType()
                    .defaultValue("5min")
                    .withDescription("Maximum time to wait before committing a partial batch (e.g., '5min', '30s').");

    public static final ConfigOption<Integer> BATCH_MAX_POOL_SIZE =
            ConfigOptions.key("batch-max-pool-size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Maximum number of connections in the batch processing connection pool.");

    public static final ConfigOption<Integer> BATCH_MIN_POOL_SIZE =
            ConfigOptions.key("batch-min-pool-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Minimum number of connections in the batch processing connection pool.");

    public static final ConfigOption<Boolean> ENABLE_FULL_LOAD =
            ConfigOptions.key("enable-full-load")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable full load mode for initial data synchronization.");

    public static final ConfigOption<Integer> PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Degree of parallelism for full load operations.");
}
