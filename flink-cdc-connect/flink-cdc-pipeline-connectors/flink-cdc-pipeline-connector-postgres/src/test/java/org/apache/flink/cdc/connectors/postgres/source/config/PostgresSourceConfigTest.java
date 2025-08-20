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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PostgresSourceConfig}. */
class PostgresSourceConfigTest {

    @Test
    void testConfigCreation() {
        String hostname = "localhost";
        int port = 5432;
        String username = "postgres";
        String password = "password";
        String databaseName = "testdb";
        String schemaName = "public";
        List<String> tableList = Arrays.asList("public.users", "public.orders");
        String slotName = "flink_cdc_slot";
        StartupOptions startupOptions = StartupOptions.snapshot();

        PostgresSourceConfig config = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, startupOptions);

        assertThat(config.getHostname()).isEqualTo(hostname);
        assertThat(config.getPort()).isEqualTo(port);
        assertThat(config.getUsername()).isEqualTo(username);
        assertThat(config.getPassword()).isEqualTo(password);
        assertThat(config.getDatabaseName()).isEqualTo(databaseName);
        assertThat(config.getSchemaName()).isEqualTo(schemaName);
        assertThat(config.getTableList()).isEqualTo(tableList);
        assertThat(config.getSlotName()).isEqualTo(slotName);
        assertThat(config.getStartupOptions()).isEqualTo(startupOptions);
    }

    @Test
    void testEqualsAndHashCode() {
        String hostname = "localhost";
        int port = 5432;
        String username = "postgres";
        String password = "password";
        String databaseName = "testdb";
        String schemaName = "public";
        List<String> tableList = Arrays.asList("public.users", "public.orders");
        String slotName = "flink_cdc_slot";
        StartupOptions startupOptions = StartupOptions.snapshot();

        PostgresSourceConfig config1 = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, startupOptions);

        PostgresSourceConfig config2 = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, startupOptions);

        PostgresSourceConfig config3 = new PostgresSourceConfig(
                "different-host", port, username, password, databaseName,
                schemaName, tableList, slotName, startupOptions);

        assertThat(config1).isEqualTo(config2);
        assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        assertThat(config1).isNotEqualTo(config3);
        assertThat(config1.hashCode()).isNotEqualTo(config3.hashCode());
    }

    @Test
    void testToString() {
        String hostname = "localhost";
        int port = 5432;
        String username = "postgres";
        String password = "password";
        String databaseName = "testdb";
        String schemaName = "public";
        List<String> tableList = Arrays.asList("public.users", "public.orders");
        String slotName = "flink_cdc_slot";
        StartupOptions startupOptions = StartupOptions.snapshot();

        PostgresSourceConfig config = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, startupOptions);

        String configString = config.toString();
        assertThat(configString).contains(hostname);
        assertThat(configString).contains(String.valueOf(port));
        assertThat(configString).contains(username);
        assertThat(configString).contains(databaseName);
        assertThat(configString).contains(schemaName);
        assertThat(configString).contains(slotName);
        // Password should not be in toString for security
        assertThat(configString).doesNotContain(password);
    }

    @Test
    void testConfigWithDifferentStartupOptions() {
        String hostname = "localhost";
        int port = 5432;
        String username = "postgres";
        String password = "password";
        String databaseName = "testdb";
        String schemaName = "public";
        List<String> tableList = Arrays.asList("public.users");
        String slotName = "flink_cdc_slot";

        // Test with different startup options
        PostgresSourceConfig snapshotConfig = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, StartupOptions.snapshot());

        PostgresSourceConfig latestConfig = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, StartupOptions.latestOffset());

        PostgresSourceConfig specificConfig = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, StartupOptions.specificOffset("0/1234567"));

        PostgresSourceConfig timestampConfig = new PostgresSourceConfig(
                hostname, port, username, password, databaseName,
                schemaName, tableList, slotName, StartupOptions.timestamp(System.currentTimeMillis()));

        assertThat(snapshotConfig).isNotEqualTo(latestConfig);
        assertThat(snapshotConfig).isNotEqualTo(specificConfig);
        assertThat(snapshotConfig).isNotEqualTo(timestampConfig);
        assertThat(latestConfig).isNotEqualTo(specificConfig);
        assertThat(latestConfig).isNotEqualTo(timestampConfig);
        assertThat(specificConfig).isNotEqualTo(timestampConfig);
    }
}