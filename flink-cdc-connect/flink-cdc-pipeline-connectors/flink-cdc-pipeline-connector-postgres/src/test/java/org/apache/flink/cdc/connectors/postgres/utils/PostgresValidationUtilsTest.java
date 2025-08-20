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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PostgresValidationUtils}. */
class PostgresValidationUtilsTest {

    @Test
    void testValidConfigurationPasses() {
        Configuration config = createValidConfiguration();

        // Should not throw any exception
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
    }

    @Test
    void testMissingHostname() {
        Configuration config = createConfigurationWithout(PostgresDataSourceFactory.HOSTNAME);
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hostname");
    }

    @Test
    void testMissingUsername() {
        Configuration config = createConfigurationWithout(PostgresDataSourceFactory.USERNAME);
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username");
    }

    @Test
    void testMissingPassword() {
        Configuration config = createConfigurationWithout(PostgresDataSourceFactory.PASSWORD);
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void testMissingDatabaseName() {
        Configuration config = createConfigurationWithout(PostgresDataSourceFactory.DATABASE_NAME);
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("database-name");
    }

    @Test
    void testMissingTables() {
        Configuration config = createConfigurationWithout(PostgresDataSourceFactory.TABLES);
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tables");
    }

    @Test
    void testInvalidPortRange() {
        Configuration config = createValidConfiguration();
        config.set(PostgresDataSourceFactory.PORT, 0);
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Port must be between");

        config.set(PostgresDataSourceFactory.PORT, 70000);
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Port must be between");
    }

    @Test
    void testInvalidSchemaName() {
        Configuration config = createValidConfiguration();
        config.set(PostgresDataSourceFactory.SCHEMA_NAME, "123invalid");
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid schema name");
    }

    @Test
    void testInvalidSlotName() {
        Configuration config = createValidConfiguration();
        config.set(PostgresDataSourceFactory.SLOT_NAME, "Invalid-Slot-Name");
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid slot name");
    }

    @Test
    void testValidSlotNames() {
        Configuration config = createValidConfiguration();
        
        // Valid slot names
        config.set(PostgresDataSourceFactory.SLOT_NAME, "flink_cdc_slot");
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
        
        config.set(PostgresDataSourceFactory.SLOT_NAME, "slot123");
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
        
        config.set(PostgresDataSourceFactory.SLOT_NAME, "my_slot_name");
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
    }

    @Test
    void testStartupModeValidation() {
        Configuration config = createValidConfiguration();
        
        // Valid startup modes
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
        
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "latest-offset");
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
        
        // Invalid startup mode
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "invalid-mode");
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported startup mode");
    }

    @Test
    void testSpecificOffsetModeValidation() {
        Configuration config = createValidConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "specific-offset");
        
        // Missing specific offset
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scan.startup.specific-offset must be specified");
        
        // Valid specific offset
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_SPECIFIC_OFFSET, "0/1234567");
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
    }

    @Test
    void testTimestampModeValidation() {
        Configuration config = createValidConfiguration();
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "timestamp");
        
        // Missing timestamp
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scan.startup.timestamp-millis must be a positive number");
        
        // Invalid timestamp (zero)
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, 0L);
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scan.startup.timestamp-millis must be a positive number");
        
        // Invalid timestamp (negative)
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, -1L);
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scan.startup.timestamp-millis must be a positive number");
        
        // Valid timestamp
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_TIMESTAMP_MILLIS, System.currentTimeMillis());
        assertThatCode(() -> PostgresValidationUtils.validateConfiguration(config)).doesNotThrowAnyException();
    }

    @Test
    void testInvalidTableConfiguration() {
        Configuration config = createValidConfiguration();
        config.set(PostgresDataSourceFactory.TABLES, "schema.table.extra");
        
        assertThatThrownBy(() -> PostgresValidationUtils.validateConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid table configuration");
    }

    private Configuration createValidConfiguration() {
        Configuration config = new Configuration();
        config.set(PostgresDataSourceFactory.HOSTNAME, "localhost");
        config.set(PostgresDataSourceFactory.PORT, 5432);
        config.set(PostgresDataSourceFactory.USERNAME, "postgres");
        config.set(PostgresDataSourceFactory.PASSWORD, "password");
        config.set(PostgresDataSourceFactory.DATABASE_NAME, "testdb");
        config.set(PostgresDataSourceFactory.SCHEMA_NAME, "public");
        config.set(PostgresDataSourceFactory.TABLES, "users,orders");
        config.set(PostgresDataSourceFactory.SLOT_NAME, "flink_cdc_slot");
        config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        return config;
    }

    private Configuration createConfigurationWithout(org.apache.flink.cdc.common.configuration.ConfigOption<?> excludeOption) {
        Configuration config = new Configuration();
        
        // Add all required parameters except the excluded one
        if (!excludeOption.equals(PostgresDataSourceFactory.HOSTNAME)) {
            config.set(PostgresDataSourceFactory.HOSTNAME, "localhost");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.PORT)) {
            config.set(PostgresDataSourceFactory.PORT, 5432);
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.USERNAME)) {
            config.set(PostgresDataSourceFactory.USERNAME, "postgres");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.PASSWORD)) {
            config.set(PostgresDataSourceFactory.PASSWORD, "password");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.DATABASE_NAME)) {
            config.set(PostgresDataSourceFactory.DATABASE_NAME, "testdb");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.SCHEMA_NAME)) {
            config.set(PostgresDataSourceFactory.SCHEMA_NAME, "public");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.TABLES)) {
            config.set(PostgresDataSourceFactory.TABLES, "users,orders");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.SLOT_NAME)) {
            config.set(PostgresDataSourceFactory.SLOT_NAME, "flink_cdc_slot");
        }
        if (!excludeOption.equals(PostgresDataSourceFactory.SCAN_STARTUP_MODE)) {
            config.set(PostgresDataSourceFactory.SCAN_STARTUP_MODE, "snapshot");
        }
        
        return config;
    }
}