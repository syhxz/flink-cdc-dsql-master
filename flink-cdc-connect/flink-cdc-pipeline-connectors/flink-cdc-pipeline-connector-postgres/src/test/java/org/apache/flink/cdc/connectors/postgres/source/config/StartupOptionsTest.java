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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link StartupOptions}. */
class StartupOptionsTest {

    @Test
    void testSnapshotMode() {
        StartupOptions options = StartupOptions.snapshot();
        assertThat(options.getMode()).isEqualTo(StartupOptions.StartupMode.SNAPSHOT);
        assertThat(options.getSpecificOffset()).isNull();
        assertThat(options.getTimestamp()).isNull();
    }

    @Test
    void testLatestOffsetMode() {
        StartupOptions options = StartupOptions.latestOffset();
        assertThat(options.getMode()).isEqualTo(StartupOptions.StartupMode.LATEST_OFFSET);
        assertThat(options.getSpecificOffset()).isNull();
        assertThat(options.getTimestamp()).isNull();
    }

    @Test
    void testSpecificOffsetMode() {
        String offset = "0/1234567";
        StartupOptions options = StartupOptions.specificOffset(offset);
        assertThat(options.getMode()).isEqualTo(StartupOptions.StartupMode.SPECIFIC_OFFSET);
        assertThat(options.getSpecificOffset()).isEqualTo(offset);
        assertThat(options.getTimestamp()).isNull();
    }

    @Test
    void testSpecificOffsetModeWithNullOffset() {
        assertThatThrownBy(() -> StartupOptions.specificOffset(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSpecificOffsetModeWithEmptyOffset() {
        assertThatThrownBy(() -> StartupOptions.specificOffset(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSpecificOffsetModeWithWhitespaceOffset() {
        assertThatThrownBy(() -> StartupOptions.specificOffset("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTimestampMode() {
        long timestamp = System.currentTimeMillis();
        StartupOptions options = StartupOptions.timestamp(timestamp);
        assertThat(options.getMode()).isEqualTo(StartupOptions.StartupMode.TIMESTAMP);
        assertThat(options.getSpecificOffset()).isNull();
        assertThat(options.getTimestamp()).isEqualTo(timestamp);
    }

    @Test
    void testTimestampModeWithZeroTimestamp() {
        assertThatThrownBy(() -> StartupOptions.timestamp(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTimestampModeWithNegativeTimestamp() {
        assertThatThrownBy(() -> StartupOptions.timestamp(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEqualsAndHashCode() {
        StartupOptions options1 = StartupOptions.snapshot();
        StartupOptions options2 = StartupOptions.snapshot();
        StartupOptions options3 = StartupOptions.latestOffset();

        assertThat(options1).isEqualTo(options2);
        assertThat(options1.hashCode()).isEqualTo(options2.hashCode());
        assertThat(options1).isNotEqualTo(options3);
        assertThat(options1.hashCode()).isNotEqualTo(options3.hashCode());

        String offset = "0/1234567";
        StartupOptions options4 = StartupOptions.specificOffset(offset);
        StartupOptions options5 = StartupOptions.specificOffset(offset);
        StartupOptions options6 = StartupOptions.specificOffset("0/7654321");

        assertThat(options4).isEqualTo(options5);
        assertThat(options4.hashCode()).isEqualTo(options5.hashCode());
        assertThat(options4).isNotEqualTo(options6);
        assertThat(options4.hashCode()).isNotEqualTo(options6.hashCode());

        long timestamp = System.currentTimeMillis();
        StartupOptions options7 = StartupOptions.timestamp(timestamp);
        StartupOptions options8 = StartupOptions.timestamp(timestamp);
        StartupOptions options9 = StartupOptions.timestamp(timestamp + 1000);

        assertThat(options7).isEqualTo(options8);
        assertThat(options7.hashCode()).isEqualTo(options8.hashCode());
        assertThat(options7).isNotEqualTo(options9);
        assertThat(options7.hashCode()).isNotEqualTo(options9.hashCode());
    }

    @Test
    void testToString() {
        StartupOptions snapshotOptions = StartupOptions.snapshot();
        assertThat(snapshotOptions.toString()).contains("SNAPSHOT");

        StartupOptions latestOptions = StartupOptions.latestOffset();
        assertThat(latestOptions.toString()).contains("LATEST_OFFSET");

        String offset = "0/1234567";
        StartupOptions specificOptions = StartupOptions.specificOffset(offset);
        String specificString = specificOptions.toString();
        assertThat(specificString).contains("SPECIFIC_OFFSET");
        assertThat(specificString).contains(offset);

        long timestamp = System.currentTimeMillis();
        StartupOptions timestampOptions = StartupOptions.timestamp(timestamp);
        String timestampString = timestampOptions.toString();
        assertThat(timestampString).contains("TIMESTAMP");
        assertThat(timestampString).contains(String.valueOf(timestamp));
    }
}
