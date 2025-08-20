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

import java.util.Objects;

/** Startup options for PostgreSQL pipeline connector. */
@Internal
public class StartupOptions {

    /** Startup mode enumeration. */
    public enum StartupMode {
        /** Start from a snapshot of the current state. */
        SNAPSHOT,
        /** Start from the latest offset. */
        LATEST_OFFSET,
        /** Start from a specific offset. */
        SPECIFIC_OFFSET,
        /** Start from a specific timestamp. */
        TIMESTAMP
    }

    private final StartupMode mode;
    private final String specificOffset;
    private final Long timestamp;

    private StartupOptions(StartupMode mode, String specificOffset, Long timestamp) {
        this.mode = mode;
        this.specificOffset = specificOffset;
        this.timestamp = timestamp;
    }

    /** Create startup options for snapshot mode. */
    public static StartupOptions snapshot() {
        return new StartupOptions(StartupMode.SNAPSHOT, null, null);
    }

    /** Create startup options for latest offset mode. */
    public static StartupOptions latestOffset() {
        return new StartupOptions(StartupMode.LATEST_OFFSET, null, null);
    }

    /** Create startup options for specific offset mode. */
    public static StartupOptions specificOffset(String offset) {
        if (offset == null || offset.trim().isEmpty()) {
            throw new IllegalArgumentException("Specific offset cannot be null or empty");
        }
        return new StartupOptions(StartupMode.SPECIFIC_OFFSET, offset, null);
    }

    /** Create startup options for timestamp mode. */
    public static StartupOptions timestamp(long timestamp) {
        if (timestamp <= 0) {
            throw new IllegalArgumentException("Timestamp must be positive");
        }
        return new StartupOptions(StartupMode.TIMESTAMP, null, timestamp);
    }

    public StartupMode getMode() {
        return mode;
    }

    public String getSpecificOffset() {
        return specificOffset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return mode == that.mode &&
                Objects.equals(specificOffset, that.specificOffset) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, specificOffset, timestamp);
    }

    @Override
    public String toString() {
        return "StartupOptions{" +
                "mode=" + mode +
                ", specificOffset='" + specificOffset + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}