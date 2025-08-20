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

package org.apache.flink.cdc.connectors.dsql.utils;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for detailed error reporting in DSQL connector.
 * Provides context-rich error messages and categorizes different types of errors.
 */
public class DsqlErrorReporter {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlErrorReporter.class);

    // Error categories
    /**
     * Categories of errors that can occur in DSQL operations.
     */
    public enum ErrorCategory {
        CONNECTION_ERROR,
        AUTHENTICATION_ERROR,
        SCHEMA_ERROR,
        DATA_TYPE_ERROR,
        FULL_LOAD_ERROR,
        CDC_ERROR,
        CONFIGURATION_ERROR,
        UNKNOWN_ERROR
    }

    /**
     * Reports a connection-related error with detailed context.
     */
    public static RuntimeException reportConnectionError(String operation, String host, int port, Throwable cause) {
        String message = String.format(
            "Failed to %s - Connection error to DSQL instance at %s:%d. " +
            "Please verify: (1) DSQL instance is running and accessible, " +
            "(2) Network connectivity is available, " +
            "(3) Security groups/firewall rules allow connections on port %d",
            operation, host, port, port
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports an authentication-related error with detailed context.
     */
    public static RuntimeException reportAuthenticationError(String operation, String region, Throwable cause) {
        String message = String.format(
            "Failed to %s - Authentication error for DSQL in region %s. " +
            "Please verify: (1) AWS credentials are properly configured, " +
            "(2) IAM role has necessary DSQL permissions, " +
            "(3) Region is correct and DSQL is available in this region, " +
            "(4) Token generation service is accessible",
            operation, region
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports a schema-related error with detailed context.
     */
    public static RuntimeException reportSchemaError(String operation, TableId tableId, String schemaDetails, Throwable cause) {
        String message = String.format(
            "Failed to %s - Schema error for table %s. " +
            "Schema details: %s. " +
            "Please verify: (1) Table schema is compatible with DSQL, " +
            "(2) Data types are supported, " +
            "(3) Primary key constraints are properly defined",
            operation, tableId, schemaDetails
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports a data type conversion error with detailed context.
     */
    public static RuntimeException reportDataTypeError(String operation, TableId tableId, String columnName,
                                                      String sourceType, String targetType, Object value, Throwable cause) {
        String message = String.format(
            "Failed to %s - Data type conversion error for table %s, column %s. " +
            "Cannot convert value '%s' from source type '%s' to target type '%s'. " +
            "Please verify: (1) Data type mapping is correct, " +
            "(2) Value format is compatible, " +
            "(3) Consider using data transformation if needed",
            operation, tableId, columnName, value, sourceType, targetType
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports a full load error with detailed context.
     */
    public static RuntimeException reportFullLoadError(String operation, TableId tableId, long recordsProcessed,
                                                      long totalRecords, Throwable cause) {
        String message = String.format(
            "Failed to %s - Full load error for table %s. " +
            "Progress: %d/%d records processed (%.1f%%). " +
            "Please verify: (1) Source database is accessible, " +
            "(2) Target DSQL instance has sufficient capacity, " +
            "(3) Network connection is stable, " +
            "(4) Consider reducing parallelism if resource constraints exist",
            operation, tableId, recordsProcessed, totalRecords,
            totalRecords > 0 ? (recordsProcessed * 100.0 / totalRecords) : 0.0
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports a CDC (Change Data Capture) error with detailed context.
     */
    public static RuntimeException reportCdcError(String operation, TableId tableId, String eventType,
                                                 String sourcePosition, Throwable cause) {
        String message = String.format(
            "Failed to %s - CDC error for table %s, event type %s at source position %s. " +
            "Please verify: (1) Source database binlog/WAL is accessible, " +
            "(2) CDC position is valid, " +
            "(3) Target table exists and schema is compatible, " +
            "(4) Connection to DSQL is stable",
            operation, tableId, eventType, sourcePosition
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports a configuration error with detailed context.
     */
    public static RuntimeException reportConfigurationError(String configKey, String configValue, String expectedFormat, Throwable cause) {
        String message = String.format(
            "Configuration error - Invalid value '%s' for configuration key '%s'. " +
            "Expected format: %s. " +
            "Please verify: (1) Configuration syntax is correct, " +
            "(2) All required parameters are provided, " +
            "(3) Parameter values are within valid ranges",
            configValue, configKey, expectedFormat
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports an SQL execution error with detailed context.
     */
    public static RuntimeException reportSqlExecutionError(String operation, String sql, TableId tableId, SQLException cause) {
        String message = String.format(
            "Failed to %s - SQL execution error for table %s. " +
            "SQL: %s. " +
            "SQL State: %s, Error Code: %d. " +
            "Please verify: (1) SQL syntax is correct for DSQL, " +
            "(2) Table and columns exist, " +
            "(3) Data constraints are satisfied, " +
            "(4) Connection is still valid",
            operation, tableId, sql, cause.getSQLState(), cause.getErrorCode()
        );

        LOG.error(message, cause);
        return new RuntimeException(message, cause);
    }

    /**
     * Reports a retry exhaustion error with detailed context.
     */
    public static RuntimeException reportRetryExhaustionError(String operation, int maxRetries, long totalDuration, Throwable lastCause) {
        String message = String.format(
            "Failed to %s - Retry exhausted after %d attempts over %d ms. " +
            "Last error: %s. " +
            "Please verify: (1) Root cause is resolved, " +
            "(2) Retry configuration is appropriate, " +
            "(3) System resources are sufficient, " +
            "(4) Consider increasing retry limits if transient issues persist",
            operation, maxRetries, totalDuration, lastCause.getMessage()
        );

        LOG.error(message, lastCause);
        return new RuntimeException(message, lastCause);
    }

    /**
     * Creates a detailed error context map for logging and debugging.
     */
    public static Map<String, Object> createErrorContext(String operation, TableId tableId, Event event, Throwable cause) {
        Map<String, Object> context = new HashMap<>();
        context.put("operation", operation);
        context.put("timestamp", System.currentTimeMillis());
        context.put("thread", Thread.currentThread().getName());

        if (tableId != null) {
            context.put("tableId", tableId.toString());
            context.put("schemaName", tableId.getSchemaName());
            context.put("tableName", tableId.getTableName());
        }

        if (event != null) {
            context.put("eventType", event.getClass().getSimpleName());
        }

        if (cause != null) {
            context.put("errorType", cause.getClass().getSimpleName());
            context.put("errorMessage", cause.getMessage());

            if (cause instanceof SQLException) {
                SQLException sqlEx = (SQLException) cause;
                context.put("sqlState", sqlEx.getSQLState());
                context.put("errorCode", sqlEx.getErrorCode());
            }
        }

        return context;
    }

    /**
     * Categorizes an error based on its type and context.
     */
    public static ErrorCategory categorizeError(Throwable error) {
        if (error == null) {
            return ErrorCategory.UNKNOWN_ERROR;
        }

        String errorMessage = error.getMessage().toLowerCase();
        String errorType = error.getClass().getSimpleName().toLowerCase();

        // Connection-related errors
        if (errorMessage.contains("connection") || errorMessage.contains("timeout") ||
            errorMessage.contains("network") || errorType.contains("connection")) {
            return ErrorCategory.CONNECTION_ERROR;
        }

        // Authentication-related errors
        if (errorMessage.contains("auth") || errorMessage.contains("credential") ||
            errorMessage.contains("permission") || errorMessage.contains("access denied")) {
            return ErrorCategory.AUTHENTICATION_ERROR;
        }

        // Schema-related errors
        if (errorMessage.contains("schema") || errorMessage.contains("table") ||
            errorMessage.contains("column") || errorMessage.contains("constraint")) {
            return ErrorCategory.SCHEMA_ERROR;
        }

        // Data type errors
        if (errorMessage.contains("type") || errorMessage.contains("conversion") ||
            errorMessage.contains("cast") || errorMessage.contains("format")) {
            return ErrorCategory.DATA_TYPE_ERROR;
        }

        // SQL-related errors
        if (error instanceof SQLException) {
            SQLException sqlEx = (SQLException) error;
            String sqlState = sqlEx.getSQLState();

            if (sqlState != null) {
                // Connection errors (08xxx)
                if (sqlState.startsWith("08")) {
                    return ErrorCategory.CONNECTION_ERROR;
                }
                // Data type errors (22xxx)
                if (sqlState.startsWith("22")) {
                    return ErrorCategory.DATA_TYPE_ERROR;
                }
                // Schema errors (42xxx)
                if (sqlState.startsWith("42")) {
                    return ErrorCategory.SCHEMA_ERROR;
                }
            }
        }

        return ErrorCategory.UNKNOWN_ERROR;
    }

    /**
     * Logs error statistics for monitoring and debugging.
     */
    public static void logErrorStatistics(String operation, ErrorCategory category, long duration, boolean recovered) {
        LOG.info("Error statistics - Operation: {}, Category: {}, Duration: {}ms, Recovered: {}",
                operation, category, duration, recovered);
    }
}
