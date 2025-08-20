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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Utility class for handling retries and error recovery in DSQL operations.
 */
public class DsqlRetryHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlRetryHelper.class);

    private final int maxRetries;
    private final long initialBackoffMs;
    private final double backoffMultiplier;

    public DsqlRetryHelper(int maxRetries, long initialBackoffMs, double backoffMultiplier) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
    }

    /**
     * Executes an operation with retry logic.
     *
     * @param operation The operation to execute
     * @param operationName Name of the operation for logging
     * @param <T> Return type of the operation
     * @return Result of the operation
     * @throws Exception if all retries are exhausted
     */
    public <T> T executeWithRetry(Callable<T> operation, String operationName) throws Exception {
        Exception lastException = null;
        long backoffMs = initialBackoffMs;
        long startTime = System.currentTimeMillis();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                LOG.debug("Executing {} (attempt {}/{})", operationName, attempt, maxRetries);
                return operation.call();

            } catch (Exception e) {
                lastException = e;

                if (attempt == maxRetries) {
                    LOG.error("Failed to execute {} after {} attempts", operationName, maxRetries, e);
                    break;
                }

                if (isRetryableException(e)) {
                    LOG.warn("Retryable error in {} (attempt {}/{}): {}. Retrying in {}ms",
                            operationName, attempt, maxRetries, e.getMessage(), backoffMs);

                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry backoff", ie);
                    }

                    backoffMs = (long) (backoffMs * backoffMultiplier);
                } else {
                    LOG.error("Non-retryable error in {}: {}", operationName, e.getMessage());
                    throw e;
                }
            }
        }

        long totalDuration = System.currentTimeMillis() - startTime;
        throw DsqlErrorReporter.reportRetryExhaustionError(operationName, maxRetries, totalDuration, lastException);
    }

    /**
     * Determines if an exception is retryable.
     *
     * @param exception The exception to check
     * @return true if the exception is retryable
     */
    private boolean isRetryableException(Exception exception) {
        if (exception instanceof SQLException) {
            SQLException sqlException = (SQLException) exception;
            String sqlState = sqlException.getSQLState();

            // Connection-related errors (retryable)
            if (sqlState != null) {
                // Connection failure
                if (sqlState.startsWith("08")) {
                    return true;
                }
                // Serialization failure (can happen in concurrent scenarios)
                if (sqlState.equals("40001")) {
                    return true;
                }
                // Lock timeout
                if (sqlState.equals("40P01")) {
                    return true;
                }
            }

            // Check error message for common retryable patterns
            String message = sqlException.getMessage().toLowerCase();
            if (message.contains("connection") ||
                message.contains("timeout") ||
                message.contains("network") ||
                message.contains("broken pipe")) {
                return true;
            }
        }

        // Network-related exceptions
        if (exception.getCause() instanceof java.net.SocketException ||
            exception.getCause() instanceof java.net.ConnectException ||
            exception.getCause() instanceof java.net.SocketTimeoutException) {
            return true;
        }

        return false;
    }

    /**
     * Creates a default retry helper with sensible defaults.
     *
     * @return A retry helper with default configuration
     */
    public static DsqlRetryHelper createDefault() {
        return new DsqlRetryHelper(3, 1000, 2.0);
    }
}
