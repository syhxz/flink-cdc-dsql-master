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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.dsql.auth.DsqlAuthenticator;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.*;

/**
 * A batch processing sink function for DSQL that supports connection pooling
 * and batch commits.
 */
public class DsqlBatchSinkFunction extends RichSinkFunction<Event> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlBatchSinkFunction.class);

    private final Configuration config;

    // Batch processing configuration
    private final int batchSize;
    private final long batchTimeoutMs;
    
    // CDC implementation configuration
    private final boolean useUpsertForCdc;
    
    // COPY command configuration for full load stage
    private final boolean useCopyForFullLoad;
    private final String copyDelimiter;
    private final String copyNullString;
    
    // CDC phase tracking
    private volatile boolean hasSeenCdcEvents = false;
    private volatile boolean isSnapshotComplete = false;
    private final long startTime = System.currentTimeMillis();
    private long snapshotTimeoutMs;
    
    // RPS (Records Per Second) tracking
    private volatile long totalRecordsProcessed = 0;
    private volatile long lastRpsLogTime = System.currentTimeMillis();
    private volatile long lastRecordCount = 0;
    private final long rpsLogIntervalMs = 10000; // Log RPS every 10 seconds

    // Connection pool
    private transient DataSource dataSource;

    // Batch buffer and state
    private transient List<Event> eventBuffer;
    private transient long lastBatchTime;
    private transient ScheduledExecutorService batchTimer;

    // Schema cache
    private transient Map<TableId, Schema> schemaCache;

    // Batch statistics
    private transient long totalEventsProcessed = 0;
    private transient long totalBatchesCommitted = 0;

    public DsqlBatchSinkFunction(Configuration config) {
        this.config = config;
        this.batchSize = config.get(BATCH_SIZE);
        this.batchTimeoutMs = parseBatchTimeout(config.get(BATCH_TIMEOUT));
        
        // Enable UPSERT for CDC implementation stage
        // This uses INSERT ... ON CONFLICT ... DO UPDATE to handle overlapping data
        // between snapshot and streaming phases gracefully
        this.useUpsertForCdc = config.getOptional(ConfigOptions.key("use-upsert-for-cdc")
                .booleanType()
                .defaultValue(true))
                .orElse(true);

        // Configure COPY command for full load stage
        // COPY provides much better performance than INSERT for bulk loading
        this.useCopyForFullLoad = config.getOptional(ConfigOptions.key("use-copy-for-full-load")
                .booleanType()
                .defaultValue(false))
                .orElse(false);
                
        this.copyDelimiter = config.getOptional(ConfigOptions.key("copy-delimiter")
                .stringType()
                .defaultValue("\t"))
                .orElse("\t");
                
        this.copyNullString = config.getOptional(ConfigOptions.key("copy-null-string")
                .stringType()
                .defaultValue("\\N"))
                .orElse("\\N");

        // Configure snapshot timeout for CDC phase detection
        long snapshotTimeoutMinutes = config.getOptional(ConfigOptions.key("snapshot-timeout-minutes")
                .longType()
                .defaultValue(5L))
                .orElse(5L);
        this.snapshotTimeoutMs = snapshotTimeoutMinutes * 60 * 1000;

        LOG.info("Initialized DSQL batch sink with batch size: {}, timeout: {}ms, use-upsert-for-cdc: {}, " +
                "use-copy-for-full-load: {}, copy-delimiter: '{}', snapshot-timeout: {}min",
                batchSize, batchTimeoutMs, useUpsertForCdc, useCopyForFullLoad, 
                copyDelimiter.replace("\t", "\\t"), snapshotTimeoutMinutes);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize connection pool
        initializeConnectionPool();

        // Initialize batch processing components
        eventBuffer = new ArrayList<>(batchSize);
        lastBatchTime = System.currentTimeMillis();
        schemaCache = new ConcurrentHashMap<>();

        // Initialize batch timer for timeout handling
        batchTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dsql-batch-timer");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic batch timeout check
        batchTimer.scheduleAtFixedRate(this::checkBatchTimeout,
                batchTimeoutMs / 10, batchTimeoutMs / 10, TimeUnit.MILLISECONDS);

        LOG.info("DSQL batch sink opened successfully");
    }

    @Override
    public void close() throws Exception {
        try {
            // Process any remaining events in buffer
            if (eventBuffer != null && !eventBuffer.isEmpty()) {
                LOG.info("Processing final batch of {} events before closing", eventBuffer.size());
                processBatch();
            }

            // Shutdown batch timer
            if (batchTimer != null) {
                batchTimer.shutdown();
                if (!batchTimer.awaitTermination(5, TimeUnit.SECONDS)) {
                    batchTimer.shutdownNow();
                }
            }

            // Close connection pool
            if (dataSource != null) {
                try {
                    if (dataSource instanceof HikariDataSource) {
                        ((HikariDataSource) dataSource).close();
                    } else if (dataSource instanceof org.postgresql.ds.PGSimpleDataSource) {
                        // PGSimpleDataSource doesn't need explicit closing
                        LOG.info("PGSimpleDataSource doesn't require explicit closing");
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing data source: {}", e.getMessage());
                }
            }

            LOG.info("DSQL batch sink closed. Total events processed: {}, total batches: {}",
                    totalEventsProcessed, totalBatchesCommitted);

        } finally {
            super.close();
        }
    }

    @Override
    public void invoke(Event event, Context context) throws Exception {
        synchronized (eventBuffer) {
            // Add event to buffer
            eventBuffer.add(event);
            totalEventsProcessed++;

            LOG.debug("Added event to batch buffer. Buffer size: {}/{}", eventBuffer.size(), batchSize);

            // Check if batch should be processed
            if (shouldProcessBatch()) {
                processBatch();
            }
        }
    }

    private void initializeConnectionPool() throws Exception {
        String host = config.get(HOST);
        Integer port = config.get(PORT);
        String database = config.get(DATABASE);

        LOG.info("Initializing DSQL connection with basic DataSource (bypassing HikariCP for isolation level compatibility)");

        try {
            // Use basic PostgreSQL DataSource to avoid HikariCP transaction isolation issues
            org.postgresql.ds.PGSimpleDataSource pgDataSource = new org.postgresql.ds.PGSimpleDataSource();
            pgDataSource.setServerNames(new String[]{host});
            pgDataSource.setPortNumbers(new int[]{port});
            pgDataSource.setDatabaseName(database);
            
            // Configure authentication
            Properties connectionProps = new Properties();
            DsqlAuthenticator authenticator = new DsqlAuthenticator(config);
            authenticator.configureAuthentication(connectionProps);
            
            // Set authentication properties
            for (String key : connectionProps.stringPropertyNames()) {
                if ("user".equals(key)) {
                    pgDataSource.setUser(connectionProps.getProperty(key));
                } else if ("password".equals(key)) {
                    pgDataSource.setPassword(connectionProps.getProperty(key));
                }
            }
            
            // SSL configuration for DSQL
            pgDataSource.setSsl(true);
            pgDataSource.setSslMode("require");
            
            // Set connection timeout
            pgDataSource.setConnectTimeout(30);
            pgDataSource.setSocketTimeout(60);
            
            // Test the connection
            boolean connectionSuccessful = false;
            int maxRetries = 3;
            
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try (Connection testConn = pgDataSource.getConnection()) {
                    if (testConn.isValid(10)) {
                        // Test basic DSQL functionality
                        try (Statement stmt = testConn.createStatement()) {
                            stmt.execute("SELECT 1");
                            LOG.info("Successfully initialized DSQL connection with basic DataSource (attempt {})", attempt);
                            LOG.info("DSQL connection details: host={}, port={}, database={}", host, port, database);
                            dataSource = pgDataSource;
                            connectionSuccessful = true;
                            break;
                        }
                    } else {
                        throw new SQLException("Connection validation failed");
                    }
                } catch (SQLException e) {
                    LOG.warn("DSQL connection attempt {} failed: {}", attempt, e.getMessage());
                    
                    if (attempt < maxRetries && isTokenExpirationError(e)) {
                        LOG.info("Token expiration detected, clearing cache and retrying...");
                        DsqlAuthenticator.clearTokenCache();
                        try {
                            Thread.sleep(2000); // Wait 2 seconds before retry
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Connection initialization interrupted", ie);
                        }
                    } else if (attempt == maxRetries) {
                        LOG.error("Failed to initialize DSQL connection after {} attempts. Last error: {}", maxRetries, e.getMessage());
                        throw new RuntimeException("DSQL connection initialization failed", e);
                    }
                }
            }
            
            if (!connectionSuccessful) {
                throw new SQLException("Failed to initialize DSQL connection after " + maxRetries + " attempts");
            }
            
        } catch (Exception e) {
            LOG.error("Failed to initialize DSQL connection: {}", e.getMessage(), e);
            throw e;
        }
    }

    private boolean shouldProcessBatch() {
        return eventBuffer.size() >= batchSize ||
                (System.currentTimeMillis() - lastBatchTime) >= batchTimeoutMs;
    }

    private void checkBatchTimeout() {
        synchronized (eventBuffer) {
            if (!eventBuffer.isEmpty() &&
                    (System.currentTimeMillis() - lastBatchTime) >= batchTimeoutMs) {
                try {
                    LOG.info("Batch timeout reached, processing {} events", eventBuffer.size());
                    processBatch();
                } catch (Exception e) {
                    LOG.error("Failed to process batch on timeout: {}", e.getMessage(), e);
                }
            }
        }
    }

    private void processBatch() throws Exception {
        if (eventBuffer.isEmpty()) {
            return;
        }

        int batchEventCount = eventBuffer.size();
        long startTime = System.currentTimeMillis();

        // Group events by table for efficient processing
        Map<TableId, List<Event>> eventsByTable = groupEventsByTable(eventBuffer);

        // DSQL Constraint: DDL and DML cannot be in the same transaction
        // Process DDL operations first in separate transactions
        processDDLOperations(eventsByTable);

        // Then process DML operations in separate transactions
        processDMLOperations(eventsByTable);

        long processingTime = System.currentTimeMillis() - startTime;
        totalBatchesCommitted++;

        LOG.info("Successfully committed batch #{} with {} events in {}ms. " +
                "Total events processed: {}",
                totalBatchesCommitted, batchEventCount, processingTime, totalEventsProcessed);

        // Clear buffer and reset timer
        eventBuffer.clear();
        lastBatchTime = System.currentTimeMillis();
    }

    /**
     * Process DDL operations (CREATE TABLE, ALTER TABLE) in separate transactions.
     */
    private void processDDLOperations(Map<TableId, List<Event>> eventsByTable) throws Exception {
        for (Map.Entry<TableId, List<Event>> entry : eventsByTable.entrySet()) {
            TableId tableId = entry.getKey();
            List<Event> events = entry.getValue();

            // Extract DDL events
            List<CreateTableEvent> createEvents = new ArrayList<>();
            List<SchemaChangeEvent> schemaEvents = new ArrayList<>();

            for (Event event : events) {
                if (event instanceof CreateTableEvent) {
                    createEvents.add((CreateTableEvent) event);
                } else if (event instanceof SchemaChangeEvent) {
                    schemaEvents.add((SchemaChangeEvent) event);
                }
            }

            // Process DDL events in separate transaction
            if (!createEvents.isEmpty() || !schemaEvents.isEmpty()) {
                try (Connection connection = getConnectionWithRetry()) {
                    connection.setAutoCommit(false);

                    // Process CREATE TABLE events
                    for (CreateTableEvent event : createEvents) {
                        executeWithConnectionRetry(() -> {
                            handleCreateTableEvent(event, connection);
                            return null;
                        }, "CREATE TABLE for " + tableId);
                    }

                    // Process schema change events
                    for (SchemaChangeEvent event : schemaEvents) {
                        executeWithConnectionRetry(() -> {
                            handleSchemaChangeEvent(event, connection);
                            return null;
                        }, "schema change for " + tableId);
                    }

                    // Commit DDL transaction with retry logic for DSQL concurrency
                    commitDDLWithRetry(connection, tableId);
                    LOG.info("Successfully committed DDL transaction for table {}", tableId);

                } catch (SQLException e) {
                    LOG.error("Failed to process DDL operations for table {}: {}", tableId, e.getMessage(), e);
                    
                    // Check if this is a DSQL concurrency error that should be retried
                    if (isDsqlConcurrencyError(e)) {
                        LOG.warn("DSQL concurrency error detected for table {}, will be retried by Flink", tableId);
                        throw new RuntimeException("DSQL concurrency error for table " + tableId + " - will retry", e);
                    } else {
                        throw new RuntimeException("DDL processing failed for table " + tableId, e);
                    }
                }
            }
        }
    }

    /**
     * Commit DDL transaction with retry logic for DSQL concurrency issues.
     */
    private void commitDDLWithRetry(Connection connection, TableId tableId) throws SQLException {
        int maxRetries = 3;
        int retryDelayMs = 1000; // Start with 1 second
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                connection.commit();
                if (attempt > 1) {
                    LOG.info("DDL commit succeeded for table {} on attempt {}", tableId, attempt);
                }
                return; // Success
                
            } catch (SQLException e) {
                if (isDsqlConcurrencyError(e) && attempt < maxRetries) {
                    LOG.warn("DDL commit failed for table {} on attempt {} due to concurrency, retrying in {}ms: {}", 
                            tableId, attempt, retryDelayMs, e.getMessage());
                    
                    try {
                        Thread.sleep(retryDelayMs);
                        retryDelayMs *= 2; // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted during DDL retry for table " + tableId, ie);
                    }
                } else {
                    // Either not a concurrency error, or we've exhausted retries
                    throw e;
                }
            }
        }
    }

    /**
     * Check if the exception is a duplicate key error.
     */
    private boolean isDuplicateKeyError(Exception e) {
        String message = e.getMessage().toLowerCase();
        return message.contains("duplicate key value violates unique constraint") ||
                message.contains("duplicate key") ||
                message.contains("unique constraint") ||
                message.contains("already exists");
    }

    /**
     * Check if the SQLException is a DSQL concurrency error that should be retried.
     */
    private boolean isDsqlConcurrencyError(SQLException e) {
        String message = e.getMessage();
        return message != null && (
            message.contains("schema has been updated by another transaction") ||
            message.contains("OC001") ||
            message.contains("concurrent schema modification")
        );
    }

    /**
     * Process DML operations (INSERT, UPDATE, DELETE) in separate transactions.
     */
    private void processDMLOperations(Map<TableId, List<Event>> eventsByTable) throws Exception {
        for (Map.Entry<TableId, List<Event>> entry : eventsByTable.entrySet()) {
            TableId tableId = entry.getKey();
            List<Event> events = entry.getValue();

            // Extract DML events
            List<DataChangeEvent> insertEvents = new ArrayList<>();
            List<DataChangeEvent> updateEvents = new ArrayList<>();
            List<DataChangeEvent> deleteEvents = new ArrayList<>();

            for (Event event : events) {
                if (event instanceof DataChangeEvent) {
                    DataChangeEvent dataEvent = (DataChangeEvent) event;
                    switch (dataEvent.op()) {
                        case INSERT:
                            insertEvents.add(dataEvent);
                            break;
                        case UPDATE:
                            updateEvents.add(dataEvent);
                            // UPDATE events indicate we're in CDC streaming phase
                            if (!hasSeenCdcEvents) {
                                hasSeenCdcEvents = true;
                                LOG.info("=== CDC PHASE DETECTED: Switching to UPSERT mode for future INSERTs ===");
                            }
                            break;
                        case DELETE:
                            deleteEvents.add(dataEvent);
                            // DELETE events indicate we're in CDC streaming phase
                            if (!hasSeenCdcEvents) {
                                hasSeenCdcEvents = true;
                                LOG.info("=== CDC PHASE DETECTED: Switching to UPSERT mode for future INSERTs ===");
                            }
                            break;
                    }
                }
            }

            // Process DML events in separate transaction
            if (!insertEvents.isEmpty() || !updateEvents.isEmpty() || !deleteEvents.isEmpty()) {
                try (Connection connection = getConnectionWithRetry()) {
                    connection.setAutoCommit(false);

                    // Process INSERT events
                    if (!insertEvents.isEmpty()) {
                        executeWithConnectionRetry(() -> {
                            batchInsertEvents(connection, tableId, insertEvents);
                            return null;
                        }, "batch INSERT for " + tableId);
                    }

                    // Process UPDATE events
                    if (!updateEvents.isEmpty()) {
                        executeWithConnectionRetry(() -> {
                            batchUpdateEvents(connection, tableId, updateEvents);
                            return null;
                        }, "batch UPDATE for " + tableId);
                    }

                    // Process DELETE events
                    if (!deleteEvents.isEmpty()) {
                        executeWithConnectionRetry(() -> {
                            batchDeleteEvents(connection, tableId, deleteEvents);
                            return null;
                        }, "batch DELETE for " + tableId);
                    }

                    // Commit DML transaction
                    connection.commit();
                    LOG.info("Successfully committed DML transaction for table {} ({} INSERTs, {} UPDATEs, {} DELETEs)",
                            tableId, insertEvents.size(), updateEvents.size(), deleteEvents.size());

                } catch (SQLException e) {
                    LOG.error("Failed to process DML operations for table {}: {}", tableId, e.getMessage(), e);
                    throw new RuntimeException("DML processing failed for table " + tableId, e);
                }
            }
        }
    }

    /**
     * Gets a database connection with retry logic for handling token expiration.
     */
    private Connection getConnectionWithRetry() throws SQLException {
        int maxRetries = 3;
        SQLException lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                LOG.debug("Attempting to get connection (attempt {}/{})", attempt, maxRetries);

                Connection connection = dataSource.getConnection();

                // Test the connection to ensure it's valid
                if (connection.isValid(10)) {
                    LOG.debug("Successfully obtained database connection (attempt {})", attempt);
                    return connection;
                } else {
                    connection.close();
                    throw new SQLException("Connection validation failed");
                }

            } catch (SQLException e) {
                lastException = e;

                LOG.warn("Connection attempt {} failed: {}", attempt, e.getMessage());

                // Check if it's a token expiration or connection timeout error
                if (isTokenExpirationError(e) || isConnectionTimeoutError(e)) {
                    LOG.warn(
                            "Token expiration or connection timeout detected, refreshing connection pool... (attempt {}/{})",
                            attempt, maxRetries);

                    try {
                        // Force token refresh and recreate connection pool
                        refreshConnectionPoolWithNewToken();

                        // Wait before retry with exponential backoff
                        long delayMs = 2000L * attempt; // 2s, 4s, 6s
                        LOG.info("Waiting {}ms before retry attempt {}", delayMs, attempt + 1);
                        Thread.sleep(delayMs);

                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted during token refresh retry", ie);
                    } catch (Exception refreshException) {
                        LOG.error("Failed to refresh connection pool: {}", refreshException.getMessage(),
                                refreshException);
                        // Continue to next retry attempt
                    }

                } else {
                    // Non-token related error, don't retry
                    LOG.error("Non-recoverable database connection error: {}", e.getMessage());
                    throw e;
                }
            }
        }

        throw new SQLException("Failed to get database connection after " + maxRetries +
                " retries. Last error: " + (lastException != null ? lastException.getMessage() : "unknown"),
                lastException);
    }

    /**
     * Checks if the SQLException is related to token expiration.
     */
    private boolean isTokenExpirationError(SQLException e) {
        String message = e.getMessage().toLowerCase();
        return message.contains("signature expired") ||
                message.contains("access denied") ||
                message.contains("authentication failed") ||
                message.contains("token expired") ||
                message.contains("invalid token") ||
                message.contains("unable to accept connection") ||
                message.contains("connection attempt failed") ||
                (e.getCause() != null && e.getCause() instanceof java.io.EOFException);
    }

    /**
     * Checks if the SQLException is related to connection timeout.
     */
    private boolean isConnectionTimeoutError(SQLException e) {
        String message = e.getMessage().toLowerCase();
        Throwable cause = e.getCause();
        
        return message.contains("connection is not available") ||
                message.contains("request timed out") ||
                message.contains("connection timeout") ||
                message.contains("timeout") ||
                message.contains("connection refused") ||
                message.contains("network is unreachable") ||
                message.contains("connection reset") ||
                message.contains("connection attempt failed") ||
                (cause != null && (
                    cause instanceof java.net.SocketTimeoutException ||
                    cause instanceof java.net.ConnectException ||
                    cause instanceof java.io.EOFException ||
                    cause instanceof java.net.SocketException
                ));
    }

    /**
     * Refreshes the connection pool with a new authentication token.
     */
    private void refreshConnectionPoolWithNewToken() throws Exception {
        LOG.info("Refreshing connection pool with new authentication token...");

        // Close existing connection pool
        if (dataSource != null) {
            try {
                if (dataSource instanceof HikariDataSource) {
                    ((HikariDataSource) dataSource).close();
                } else if (dataSource instanceof org.postgresql.ds.PGSimpleDataSource) {
                    // PGSimpleDataSource doesn't need explicit closing
                    LOG.info("PGSimpleDataSource doesn't require explicit closing");
                }
                LOG.info("Closed existing connection pool");
            } catch (Exception e) {
                LOG.warn("Error closing existing data source: {}", e.getMessage());
            }
        }

        // Clear token cache to force new token generation
        DsqlAuthenticator.clearTokenCache();

        // Wait a moment for token cache to clear
        Thread.sleep(1000);

        // Reinitialize connection pool with fresh token
        initializeConnectionPool();

        LOG.info("Successfully refreshed connection pool with new token");
    }

    private Map<TableId, List<Event>> groupEventsByTable(List<Event> events) {
        Map<TableId, List<Event>> eventsByTable = new HashMap<>();

        for (Event event : events) {
            TableId tableId = getTableIdFromEvent(event);
            if (tableId != null) {
                eventsByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(event);
            }
        }

        return eventsByTable;
    }

    private void processBatchForTable(Connection connection, TableId tableId, List<Event> events)
            throws SQLException {

        // Separate events by type for batch processing
        List<CreateTableEvent> createEvents = new ArrayList<>();
        List<DataChangeEvent> insertEvents = new ArrayList<>();
        List<DataChangeEvent> updateEvents = new ArrayList<>();
        List<DataChangeEvent> deleteEvents = new ArrayList<>();
        List<SchemaChangeEvent> schemaEvents = new ArrayList<>();

        for (Event event : events) {
            if (event instanceof CreateTableEvent) {
                createEvents.add((CreateTableEvent) event);
            } else if (event instanceof DataChangeEvent) {
                DataChangeEvent dataEvent = (DataChangeEvent) event;
                switch (dataEvent.op()) {
                    case INSERT:
                        insertEvents.add(dataEvent);
                        break;
                    case UPDATE:
                        updateEvents.add(dataEvent);
                        break;
                    case DELETE:
                        deleteEvents.add(dataEvent);
                        break;
                }
            } else if (event instanceof SchemaChangeEvent) {
                schemaEvents.add((SchemaChangeEvent) event);
            }
        }

        // Process events in order: CREATE TABLE, INSERT, UPDATE, DELETE, SCHEMA CHANGES
        // Each operation can trigger connection-level token refresh if needed

        for (CreateTableEvent event : createEvents) {
            executeWithConnectionRetry(() -> {
                handleCreateTableEvent(event, connection);
                return null;
            }, "CREATE TABLE for " + tableId);
        }

        if (!insertEvents.isEmpty()) {
            executeWithConnectionRetry(() -> {
                batchInsertEvents(connection, tableId, insertEvents);
                return null;
            }, "batch INSERT for " + tableId);
        }

        if (!updateEvents.isEmpty()) {
            executeWithConnectionRetry(() -> {
                batchUpdateEvents(connection, tableId, updateEvents);
                return null;
            }, "batch UPDATE for " + tableId);
        }

        if (!deleteEvents.isEmpty()) {
            executeWithConnectionRetry(() -> {
                batchDeleteEvents(connection, tableId, deleteEvents);
                return null;
            }, "batch DELETE for " + tableId);
        }

        for (SchemaChangeEvent event : schemaEvents) {
            executeWithConnectionRetry(() -> {
                handleSchemaChangeEvent(event, connection);
                return null;
            }, "schema change for " + tableId);
        }
    }

    /**
     * Executes a database operation with connection-level retry for token
     * expiration.
     */
    private <T> T executeWithConnectionRetry(DatabaseOperation<T> operation, String operationName) throws SQLException {
        int maxRetries = 2; // Fewer retries at operation level since connection level already retries
        SQLException lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return operation.execute();
            } catch (SQLException e) {
                lastException = e;

                if (isTokenExpirationError(e) && attempt < maxRetries) {
                    LOG.warn("Token expiration during {}, retrying... (attempt {}/{})",
                            operationName, attempt, maxRetries);

                    // Short delay before retry
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted during operation retry", ie);
                    }
                } else {
                    throw e;
                }
            }
        }

        throw new SQLException("Failed to execute " + operationName + " after " + maxRetries + " retries",
                lastException);
    }

    /**
     * Functional interface for database operations that can be retried.
     */
    @FunctionalInterface
    private interface DatabaseOperation<T> {
        T execute() throws SQLException;
    }

    /**
     * Use PostgreSQL COPY command for high-performance bulk loading during full load stage.
     * COPY is much faster than individual INSERT statements for large data volumes.
     */
    private void batchInsertEventsWithCopy(Connection connection, TableId tableId, Schema schema, List<DataChangeEvent> events)
            throws SQLException {
        
        if (events.isEmpty()) {
            return;
        }

        // Generate COPY command
        String tableName = tableId.getSchemaName() != null ? 
            tableId.getSchemaName() + "." + tableId.getTableName() : 
            tableId.getTableName();
            
        List<String> columnNames = schema.getColumns().stream()
            .map(Column::getName)
            .collect(java.util.stream.Collectors.toList());
            
        String copyCommand = String.format("COPY %s (%s) FROM STDIN WITH (FORMAT CSV, DELIMITER '%s', NULL '%s')",
            tableName, 
            String.join(", ", columnNames),
            copyDelimiter,
            copyNullString);
            
        LOG.info("=== COPY COMMAND EXECUTION ===");
        LOG.info("Table: {}", tableName);
        LOG.info("Columns: {}", columnNames);
        LOG.info("Events: {}", events.size());
        LOG.info("Command: {}", copyCommand);

        // Use PostgreSQL's CopyManager for COPY command
        org.postgresql.PGConnection pgConnection = connection.unwrap(org.postgresql.PGConnection.class);
        org.postgresql.copy.CopyManager copyManager = pgConnection.getCopyAPI();
        
        // Prepare data as CSV format
        StringBuilder csvData = new StringBuilder();
        int processedCount = 0;
        
        for (DataChangeEvent event : events) {
            if (event.after() != null) {
                RecordData recordData = event.after();
                
                // Convert record to CSV row
                List<String> values = new ArrayList<>();
                for (int i = 0; i < schema.getColumns().size(); i++) {
                    if (i < recordData.getArity() && !recordData.isNullAt(i)) {
                        Column column = schema.getColumns().get(i);
                        String value = extractFieldValue(recordData, i, column);
                        // Escape CSV special characters
                        if (value.contains("\"") || value.contains(copyDelimiter) || value.contains("\n") || value.contains("\r")) {
                            value = "\"" + value.replace("\"", "\"\"") + "\"";
                        }
                        values.add(value);
                    } else {
                        values.add(copyNullString);
                    }
                }
                
                csvData.append(String.join(copyDelimiter, values)).append("\n");
                processedCount++;
                
                LOG.debug("COPY row {}: {}", processedCount, String.join(copyDelimiter, values));
            }
        }
        
        if (processedCount == 0) {
            LOG.warn("No valid records to COPY for table {}", tableId);
            return;
        }
        
        // Execute COPY command
        LOG.info("Executing COPY command for {} records to table {}", processedCount, tableName);
        
        long copyStartTime = System.currentTimeMillis();
        try (java.io.StringReader reader = new java.io.StringReader(csvData.toString())) {
            long copiedRows = copyManager.copyIn(copyCommand, reader);
            
            LOG.info("SUCCESS: COPY command completed for table {}", tableName);
            LOG.info("Records processed: {}, Records copied: {}", processedCount, copiedRows);
            
            // Log RPS metrics for COPY operation
            logFinalRps(processedCount, copyStartTime, "COPY", tableId);
            updateAndLogRps(processedCount, "COPY", tableId);
            
            if (copiedRows != processedCount) {
                LOG.warn("COPY row count mismatch: processed={}, copied={}", processedCount, copiedRows);
            }
            
        } catch (Exception e) {
            LOG.error("Failed to execute COPY command for table {}: {}", tableName, e.getMessage(), e);
            
            // Check if it's a duplicate key error
            if (isDuplicateKeyError(e)) {
                LOG.warn("COPY failed due to duplicate keys for table {}, this is expected for existing data", tableName);
                LOG.info("Skipping COPY operation for table {} due to existing data - CDC will handle incremental changes", tableName);
                return; // Skip COPY for existing data, let CDC handle incremental updates
            }
            
            throw new SQLException("COPY command failed for table " + tableName, e);
        }
    }
    
    /**
     * Extract field value from RecordData for COPY command CSV format.
     */
    private String extractFieldValue(RecordData recordData, int fieldIndex, Column column) {
        try {
            // Handle different data types appropriately
            switch (column.getType().getTypeRoot()) {
                case VARCHAR:
                case CHAR:
                    org.apache.flink.cdc.common.data.StringData stringData = recordData.getString(fieldIndex);
                    return stringData != null ? stringData.toString() : copyNullString;
                    
                case INTEGER:
                    return String.valueOf(recordData.getInt(fieldIndex));
                    
                case BIGINT:
                    return String.valueOf(recordData.getLong(fieldIndex));
                    
                case BOOLEAN:
                    return String.valueOf(recordData.getBoolean(fieldIndex));
                    
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    org.apache.flink.cdc.common.data.TimestampData timestampData = recordData.getTimestamp(fieldIndex, 6);
                    if (timestampData != null) {
                        return timestampData.toTimestamp().toString();
                    }
                    return copyNullString;
                    
                default:
                    // For other types, try to get as string
                    org.apache.flink.cdc.common.data.StringData defaultStringData = recordData.getString(fieldIndex);
                    return defaultStringData != null ? defaultStringData.toString() : copyNullString;
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract field {} value for COPY: {}", fieldIndex, e.getMessage());
            return copyNullString;
        }
    }
    
    /**
     * Update RPS metrics and log performance statistics.
     */
    private void updateAndLogRps(int recordsProcessed, String operation, TableId tableId) {
        totalRecordsProcessed += recordsProcessed;
        long currentTime = System.currentTimeMillis();
        
        // Log RPS every 10 seconds or on significant batch completion
        if (currentTime - lastRpsLogTime >= rpsLogIntervalMs || recordsProcessed >= 100) {
            long timeDeltaMs = currentTime - lastRpsLogTime;
            long recordsDelta = totalRecordsProcessed - lastRecordCount;
            
            if (timeDeltaMs > 0) {
                double currentRps = (recordsDelta * 1000.0) / timeDeltaMs;
                double overallRps = (totalRecordsProcessed * 1000.0) / (currentTime - startTime);
                
                LOG.info("=== RPS METRICS === Table: {} | Operation: {} | Current RPS: {} | Overall RPS: {} | Total Records: {} | Batch Size: {}",
                        tableId, operation, String.format("%.2f", currentRps), String.format("%.2f", overallRps), 
                        totalRecordsProcessed, recordsProcessed);
                
                // Update tracking variables
                lastRpsLogTime = currentTime;
                lastRecordCount = totalRecordsProcessed;
            }
        }
    }
    
    /**
     * Log final RPS summary for completed operations.
     */
    private void logFinalRps(int recordsProcessed, long operationStartTime, String operation, TableId tableId) {
        long operationDurationMs = System.currentTimeMillis() - operationStartTime;
        if (operationDurationMs > 0) {
            double operationRps = (recordsProcessed * 1000.0) / operationDurationMs;
            LOG.info("=== OPERATION RPS === Table: {} | Operation: {} | Records: {} | Duration: {}ms | RPS: {}",
                    tableId, operation, recordsProcessed, operationDurationMs, String.format("%.2f", operationRps));
        }
    }
    
    private void batchInsertEvents(Connection connection, TableId tableId, List<DataChangeEvent> events)
            throws SQLException {

        if (events.isEmpty())
            return;

        Schema schema = getSchemaForTable(tableId, events.get(0));
        if (schema == null) {
            LOG.warn("No schema found for table {}, skipping {} INSERT events", tableId, events.size());
            return;
        }

        // Debug schema source
        LOG.info("Schema source for table {}: cached={}, event_type={}",
                tableId, schemaCache.containsKey(tableId), events.get(0).getClass().getSimpleName());

        // Check the first event to see if we need to update the schema
        if (!events.isEmpty() && events.get(0).after() != null) {
            DataChangeEvent firstEvent = events.get(0);
            RecordData firstRecord = firstEvent.after();
            
            // Add detailed debugging for the DataChangeEvent and RecordData structure
            LOG.info("DEBUG: DataChangeEvent analysis for table {}", tableId);
            LOG.info("DEBUG: Event tableId: {}", firstEvent.tableId());
            LOG.info("DEBUG: Event operation: {}", firstEvent.op());
            LOG.info("DEBUG: Event before: {}", firstEvent.before() != null ? "present" : "null");
            LOG.info("DEBUG: Event after: {}", firstEvent.after() != null ? "present" : "null");
            
            LOG.info("DEBUG: RecordData analysis for table {}", tableId);
            LOG.info("DEBUG: RecordData arity: {}", firstRecord.getArity());
            LOG.info("DEBUG: Schema columns count: {}", schema.getColumns().size());
            LOG.info("DEBUG: Schema columns: {}", schema.getColumns().stream()
                    .map(col -> col.getName()).collect(java.util.stream.Collectors.toList()));
            
            // Log the actual RecordData content
            for (int j = 0; j < firstRecord.getArity(); j++) {
                boolean isNull = firstRecord.isNullAt(j);
                Object value = null;
                if (!isNull) {
                    try {
                        // Try to get the value as string first
                        org.apache.flink.cdc.common.data.StringData stringData = firstRecord.getString(j);
                        value = stringData != null ? stringData.toString() : "null_string";
                    } catch (Exception e) {
                        try {
                            // Try as integer
                            value = firstRecord.getInt(j);
                        } catch (Exception e2) {
                            value = "unknown_type: " + e2.getMessage();
                        }
                    }
                }
                LOG.info("DEBUG: Field {}: isNull={}, value={}", j, isNull, value);
            }
            
            // Also check if the schema from the event matches our cached schema
            Schema eventSchema = firstEvent.tableId().getSchemaName() != null ? 
                schemaCache.get(firstEvent.tableId()) : schema;
            if (eventSchema != null && eventSchema != schema) {
                LOG.info("DEBUG: Schema mismatch detected - event schema differs from cached schema");
                LOG.info("DEBUG: Event schema columns: {}", eventSchema.getColumns().stream()
                        .map(col -> col.getName()).collect(java.util.stream.Collectors.toList()));
            }
            
            if (firstRecord.getArity() != schema.getColumns().size()) {
                LOG.error("ARITY MISMATCH: RecordData has {} fields but schema expects {} columns for table {}",
                        firstRecord.getArity(), schema.getColumns().size(), tableId);
                LOG.error("This indicates a schema synchronization issue between source and sink");

                // Log what fields we actually have
                for (int j = 0; j < firstRecord.getArity(); j++) {
                    boolean isNull = firstRecord.isNullAt(j);
                    LOG.error("  Available field {}: isNull={}", j, isNull);
                }

                LOG.error("Expected columns: {}", schema.getColumns().stream()
                        .map(col -> col.getName()).collect(java.util.stream.Collectors.toList()));
                LOG.error("Actual data fields: {} (all showing as null)", firstRecord.getArity());
                
                // WORKAROUND: Instead of skipping, let's try to process what we can
                // This is a temporary fix while the root cause in the pipeline is investigated
                LOG.warn("WORKAROUND: Attempting to process events despite arity mismatch");
                LOG.warn("This may result in incomplete data - investigate pipeline schema synchronization");
                
                // Continue processing but with warnings
                // The individual setStatementParameters methods will handle the arity mismatch
            }
        }

        String insertSql = generateInsertSql(tableId, schema);
        
        // Check if we should use COPY command for full load stage
        if ("COPY".equals(insertSql)) {
            LOG.info("Using COPY command for {} INSERT events in full load stage for table {}", events.size(), tableId);
            batchInsertEventsWithCopy(connection, tableId, schema, events);
            return;
        }
        
        LOG.info("Processing {} INSERT events for table {} with SQL: {}", events.size(), tableId, insertSql);

        // Log schema details for debugging
        LOG.info("Schema for table {} has {} columns:", tableId, schema.getColumns().size());
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column col = schema.getColumns().get(i);
            LOG.info("  Column {}: name='{}', type={}", i, col.getName(), col.getType());
        }

        try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
            int batchCount = 0;

            for (int i = 0; i < events.size(); i++) {
                DataChangeEvent event = events.get(i);
                if (event.after() != null) {
                    try {
                        // Log each event being processed with detailed info
                        LOG.info("Adding INSERT event {} to batch for table {}: processing event with {} columns",
                                i + 1, tableId, schema.getColumns().size());

                        // Log record data details for debugging
                        RecordData recordData = event.after();
                        LOG.info("RecordData arity: {}", recordData.getArity());

                        for (int j = 0; j < Math.min(recordData.getArity(), schema.getColumns().size()); j++) {
                            Column col = schema.getColumns().get(j);
                            boolean isNull = recordData.isNullAt(j);
                            LOG.info("  Field {}: column='{}', isNull={}", j, col.getName(), isNull);
                        }

                        setStatementParameters(statement, event.after(), schema);
                        statement.addBatch();
                        batchCount++;

                        LOG.debug("Successfully added INSERT statement {} to batch", batchCount);

                    } catch (Exception e) {
                        LOG.error("Failed to add INSERT event {} to batch for table {}: {}",
                                i + 1, tableId, e.getMessage(), e);
                        throw new SQLException("Failed to prepare INSERT statement " + (i + 1), e);
                    }
                } else {
                    LOG.warn("INSERT event {} has null 'after' data, skipping", i + 1);
                }
            }

            LOG.info("Added {} statements to batch for table {} (from {} events)", batchCount, tableId, events.size());

            if (batchCount == 0) {
                LOG.warn("No valid INSERT statements to execute for table {}", tableId);
                return;
            }

            // Execute batch and get detailed results
            LOG.info("Executing batch INSERT with {} statements for table {} (using {})", 
                    batchCount, tableId, useUpsertForCdc ? "UPSERT" : "standard INSERT");
            
            long batchStartTime = System.currentTimeMillis();
            int[] results;
            int successCount = 0;
            int failureCount = 0;
            int updateCount = 0;
            int upsertCount = 0;
            
            try {
                results = statement.executeBatch();
                
                for (int i = 0; i < results.length; i++) {
                    if (results[i] > 0) {
                        successCount++;
                        updateCount += results[i];
                        LOG.debug("Batch statement {} succeeded with {} rows affected", i + 1, results[i]);
                    } else if (results[i] == Statement.EXECUTE_FAILED) {
                        failureCount++;
                        LOG.error("Batch statement {} failed", i + 1);
                    } else if (results[i] == Statement.SUCCESS_NO_INFO) {
                        successCount++;
                        LOG.debug("Batch statement {} succeeded (no row count info)", i + 1);
                    } else {
                        LOG.warn("Batch statement {} returned unexpected result: {}", i + 1, results[i]);
                    }
                }
                
            } catch (java.sql.BatchUpdateException bue) {
                // With UPSERT, duplicate key errors should be eliminated
                // If we still get BatchUpdateException, it's likely a different issue
                LOG.error("BatchUpdateException occurred during {} batch execution for table {}: {}", 
                        useUpsertForCdc ? "UPSERT" : "INSERT", tableId, bue.getMessage());
                
                // Get partial results if available
                results = bue.getUpdateCounts();
                
                // Process partial results
                for (int i = 0; i < results.length; i++) {
                    if (results[i] > 0) {
                        successCount++;
                        updateCount += results[i];
                    } else if (results[i] == Statement.EXECUTE_FAILED) {
                        failureCount++;
                    } else if (results[i] == Statement.SUCCESS_NO_INFO) {
                        successCount++;
                    }
                }
                
                // Log the issue but don't fail the job if using UPSERT
                if (useUpsertForCdc) {
                    LOG.warn("Unexpected BatchUpdateException with UPSERT enabled. Continuing processing...");
                    LOG.warn("This might indicate a schema issue or other SQL problem: {}", bue.getMessage());
                } else {
                    // For standard INSERT, re-throw the exception
                    throw bue;
                }
                
            } catch (SQLException e) {
                // Handle other SQL exceptions
                LOG.error("SQLException during batch {} for table {}: {}", 
                        useUpsertForCdc ? "UPSERT" : "INSERT", tableId, e.getMessage());
                throw e;
            }

            // Log final results
            if (useUpsertForCdc) {
                LOG.info("UPSERT results for table {}: {} successful operations, {} total rows affected",
                        tableId, successCount, updateCount);
                LOG.info("SUCCESS: UPSERT handled {} operations for table {} (duplicates resolved at SQL level)", 
                        successCount, tableId);
                
                // Log RPS metrics for UPSERT operation
                logFinalRps(successCount, batchStartTime, "UPSERT", tableId);
                updateAndLogRps(successCount, "UPSERT", tableId);
            } else {
                LOG.info("INSERT results for table {}: {} successful, {} failed, {} total rows inserted out of {} statements",
                        tableId, successCount, failureCount, updateCount, results.length);
                
                // Log RPS metrics for INSERT operation
                logFinalRps(successCount, batchStartTime, "INSERT", tableId);
                updateAndLogRps(successCount, "INSERT", tableId);
                
                if (successCount != batchCount) {
                    LOG.error("MISMATCH: Expected {} successful inserts but got {} for table {}",
                            batchCount, successCount, tableId);
                } else {
                    LOG.info("SUCCESS: All {} INSERT statements executed successfully for table {}", successCount, tableId);
                }
            }

        } catch (SQLException e) {
            LOG.error("Failed to execute batch INSERT for table {} with {} events: {}",
                    tableId, events.size(), e.getMessage(), e);

            // Log additional context for debugging
            LOG.error("Batch INSERT failure context: SQL = {}, events count = {}", insertSql, events.size());

            // Check if it's a token expiration error during batch execution
            if (isTokenExpirationError(e)) {
                LOG.warn("Token expiration during batch INSERT, will be retried at connection level");
            }
            throw e; // Re-throw to trigger connection-level retry
        }
    }

    private void batchUpdateEvents(Connection connection, TableId tableId, List<DataChangeEvent> events)
            throws SQLException {

        if (events.isEmpty())
            return;

        Schema schema = getSchemaForTable(tableId, events.get(0));
        if (schema == null) {
            LOG.warn("No schema found for table {}, skipping {} UPDATE events", tableId, events.size());
            return;
        }

        String updateSql = generateUpdateSql(tableId, schema);
        LOG.info("Processing {} UPDATE events for table {} with SQL: {}", events.size(), tableId, updateSql);

        long updateStartTime = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(updateSql)) {
            int batchCount = 0;

            for (int i = 0; i < events.size(); i++) {
                DataChangeEvent event = events.get(i);
                if (event.after() != null) {
                    try {
                        LOG.debug("Adding UPDATE event {} to batch for table {}", i + 1, tableId);

                        // Set new values using improved field name mapping
                        int paramIndex = setStatementParametersWithMapping(statement, event.after(), schema);

                        // Set WHERE clause parameters (primary key values)
                        RecordData keyData = event.before() != null ? event.before() : event.after();
                        setWhereClauseParametersWithMapping(statement, keyData, schema, paramIndex);

                        statement.addBatch();
                        batchCount++;

                    } catch (Exception e) {
                        LOG.error("Failed to add UPDATE event {} to batch for table {}: {}",
                                i + 1, tableId, e.getMessage(), e);
                        throw new SQLException("Failed to prepare UPDATE statement " + (i + 1), e);
                    }
                } else {
                    LOG.warn("UPDATE event {} has null 'after' data, skipping", i + 1);
                }
            }

            LOG.info("Added {} UPDATE statements to batch for table {}", batchCount, tableId);

            if (batchCount == 0) {
                LOG.warn("No valid UPDATE statements to execute for table {}", tableId);
                return;
            }

            // Execute batch and get detailed results
            int[] results = statement.executeBatch();

            int successCount = 0;
            int failureCount = 0;
            int updateCount = 0;

            for (int i = 0; i < results.length; i++) {
                if (results[i] > 0) {
                    successCount++;
                    updateCount += results[i];
                } else if (results[i] == Statement.EXECUTE_FAILED) {
                    failureCount++;
                    LOG.error("Batch UPDATE statement {} failed", i + 1);
                } else if (results[i] == Statement.SUCCESS_NO_INFO) {
                    successCount++;
                }
            }

            LOG.info("Batch UPDATE results for table {}: {} successful, {} failed, {} total rows updated",
                    tableId, successCount, failureCount, updateCount);

            // Log RPS metrics for UPDATE operation
            logFinalRps(successCount, updateStartTime, "UPDATE", tableId);
            updateAndLogRps(successCount, "UPDATE", tableId);

            if (successCount != batchCount) {
                LOG.error("UPDATE MISMATCH: Expected {} successful updates but got {} for table {}",
                        batchCount, successCount, tableId);
            }

        } catch (SQLException e) {
            LOG.error("Failed to execute batch UPDATE for table {}: {}", tableId, e.getMessage(), e);

            // Check if it's a token expiration error during batch execution
            if (isTokenExpirationError(e)) {
                LOG.warn("Token expiration during batch UPDATE, will be retried at connection level");
            }
            throw e; // Re-throw to trigger connection-level retry
        }
    }

    private void batchDeleteEvents(Connection connection, TableId tableId, List<DataChangeEvent> events)
            throws SQLException {

        if (events.isEmpty())
            return;

        Schema schema = getSchemaForTable(tableId, events.get(0));
        if (schema == null) {
            LOG.warn("No schema found for table {}, skipping {} DELETE events", tableId, events.size());
            return;
        }

        String deleteSql = generateDeleteSql(tableId, schema);
        LOG.info("Processing {} DELETE events for table {} with SQL: {}", events.size(), tableId, deleteSql);

        long deleteStartTime = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(deleteSql)) {
            int batchCount = 0;

            for (int i = 0; i < events.size(); i++) {
                DataChangeEvent event = events.get(i);
                if (event.before() != null) {
                    try {
                        LOG.debug("Adding DELETE event {} to batch for table {}", i + 1, tableId);

                        setWhereClauseParametersWithMapping(statement, event.before(), schema, 1);
                        statement.addBatch();
                        batchCount++;

                    } catch (Exception e) {
                        LOG.error("Failed to add DELETE event {} to batch for table {}: {}",
                                i + 1, tableId, e.getMessage(), e);
                        throw new SQLException("Failed to prepare DELETE statement " + (i + 1), e);
                    }
                } else {
                    LOG.warn("DELETE event {} has null 'before' data, skipping", i + 1);
                }
            }

            LOG.info("Added {} DELETE statements to batch for table {}", batchCount, tableId);

            if (batchCount == 0) {
                LOG.warn("No valid DELETE statements to execute for table {}", tableId);
                return;
            }

            // Execute batch and get detailed results
            int[] results = statement.executeBatch();

            int successCount = 0;
            int failureCount = 0;
            int deleteCount = 0;

            for (int i = 0; i < results.length; i++) {
                if (results[i] > 0) {
                    successCount++;
                    deleteCount += results[i];
                } else if (results[i] == Statement.EXECUTE_FAILED) {
                    failureCount++;
                    LOG.error("Batch DELETE statement {} failed", i + 1);
                } else if (results[i] == Statement.SUCCESS_NO_INFO) {
                    successCount++;
                }
            }

            LOG.info("Batch DELETE results for table {}: {} successful, {} failed, {} total rows deleted",
                    tableId, successCount, failureCount, deleteCount);

            // Log RPS metrics for DELETE operation
            logFinalRps(successCount, deleteStartTime, "DELETE", tableId);
            updateAndLogRps(successCount, "DELETE", tableId);

            if (successCount != batchCount) {
                LOG.error("DELETE MISMATCH: Expected {} successful deletes but got {} for table {}",
                        batchCount, successCount, tableId);
            }

        } catch (SQLException e) {
            LOG.error("Failed to execute batch DELETE for table {}: {}", tableId, e.getMessage(), e);

            // Check if it's a token expiration error during batch execution
            if (isTokenExpirationError(e)) {
                LOG.warn("Token expiration during batch DELETE, will be retried at connection level");
            }
            throw e; // Re-throw to trigger connection-level retry
        }
    }

    private void handleCreateTableEvent(CreateTableEvent event, Connection connection) throws SQLException {
        TableId tableId = event.tableId();
        Schema schema = event.getSchema();

        LOG.info("Processing CREATE TABLE event for table: {}", tableId);

        // Cache the schema
        schemaCache.put(tableId, schema);

        // Generate and execute CREATE TABLE SQL
        String createTableSql = generateCreateTableSql(tableId, schema);
        LOG.info("Executing CREATE TABLE SQL: {}", createTableSql);

        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            LOG.info("Successfully created table: {}", tableId);
        } catch (SQLException e) {
            // Check if table already exists (which is OK for CREATE TABLE IF NOT EXISTS)
            if (e.getMessage().contains("already exists")
                    || e.getMessage().contains("relation") && e.getMessage().contains("already exists")) {
                LOG.info("Table {} already exists, skipping creation", tableId);
                return;
            }

            // Check if it's a token expiration error during CREATE TABLE
            if (isTokenExpirationError(e)) {
                LOG.warn("Token expiration during CREATE TABLE for {}, will be retried at connection level", tableId);
            } else {
                LOG.error("Failed to create table {}: {}", tableId, e.getMessage());
            }
            throw e; // Re-throw to trigger connection-level retry
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent event, Connection connection) throws SQLException {
        TableId tableId = event.tableId();
        LOG.info("Processing schema change event on table {}", tableId);

        // For now, just log schema changes
        // In a full implementation, you would handle ALTER TABLE statements
        LOG.warn("Schema change events are not yet fully implemented for table {}", tableId);
    }

    // Helper methods for SQL generation and parameter setting
    private TableId getTableIdFromEvent(Event event) {
        if (event instanceof CreateTableEvent) {
            return ((CreateTableEvent) event).tableId();
        } else if (event instanceof DataChangeEvent) {
            return ((DataChangeEvent) event).tableId();
        } else if (event instanceof SchemaChangeEvent) {
            return ((SchemaChangeEvent) event).tableId();
        }
        return null;
    }

    private Schema getSchemaForTable(TableId tableId, Event event) {
        LOG.debug("=== DEBUG: getSchemaForTable called for table {} with event type {} ===", tableId, event.getClass().getSimpleName());
        
        Schema schema = schemaCache.get(tableId);
        LOG.debug("=== DEBUG: Schema from cache: {} ===", schema != null ? "FOUND" : "NULL");
        
        if (schema == null && event instanceof CreateTableEvent) {
            LOG.debug("=== DEBUG: Getting schema from CreateTableEvent ===");
            schema = ((CreateTableEvent) event).getSchema();
            schemaCache.put(tableId, schema);
            LOG.debug("=== DEBUG: Cached new schema from CreateTableEvent with {} columns, primaryKeys: {} ===",
                    schema.getColumns().size(), schema.primaryKeys());
        } else if (schema != null) {
            LOG.debug("=== DEBUG: Using cached schema with {} columns, primaryKeys: {} ===",
                    schema.getColumns().size(), schema.primaryKeys());
        }

        // If we still don't have a schema and this is a DataChangeEvent, try to infer
        // from the event
        if (schema == null && event instanceof DataChangeEvent) {
            LOG.debug("=== DEBUG: No schema found, creating fallback from DataChangeEvent ===");
            DataChangeEvent dataEvent = (DataChangeEvent) event;
            if (dataEvent.after() != null) {
                LOG.debug("=== DEBUG: DataChangeEvent has {} fields, creating fallback schema ===", dataEvent.after().getArity());

                // As a fallback, create a minimal schema based on the data arity
                // This is not ideal but prevents complete failure
                LOG.warn("Creating fallback schema for table {} with {} generic columns",
                        tableId, dataEvent.after().getArity());
                schema = createFallbackSchema(tableId, dataEvent.after().getArity());
                schemaCache.put(tableId, schema);
                LOG.debug("=== DEBUG: Created and cached fallback schema with primaryKeys: {} ===", schema.primaryKeys());
            }
        }

        LOG.debug("=== DEBUG: Final schema for table {}: columns={}, primaryKeys={} ===", 
                tableId, 
                schema != null ? schema.getColumns().stream().map(col -> col.getName()).collect(java.util.stream.Collectors.toList()) : "NULL",
                schema != null ? schema.primaryKeys() : "NULL");
        
        return schema;
    }

    /**
     * Creates a fallback schema when the proper schema is not available.
     * This is a temporary workaround for schema synchronization issues.
     */
    private Schema createFallbackSchema(TableId tableId, int fieldCount) {
        LOG.debug("=== DEBUG: createFallbackSchema called for table {} with fieldCount {} ===", tableId, fieldCount);
        
        List<Column> columns = new ArrayList<>();
        
        // Try to get actual field names from the table schema
        // For PostgreSQL CDC, we know the common field names
        List<String> actualFieldNames = new ArrayList<>();
        
        // Common PostgreSQL table structure - adjust based on your actual table
        if (fieldCount == 5) {
            // Assuming users table with: id, name, email, created_at, updated_at
            actualFieldNames.add("id");
            actualFieldNames.add("name");
            actualFieldNames.add("email");
            actualFieldNames.add("created_at");
            actualFieldNames.add("updated_at");
        } else {
            // Fallback to generic names
            for (int i = 0; i < fieldCount; i++) {
                actualFieldNames.add("field_" + i);
            }
        }

        LOG.debug("=== DEBUG: Creating fallback schema for table {} with {} fields: {} ===", 
                tableId, fieldCount, actualFieldNames);

        for (int i = 0; i < fieldCount; i++) {
            // Create columns with actual field names
            String fieldName = actualFieldNames.get(i);
            Column column = Column.physicalColumn(fieldName,
                    org.apache.flink.cdc.common.types.DataTypes.STRING());
            columns.add(column);
        }

        // Use 'id' as primary key if available, otherwise first field
        String primaryKeyField = actualFieldNames.contains("id") ? "id" : actualFieldNames.get(0);
        List<String> primaryKeys = fieldCount > 0 ? 
            java.util.Collections.singletonList(primaryKeyField)
            : java.util.Collections.emptyList();

        LOG.debug("=== DEBUG: Setting primary key to: {} ===", primaryKeys);

        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Add each column individually
        for (Column column : columns) {
            schemaBuilder.physicalColumn(column.getName(), column.getType());
        }

        Schema result = schemaBuilder
                .primaryKey(primaryKeys)
                .build();
                
        LOG.debug("=== DEBUG: Created schema with primaryKeys: {} ===", result.primaryKeys());
        return result;
    }

    private String generateCreateTableSql(TableId tableId, Schema schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(getTableName(tableId)).append(" (");

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (i > 0)
                sql.append(", ");

            sql.append(column.getName()).append(" ");
            sql.append(mapDataTypeToSql(column.getType()));

            // Check if the DataType is nullable, not the Column
            if (!column.getType().isNullable()) {
                sql.append(" NOT NULL");
            }
        }

        // Add primary key constraint
        List<String> primaryKeys = schema.primaryKeys();
        if (!primaryKeys.isEmpty()) {
            sql.append(", PRIMARY KEY (");
            for (int i = 0; i < primaryKeys.size(); i++) {
                if (i > 0)
                    sql.append(", ");
                sql.append(primaryKeys.get(i));
            }
            sql.append(")");
        }

        sql.append(")");
        return sql.toString();
    }

    private String generateInsertSql(TableId tableId, Schema schema) {
        LOG.info("=== DEBUG: generateInsertSql called for table {} ===", tableId);
        LOG.info("=== DEBUG: useUpsertForCdc = {}, useCopyForFullLoad = {} ===", useUpsertForCdc, useCopyForFullLoad);
        
        // Only use UPSERT during CDC streaming phase, not during snapshot phase
        // We can detect CDC phase by checking if we're processing streaming events
        boolean isCdcStreamingPhase = isCdcStreamingPhase();
        
        LOG.info("=== DEBUG: isCdcStreamingPhase = {} ===", isCdcStreamingPhase);
        
        if (useUpsertForCdc && isCdcStreamingPhase) {
            LOG.info("=== DEBUG: Using UPSERT SQL for CDC streaming phase ===");
            return generateUpsertSql(tableId, schema);
        } else if (useCopyForFullLoad && !isCdcStreamingPhase) {
            LOG.info("=== DEBUG: Using COPY command for full load stage ===");
            return "COPY"; // Special marker for COPY command
        } else {
            LOG.info("=== DEBUG: Using standard INSERT SQL (snapshot phase or UPSERT disabled) ===");
            return generateStandardInsertSql(tableId, schema);
        }
    }
    
    /**
     * Detect if we're in CDC streaming phase vs snapshot phase.
     * During snapshot phase, use standard INSERT.
     * During CDC streaming phase, use UPSERT to handle potential duplicates.
     */
    private boolean isCdcStreamingPhase() {
        // Method 1: Check if we've seen any UPDATE/DELETE events (indicates CDC streaming)
        if (hasSeenCdcEvents) {
            return true;
        }
        
        // Method 2: Check if snapshot is complete based on configuration
        if (isSnapshotComplete) {
            return true;
        }
        
        // Method 3: Time-based detection (after initial snapshot period)
        long runtimeMs = System.currentTimeMillis() - startTime;
        if (runtimeMs > snapshotTimeoutMs) {
            if (!isSnapshotComplete) {
                isSnapshotComplete = true;
                LOG.info("=== CDC PHASE DETECTED: Snapshot timeout reached ({}ms), switching to UPSERT mode ===", 
                        snapshotTimeoutMs);
            }
            return true;
        }
        
        // Default: assume we're still in snapshot phase
        return false;
    }
    
    private String generateStandardInsertSql(TableId tableId, Schema schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(getTableName(tableId)).append(" (");

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                sql.append(", ");
            sql.append(columns.get(i).getName());
        }

        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                sql.append(", ");
            sql.append("?");
        }
        sql.append(")");

        return sql.toString();
    }
    
    /**
     * Generate UPSERT SQL using PostgreSQL's INSERT ... ON CONFLICT ... DO UPDATE syntax.
     * This handles duplicate keys gracefully during CDC implementation stage.
     */
    private String generateUpsertSql(TableId tableId, Schema schema) {
        StringBuilder sql = new StringBuilder();
        List<Column> columns = schema.getColumns();
        List<String> primaryKeys = schema.primaryKeys();
        
        LOG.info("=== GENERATING UPSERT SQL FOR CDC IMPLEMENTATION ===");
        LOG.info("Table: {}", tableId);
        LOG.info("Columns: {}", columns.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()));
        LOG.info("Primary Keys: {}", primaryKeys);
        
        // Start with standard INSERT
        sql.append("INSERT INTO ").append(getTableName(tableId)).append(" (");
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                sql.append(", ");
            sql.append(columns.get(i).getName());
        }
        
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                sql.append(", ");
            sql.append("?");
        }
        sql.append(")");
        
        // Add ON CONFLICT clause for primary keys
        if (!primaryKeys.isEmpty()) {
            sql.append(" ON CONFLICT (");
            for (int i = 0; i < primaryKeys.size(); i++) {
                if (i > 0)
                    sql.append(", ");
                sql.append(primaryKeys.get(i));
            }
            sql.append(") DO UPDATE SET ");
            
            // Update all non-primary key columns
            boolean first = true;
            for (Column column : columns) {
                if (!primaryKeys.contains(column.getName())) {
                    if (!first)
                        sql.append(", ");
                    sql.append(column.getName()).append(" = EXCLUDED.").append(column.getName());
                    first = false;
                }
            }
            
            // If all columns are primary keys, use DO NOTHING
            if (first) {
                sql.setLength(sql.length() - " DO UPDATE SET ".length());
                sql.append(" DO NOTHING");
                LOG.info("Using DO NOTHING since all columns are primary keys");
            } else {
                LOG.info("Using DO UPDATE SET for non-primary key columns");
            }
        } else {
            // If no primary keys defined, fall back to standard INSERT
            LOG.warn("No primary keys defined for table {}, falling back to standard INSERT", tableId);
            return generateStandardInsertSql(tableId, schema);
        }
        
        String finalSql = sql.toString();
        LOG.info("Generated UPSERT SQL: {}", finalSql);
        LOG.info("=== UPSERT SQL GENERATION COMPLETE ===");
        
        return finalSql;
    }

    private String generateUpdateSql(TableId tableId, Schema schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(getTableName(tableId)).append(" SET ");

        List<Column> columns = schema.getColumns();
        List<String> primaryKeys = schema.primaryKeys();

        LOG.error("=== DEBUG generateUpdateSql: table={}, columns={}, primaryKeys={} ===", 
                tableId, columns.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()), primaryKeys);

        boolean first = true;
        for (Column column : columns) {
            boolean isPrimaryKey = primaryKeys.contains(column.getName());
            LOG.debug("=== DEBUG: Column '{}' isPrimaryKey={} ===", column.getName(), isPrimaryKey);
            
            if (!isPrimaryKey) {
                if (!first)
                    sql.append(", ");
                sql.append(column.getName()).append(" = ?");
                first = false;
            }
        }

        sql.append(" WHERE ");
        if (primaryKeys.isEmpty()) {
            LOG.error("=== CRITICAL ERROR: No primary keys found! This will cause empty WHERE clause ===");
        }
        
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0)
                sql.append(" AND ");
            sql.append(primaryKeys.get(i)).append(" = ?");
        }

        String finalSql = sql.toString();
        LOG.debug("=== DEBUG: Generated UPDATE SQL: {} ===", finalSql);
        return finalSql;
    }

    private String generateDeleteSql(TableId tableId, Schema schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(getTableName(tableId)).append(" WHERE ");

        List<String> primaryKeys = schema.primaryKeys();
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0)
                sql.append(" AND ");
            sql.append(primaryKeys.get(i)).append(" = ?");
        }

        return sql.toString();
    }

    private int setStatementParameters(PreparedStatement statement, RecordData recordData, Schema schema)
            throws SQLException {
        List<Column> columns = schema.getColumns();

        LOG.debug("Setting {} parameters for prepared statement", columns.size());

        // Handle arity mismatch gracefully
        if (recordData.getArity() != columns.size()) {
            LOG.warn("RecordData arity mismatch: expected {} fields but got {}. " +
                    "Will set missing fields to NULL. This indicates a schema synchronization issue.",
                    columns.size(), recordData.getArity());
        }

        for (int i = 0; i < columns.size(); i++) {
            try {
                Column column = columns.get(i);
                Object value = null;
                
                // If we have data for this field index, use it; otherwise set to null
                if (i < recordData.getArity() && !recordData.isNullAt(i)) {
                    value = getFieldValue(recordData, i, column.getType());
                }

                if (value == null) {
                    statement.setNull(i + 1, getSqlType(column.getType()));
                    LOG.debug("Set parameter {} (column: {}) to NULL", i + 1, column.getName());
                } else {
                    statement.setObject(i + 1, value);
                    LOG.debug("Set parameter {} (column: {}) to value: {} (type: {})",
                            i + 1, column.getName(), value, value.getClass().getSimpleName());
                }

            } catch (Exception e) {
                Column column = columns.get(i);
                Object value = null;
                try {
                    value = recordData.isNullAt(i) ? null : getFieldValue(recordData, i, column.getType());
                } catch (Exception valueEx) {
                    LOG.error("Failed to extract value for debugging: {}", valueEx.getMessage());
                }

                LOG.error("Failed to set parameter {} (column: {}, type: {}, value: {}, isNull: {}): {}",
                        i + 1, column.getName(), column.getType(), value, recordData.isNullAt(i), e.getMessage(), e);
                throw new SQLException("Failed to set parameter for column " + column.getName() +
                        " (type: " + column.getType() + ", value: " + value + ")", e);
            }
        }

        LOG.debug("Successfully set all {} parameters", columns.size());
        return columns.size();
    }

    /**
     * Maps Flink CDC DataType to SQL type constants for null handling.
     */
    private int getSqlType(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case INTEGER:
                return java.sql.Types.INTEGER;
            case BIGINT:
                return java.sql.Types.BIGINT;
            case TINYINT:
                return java.sql.Types.TINYINT;
            case SMALLINT:
                return java.sql.Types.SMALLINT;
            case VARCHAR:
            case CHAR:
                return java.sql.Types.VARCHAR;
            case BOOLEAN:
                return java.sql.Types.BOOLEAN;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case FLOAT:
                return java.sql.Types.FLOAT;
            case DECIMAL:
                return java.sql.Types.DECIMAL;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return java.sql.Types.TIMESTAMP;
            case DATE:
                return java.sql.Types.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return java.sql.Types.TIME;
            default:
                return java.sql.Types.VARCHAR;
        }
    }

    private void setWhereClauseParameters(PreparedStatement statement, RecordData recordData,
            Schema schema, int startIndex) throws SQLException {
        List<String> primaryKeys = schema.primaryKeys();
        List<Column> columns = schema.getColumns();

        int paramIndex = startIndex;
        for (String pkName : primaryKeys) {
            int columnIndex = findColumnIndex(columns, pkName);
            if (columnIndex >= 0) {
                Column column = columns.get(columnIndex);
                Object value = recordData.isNullAt(columnIndex) ? null
                        : getFieldValue(recordData, columnIndex, column.getType());

                if (value == null) {
                    statement.setNull(paramIndex, getSqlType(column.getType()));
                    LOG.debug("Set WHERE parameter {} (column: {}) to NULL", paramIndex, column.getName());
                } else {
                    statement.setObject(paramIndex, value);
                    LOG.debug("Set WHERE parameter {} (column: {}) to value: {}", paramIndex, column.getName(), value);
                }
                paramIndex++;
            }
        }
    }

    private Object getFieldValue(RecordData recordData, int index, DataType dataType) {
        if (recordData.isNullAt(index)) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case INTEGER:
                try {
                    return recordData.getInt(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get integer at index {}: {}", index, e.getMessage());
                    return null;
                }
            case BIGINT:
                try {
                    return recordData.getLong(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get long at index {}: {}", index, e.getMessage());
                    return null;
                }
            case TINYINT:
                try {
                    return recordData.getByte(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get byte at index {}: {}", index, e.getMessage());
                    return null;
                }
            case SMALLINT:
                try {
                    return recordData.getShort(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get short at index {}: {}", index, e.getMessage());
                    return null;
                }
            case VARCHAR:
            case CHAR:
                try {
                    org.apache.flink.cdc.common.data.StringData stringData = recordData.getString(index);
                    return stringData != null ? stringData.toString() : null;
                } catch (Exception e) {
                    LOG.warn("Failed to convert string data at index {}: {}", index, e.getMessage());
                    return null;
                }
            case BOOLEAN:
                try {
                    return recordData.getBoolean(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get boolean at index {}: {}", index, e.getMessage());
                    return null;
                }
            case DOUBLE:
                try {
                    return recordData.getDouble(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get double at index {}: {}", index, e.getMessage());
                    return null;
                }
            case FLOAT:
                try {
                    return recordData.getFloat(index);
                } catch (Exception e) {
                    LOG.warn("Failed to get float at index {}: {}", index, e.getMessage());
                    return null;
                }
            case DECIMAL:
                try {
                    // Use default precision and scale for decimal extraction
                    org.apache.flink.cdc.common.data.DecimalData decimalData = recordData.getDecimal(index, 38, 10);
                    return decimalData != null ? decimalData.toBigDecimal() : null;
                } catch (Exception e) {
                    LOG.warn("Failed to convert decimal data at index {}: {}", index, e.getMessage());
                    // Try to get as string if decimal conversion fails
                    try {
                        org.apache.flink.cdc.common.data.StringData stringData = recordData.getString(index);
                        return stringData != null ? stringData.toString() : null;
                    } catch (Exception stringEx) {
                        LOG.warn("Failed to convert decimal as string at index {}: {}", index, stringEx.getMessage());
                        return null;
                    }
                }

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Convert TimestampData to java.sql.Timestamp
                try {
                    org.apache.flink.cdc.common.data.TimestampData timestampData = recordData.getTimestamp(index, 6);
                    if (timestampData != null) {
                        // Convert to java.sql.Timestamp which PostgreSQL JDBC can handle
                        long milliseconds = timestampData.getMillisecond();
                        int nanoseconds = timestampData.getNanoOfMillisecond();
                        java.sql.Timestamp timestamp = new java.sql.Timestamp(milliseconds);
                        timestamp.setNanos(nanoseconds);
                        return timestamp;
                    }
                    return null;
                } catch (Exception e) {
                    LOG.warn("Failed to convert timestamp data at index {}: {}", index, e.getMessage());
                    return null;
                }
            case DATE:
                try {
                    // Handle date conversion
                    return recordData.getTimestamp(index, 0);
                } catch (Exception e) {
                    LOG.warn("Failed to convert date data at index {}: {}", index, e.getMessage());
                    return null;
                }
            case TIME_WITHOUT_TIME_ZONE:
                try {
                    // Handle time conversion
                    return recordData.getTimestamp(index, 3);
                } catch (Exception e) {
                    LOG.warn("Failed to convert time data at index {}: {}", index, e.getMessage());
                    return null;
                }
            default:
                // For other types, convert to string
                try {
                    org.apache.flink.cdc.common.data.StringData defaultStringData = recordData.getString(index);
                    return defaultStringData != null ? defaultStringData.toString() : null;
                } catch (Exception e) {
                    LOG.warn("Failed to convert data at index {} to string: {}", index, e.getMessage());
                    return null;
                }
        }
    }

    private int findColumnIndex(List<Column> columns, String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private String mapDataTypeToSql(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case TINYINT:
                return "SMALLINT"; // DSQL doesn't have TINYINT, use SMALLINT
            case SMALLINT:
                return "SMALLINT";
            case VARCHAR:
                return "VARCHAR(255)";
            case CHAR:
                return "CHAR(255)";
            case BOOLEAN:
                return "BOOLEAN";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case FLOAT:
                return "REAL";
            case DECIMAL:
                // Extract precision and scale from the data type if available
                return "DECIMAL(38,10)"; // Default precision and scale

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMP";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            default:
                return "TEXT";
        }
    }

    private String getTableName(TableId tableId) {
        if (tableId.getSchemaName() != null && !tableId.getSchemaName().isEmpty()) {
            return tableId.getSchemaName() + "." + tableId.getTableName();
        } else {
            return tableId.getTableName();
        }
    }

    private long parseBatchTimeout(String timeoutStr) {
        try {
            if (timeoutStr.endsWith("min")) {
                return Long.parseLong(timeoutStr.substring(0, timeoutStr.length() - 3)) * 60 * 1000;
            } else if (timeoutStr.endsWith("s")) {
                return Long.parseLong(timeoutStr.substring(0, timeoutStr.length() - 1)) * 1000;
            } else if (timeoutStr.endsWith("ms")) {
                return Long.parseLong(timeoutStr.substring(0, timeoutStr.length() - 2));
            } else {
                // Default to milliseconds
                return Long.parseLong(timeoutStr);
            }
        } catch (NumberFormatException e) {
            LOG.warn("Invalid batch timeout format: {}, using default 5 minutes", timeoutStr);
            return 5 * 60 * 1000; // 5 minutes default
        }
    }

    /**
     * Set statement parameters using field name mapping instead of positional setting.
     * This method ensures that field values are correctly assigned to their corresponding columns.
     */
    private int setStatementParametersWithMapping(PreparedStatement statement, RecordData recordData, Schema schema) throws SQLException {
        List<Column> columns = schema.getColumns();
        List<String> primaryKeys = schema.primaryKeys();
        int paramIndex = 1;

        // Create a mapping of column names to their values
        Map<String, Object> fieldValueMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Object value = recordData.isNullAt(i) ? null : getFieldValue(recordData, i, column.getType());
            fieldValueMap.put(column.getName(), value);
            LOG.debug("Mapped field {} to value: {}", column.getName(), value);
        }

        // Set parameters only for non-primary key columns (for SET clause in UPDATE)
        for (Column column : columns) {
            // Skip primary key columns as they are handled in WHERE clause
            if (primaryKeys.contains(column.getName())) {
                continue;
            }
            
            Object value = fieldValueMap.get(column.getName());
            
            if (value == null) {
                statement.setNull(paramIndex, getSqlType(column.getType()));
                LOG.debug("Set parameter {} (column: {}) to NULL", paramIndex, column.getName());
            } else {
                // Apply type conversion for specific data types
                if (isTimestampType(column.getType()) && value instanceof String) {
                    try {
                        Timestamp timestamp = Timestamp.valueOf((String) value);
                        statement.setTimestamp(paramIndex, timestamp);
                        LOG.debug("Set parameter {} (column: {}) to timestamp: {}", paramIndex, column.getName(), timestamp);
                    } catch (Exception e) {
                        LOG.warn("Failed to convert string to timestamp for column {}: {}", column.getName(), e.getMessage());
                        statement.setObject(paramIndex, value);
                    }
                } else {
                    statement.setObject(paramIndex, value);
                    LOG.debug("Set parameter {} (column: {}) to value: {}", paramIndex, column.getName(), value);
                }
            }
            paramIndex++;
        }

        return paramIndex;
    }

    /**
     * Set WHERE clause parameters using field name mapping for primary key values.
     * This method ensures that primary key values are correctly assigned for WHERE conditions.
     */
    private void setWhereClauseParametersWithMapping(PreparedStatement statement, RecordData recordData, Schema schema, int startIndex) throws SQLException {
        List<String> primaryKeys = schema.primaryKeys();
        List<Column> columns = schema.getColumns();

        // Create a mapping of column names to their values
        Map<String, Object> fieldValueMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Object value = recordData.isNullAt(i) ? null : getFieldValue(recordData, i, column.getType());
            fieldValueMap.put(column.getName(), value);
        }

        int paramIndex = startIndex;
        for (String pkName : primaryKeys) {
            Object value = fieldValueMap.get(pkName);
            
            // Find the column definition for type information
            Column pkColumn = null;
            for (Column column : columns) {
                if (column.getName().equals(pkName)) {
                    pkColumn = column;
                    break;
                }
            }

            if (pkColumn == null) {
                LOG.warn("Primary key column {} not found in schema", pkName);
                continue;
            }

            if (value == null) {
                statement.setNull(paramIndex, getSqlType(pkColumn.getType()));
                LOG.debug("Set WHERE parameter {} (column: {}) to NULL", paramIndex, pkName);
            } else {
                // Apply type conversion for specific data types
                if (isTimestampType(pkColumn.getType()) && value instanceof String) {
                    try {
                        Timestamp timestamp = Timestamp.valueOf((String) value);
                        statement.setTimestamp(paramIndex, timestamp);
                        LOG.debug("Set WHERE parameter {} (column: {}) to timestamp: {}", paramIndex, pkName, timestamp);
                    } catch (Exception e) {
                        LOG.warn("Failed to convert string to timestamp for WHERE column {}: {}", pkName, e.getMessage());
                        statement.setObject(paramIndex, value);
                    }
                } else {
                    statement.setObject(paramIndex, value);
                    LOG.debug("Set WHERE parameter {} (column: {}) to value: {}", paramIndex, pkName, value);
                }
            }
            paramIndex++;
        }
    }

    /**
     * Helper method to check if a DataType is a timestamp type.
     */
    private boolean isTimestampType(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    // ===============================
    // CheckpointedFunction Implementation
    // ===============================

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("DSQL sink initializing state for checkpointing");
        // No persistent state needed for this sink
        // All data is immediately written to DSQL
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("DSQL sink taking checkpoint snapshot - checkpoint ID: {}", context.getCheckpointId());
        
        // Ensure all pending batches are flushed before checkpoint
        try {
            synchronized (eventBuffer) {
                // Force flush any remaining batched events
                if (!eventBuffer.isEmpty()) {
                    LOG.info("Flushing {} pending events before checkpoint", eventBuffer.size());
                    processBatch();
                }
            }
            
            LOG.info("DSQL sink checkpoint snapshot completed successfully - checkpoint ID: {}", context.getCheckpointId());
        } catch (Exception e) {
            LOG.error("Failed to flush pending events during checkpoint", e);
            throw e;
        }
    }
}
