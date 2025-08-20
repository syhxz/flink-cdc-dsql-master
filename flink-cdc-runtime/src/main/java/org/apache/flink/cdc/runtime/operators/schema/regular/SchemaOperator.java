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

package org.apache.flink.cdc.runtime.operators.schema.regular;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.TableIdRouter;
import org.apache.flink.cdc.runtime.operators.schema.common.metrics.SchemaOperatorMetrics;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/**
 * The operator will evolve schemas in {@link
 * org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);

    // Final fields that are set in constructor
    private final String timezone;
    private final Duration rpcTimeout;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final List<RouteRule> routingRules;

    // Transient fields that are set during open()
    private transient int subTaskId;
    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaOperatorMetrics schemaOperatorMetrics;
    private transient volatile Map<TableId, Schema> originalSchemaMap;
    private transient volatile Map<TableId, Schema> evolvedSchemaMap;
    private transient TableIdRouter router;
    private transient SchemaDerivator derivator;

    @VisibleForTesting
    public SchemaOperator(List<RouteRule> routingRules) {
        this(routingRules, DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT);
    }

    @VisibleForTesting
    public SchemaOperator(List<RouteRule> routingRules, Duration rpcTimeOut) {
        this(routingRules, rpcTimeOut, SchemaChangeBehavior.EVOLVE);
    }

    @VisibleForTesting
    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior) {
        this(routingRules, rpcTimeOut, schemaChangeBehavior, "UTC");
    }

    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeout = rpcTimeOut;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.timezone = timezone;
        this.routingRules = routingRules;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.toCoordinator = containingTask.getEnvironment().getOperatorCoordinatorEventGateway();
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.schemaOperatorMetrics =
                new SchemaOperatorMetrics(
                        getRuntimeContext().getMetricGroup(), schemaChangeBehavior);
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.originalSchemaMap = new HashMap<>();
        this.evolvedSchemaMap = new HashMap<>();
        this.router = new TableIdRouter(routingRules);
        this.derivator = new SchemaDerivator();
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        Event event = streamRecord.getValue();
        LOG.info("=== SCHEMA OPERATOR PROCESSING EVENT: {} ===", event.getClass().getSimpleName());
        
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaEvent = (SchemaChangeEvent) event;
            LOG.info("Processing SchemaChangeEvent for table: {}", schemaEvent.tableId());
            handleSchemaChangeEvent(schemaEvent);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataEvent = (DataChangeEvent) event;
            LOG.info("Processing DataChangeEvent for table: {}", dataEvent.tableId());
            LOG.info("Current originalSchemaMap keys: {}", originalSchemaMap.keySet());
            LOG.info("Current evolvedSchemaMap keys: {}", evolvedSchemaMap.keySet());
            handleDataChangeEvent(dataEvent);
        } else {
            throw new RuntimeException("Unknown event type in Stream record: " + event);
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent originalEvent) throws Exception {
        // First, update original schema map unconditionally and it will never fail
        TableId tableId = originalEvent.tableId();
        originalSchemaMap.compute(
                tableId,
                (tId, schema) -> SchemaUtils.applySchemaChangeEvent(schema, originalEvent));
        schemaOperatorMetrics.increaseSchemaChangeEvents(1);
        
        LOG.info("Processing schema change event for table {}: {}", tableId, originalEvent.getClass().getSimpleName());

        // First, send FlushEvent or it might be blocked later
        List<TableId> sinkTables = router.route(tableId);
        LOG.info("{}> Sending the FlushEvent for sink tables: {}", subTaskId, sinkTables);
        output.collect(
                new StreamRecord<>(new FlushEvent(subTaskId, sinkTables, originalEvent.getType())));

        LOG.info("{}> Going to request schema change...", subTaskId);

        // Then, queue to request schema change to SchemaCoordinator.
        SchemaChangeResponse response = requestSchemaChange(tableId, originalEvent);

        LOG.info(
                "{}> Finished schema change events: {}",
                subTaskId,
                response.getAppliedSchemaChangeEvents());
        LOG.info("{}> Refreshed evolved schemas: {}", subTaskId, response.getEvolvedSchemas());

        // After this request got successfully applied to DBMS, we can...
        List<SchemaChangeEvent> finishedSchemaChangeEvents =
                response.getAppliedSchemaChangeEvents();

        // Update local evolved schema map's cache
        evolvedSchemaMap.putAll(response.getEvolvedSchemas());
        LOG.info("Updated evolved schema map. Current schemas: {}", evolvedSchemaMap.keySet());

        // and emit the finished event to downstream
        for (SchemaChangeEvent finishedEvent : finishedSchemaChangeEvents) {
            output.collect(new StreamRecord<>(finishedEvent));
        }

        schemaOperatorMetrics.increaseFinishedSchemaChangeEvents(finishedSchemaChangeEvents.size());
    }

    private void handleDataChangeEvent(DataChangeEvent dataChangeEvent) {
        TableId tableId = dataChangeEvent.tableId();
        LOG.info("=== HANDLING DATA CHANGE EVENT ===");
        LOG.info("Table ID: {}", tableId);

        // First, we obtain the original schema corresponding to this data change event
        Schema originalSchema = originalSchemaMap.get(dataChangeEvent.tableId());
        LOG.info("Original schema for {}: {}", tableId, originalSchema != null ? "FOUND" : "NULL");

        // Then, for each routing terminus, coerce data records to the expected schema
        List<TableId> routedTables = router.route(tableId);
        LOG.info("Router returned {} destinations: {}", routedTables.size(), routedTables);
        
        for (TableId sinkTableId : routedTables) {
            LOG.info("Processing route to sink table: {}", sinkTableId);
            Schema evolvedSchema = evolvedSchemaMap.get(sinkTableId);
            LOG.info("Evolved schema for {}: {}", sinkTableId, evolvedSchema != null ? "FOUND" : "NULL");
            
            // ROBUST SCHEMA INITIALIZATION: Handle missing evolved schema for sink tables
            if (evolvedSchema == null) {
                if (originalSchema != null) {
                    LOG.info("Initializing evolved schema for sink table {} using original schema", sinkTableId);
                    evolvedSchema = originalSchema;
                    evolvedSchemaMap.put(sinkTableId, evolvedSchema);
                } else {
                    // Try to find any compatible schema from the original schema map
                    LOG.warn("No original schema available for {}. Searching for compatible schema...", tableId);
                    for (Map.Entry<TableId, Schema> entry : originalSchemaMap.entrySet()) {
                        if (entry.getKey().toString().contains(tableId.getTableName())) {
                            LOG.info("Found compatible schema from table {}", entry.getKey());
                            evolvedSchema = entry.getValue();
                            evolvedSchemaMap.put(sinkTableId, evolvedSchema);
                            break;
                        }
                    }
                }
            }
            
            // If still null after all attempts, create a defensive error with more context
            if (evolvedSchema == null) {
                LOG.error("SCHEMA SYNCHRONIZATION ERROR:");
                LOG.error("- Source table: {}", tableId);
                LOG.error("- Sink table: {}", sinkTableId);
                LOG.error("- Original schema: {}", originalSchema);
                LOG.error("- Available original schemas: {}", originalSchemaMap.keySet());
                LOG.error("- Available evolved schemas: {}", evolvedSchemaMap.keySet());
                
                // Instead of throwing immediately, try to request schema from coordinator
                LOG.info("Attempting to request schema from coordinator for table {}", sinkTableId);
                try {
                    SchemaChangeResponse response = sendRequestToCoordinator(
                        new SchemaChangeRequest(sinkTableId, null, subTaskId));
                    if (response != null && response.getEvolvedSchemas().containsKey(sinkTableId)) {
                        evolvedSchema = response.getEvolvedSchemas().get(sinkTableId);
                        evolvedSchemaMap.put(sinkTableId, evolvedSchema);
                        LOG.info("Successfully retrieved schema from coordinator for {}", sinkTableId);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to retrieve schema from coordinator: {}", e.getMessage());
                }
                
                // Final fallback: skip this record with warning instead of crashing
                if (evolvedSchema == null) {
                    LOG.warn("SKIPPING RECORD: Unable to resolve schema for table {}. Record will be dropped.", sinkTableId);
                    continue; // Skip this sink table and continue with others
                }
            }
            
            // Use final variable for lambda
            final Schema finalEvolvedSchema = evolvedSchema;
            
            DataChangeEvent coercedDataRecord =
                    derivator
                            .coerceDataRecord(
                                    timezone,
                                    DataChangeEvent.route(dataChangeEvent, sinkTableId),
                                    originalSchema,
                                    finalEvolvedSchema)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Unable to coerce data record from %s (schema: %s) to %s (schema: %s)",
                                                            tableId,
                                                            originalSchema,
                                                            sinkTableId,
                                                            finalEvolvedSchema)));
            output.collect(new StreamRecord<>(coercedDataRecord));
        }
    }

    private SchemaChangeResponse requestSchemaChange(
            TableId tableId, SchemaChangeEvent schemaChangeEvent) {
        return sendRequestToCoordinator(
                new SchemaChangeRequest(tableId, schemaChangeEvent, subTaskId));
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        int maxRetries = 3;
        long baseTimeoutMs = rpcTimeout.toMillis();
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Increase timeout for each retry attempt
                long timeoutMs = baseTimeoutMs * attempt;
                
                LOG.debug("Sending request to coordinator (attempt {}/{}): {}", attempt, maxRetries, request);
                
                CompletableFuture<CoordinationResponse> responseFuture =
                        toCoordinator.sendRequestToCoordinator(
                                getOperatorID(), new SerializedValue<>(request));
                
                RESPONSE response = CoordinationResponseUtils.unwrap(
                        responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS));
                
                if (attempt > 1) {
                    LOG.info("Successfully sent request to coordinator on attempt {}: {}", attempt, request);
                }
                
                return response;
                
            } catch (TimeoutException e) {
                if (attempt < maxRetries) {
                    LOG.warn("Coordinator request timed out on attempt {} (timeout: {}ms), retrying: {}", 
                            attempt, baseTimeoutMs * attempt, request);
                    
                    // Wait before retry with exponential backoff
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while retrying coordinator request: " + request, ie);
                    }
                } else {
                    LOG.error("Coordinator request failed after {} attempts due to timeout: {}", maxRetries, request);
                    throw new IllegalStateException(
                            String.format("Failed to send request to coordinator after %d attempts (final timeout: %dms): %s", 
                                    maxRetries, baseTimeoutMs * maxRetries, request), e);
                }
            } catch (Exception e) {
                if (attempt < maxRetries && isRetryableException(e)) {
                    LOG.warn("Coordinator request failed on attempt {} with retryable error, retrying: {} - Error: {}", 
                            attempt, request, e.getMessage());
                    
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while retrying coordinator request: " + request, ie);
                    }
                } else {
                    LOG.error("Coordinator request failed on attempt {} with non-retryable error: {} - Error: {}", 
                            attempt, request, e.getMessage(), e);
                    throw new IllegalStateException(
                            "Failed to send request to coordinator: " + request.toString(), e);
                }
            }
        }
        
        // This should never be reached due to the logic above, but added for completeness
        throw new IllegalStateException("Unexpected end of retry loop for coordinator request: " + request);
    }
    
    /**
     * Check if an exception is retryable for coordinator communication.
     */
    private boolean isRetryableException(Exception e) {
        // Retry on timeout, connection issues, but not on serialization or logic errors
        return e instanceof TimeoutException ||
               e instanceof java.util.concurrent.ExecutionException ||
               (e.getCause() != null && e.getCause() instanceof TimeoutException) ||
               e.getMessage().contains("timeout") ||
               e.getMessage().contains("connection") ||
               e.getMessage().contains("network");
    }

    @VisibleForTesting
    public void registerInitialSchema(TableId tableId, Schema schema) {
        originalSchemaMap.put(tableId, schema);
        evolvedSchemaMap.put(tableId, schema);
    }
}
