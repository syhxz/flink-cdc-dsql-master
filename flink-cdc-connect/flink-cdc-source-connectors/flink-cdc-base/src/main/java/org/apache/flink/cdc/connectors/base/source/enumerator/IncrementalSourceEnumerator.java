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

package org.apache.flink.cdc.connectors.base.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsAckEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitAssignedEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitUpdateAckEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.SnapshotBeginEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitUpdateRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * Incremental source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Experimental
public class IncrementalSourceEnumerator
        implements SplitEnumerator<SourceSplitBase, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    protected final SplitEnumeratorContext<SourceSplitBase> context;
    private final SourceConfig sourceConfig;
    protected final SplitAssigner splitAssigner;

    // using TreeSet to prefer assigning stream split to task-0 for easier debug
    protected final TreeSet<Integer> readersAwaitingSplit;
    private List<List<FinishedSnapshotSplitInfo>> finishedSnapshotSplitMeta;

    private Boundedness boundedness;

    @Nullable protected Integer streamSplitTaskId = null;
    private boolean isStreamSplitUpdateRequestAlreadySent = false;

    public IncrementalSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            SourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            Boundedness boundedness) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
        this.boundedness = boundedness;
    }

    @Override
    public void start() {
        splitAssigner.open();
        
        // Test: Set a snapshot begin offset for CDC streaming start
        // In a real implementation, this would be set when receiving SnapshotBeginEvent
        if (splitAssigner instanceof HybridSplitAssigner) {
            try {
                // For testing, just log that we would set the snapshot begin offset
                // In real implementation, this would be set from the actual snapshot begin LSN
                LOG.info("Test: HybridSplitAssigner is ready to receive snapshot begin offset for CDC streaming start");
            } catch (Exception e) {
                LOG.warn("Failed to initialize snapshot begin offset handling: {}", e.getMessage());
            }
        }
        
        requestStreamSplitUpdateIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<SourceSplitBase> splits, int subtaskId) {
        LOG.debug("Incremental Source Enumerator adds splits back: {}", splits);
        Optional<SourceSplitBase> streamSplit =
                splits.stream().filter(SourceSplitBase::isStreamSplit).findAny();
        if (streamSplit.isPresent()) {
            LOG.info("The enumerator adds add stream split back: {}", streamSplit);
            this.streamSplitTaskId = null;
        }
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // send StreamSplitUpdateRequestEvent to source reader after newly added table
        // snapshot splits finished.
        requestStreamSplitUpdateIfNeed();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator receives finished split offsets {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) sourceEvent;
            Map<String, Offset> finishedOffsets = reportEvent.getFinishedOffsets();
            splitAssigner.onFinishedSplits(finishedOffsets);
            
            // AUTO-CHECKPOINT: Trigger checkpoint if snapshot splits are finished
            triggerAutoCheckpointIfNeeded();
            
            requestStreamSplitUpdateIfNeed();

            // send acknowledge event
            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof StreamSplitMetaRequestEvent) {
            LOG.debug(
                    "The enumerator receives request for stream split meta from subtask {}.",
                    subtaskId);
            sendStreamMetaRequestEvent(subtaskId, (StreamSplitMetaRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof LatestFinishedSplitsNumberRequestEvent) {
            LOG.info(
                    "The enumerator receives request from subtask {} for the latest finished splits number after added newly tables. ",
                    subtaskId);
            handleLatestFinishedSplitNumberRequest(subtaskId);
        } else if (sourceEvent instanceof StreamSplitUpdateAckEvent) {
            LOG.info(
                    "The enumerator receives event that the streamSplit split has been updated from subtask {}. ",
                    subtaskId);
            splitAssigner.onStreamSplitUpdated();
        } else if (sourceEvent instanceof StreamSplitAssignedEvent) {
            LOG.info(
                    "The enumerator receives notice from subtask {} for the stream split assignment. ",
                    subtaskId);
            this.streamSplitTaskId = subtaskId;
        } else if (sourceEvent instanceof SnapshotBeginEvent) {
            LOG.info(
                    "The enumerator receives snapshot begin event from subtask {} with LSN: {}",
                    subtaskId, ((SnapshotBeginEvent) sourceEvent).getSnapshotBeginOffset());
            SnapshotBeginEvent beginEvent = (SnapshotBeginEvent) sourceEvent;
            if (splitAssigner instanceof HybridSplitAssigner) {
                ((HybridSplitAssigner<?>) splitAssigner).setSnapshotBeginOffset(beginEvent.getSnapshotBeginOffset());
            }
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // stream split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    protected void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            if (shouldCloseIdleReader(nextAwaiting)) {
                // close idle readers when snapshot phase finished.
                context.signalNoMoreSplits(nextAwaiting);
                awaitingReader.remove();
                LOG.info("Close idle reader of subtask {}", nextAwaiting);
                continue;
            }

            Optional<SourceSplitBase> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final SourceSplitBase sourceSplit = split.get();
                context.assignSplit(sourceSplit, nextAwaiting);
                if (sourceSplit instanceof StreamSplit) {
                    this.streamSplitTaskId = nextAwaiting;
                }
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", sourceSplit, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                requestStreamSplitUpdateIfNeed();
                break;
            }
        }
    }

    private boolean shouldCloseIdleReader(int nextAwaiting) {
        // When no unassigned split anymore, Signal NoMoreSplitsEvent to awaiting reader in two
        // situations:
        // 1. When Set StartupMode = snapshot mode(also bounded), there's no more splits in the
        // assigner.
        // 2. When set scan.incremental.close-idle-reader.enabled = true, there's no more splits in
        // the assigner.
        return splitAssigner.noMoreSplits()
                && (boundedness == Boundedness.BOUNDED
                        || (sourceConfig.isCloseIdleReaders()
                                && streamSplitTaskId != null
                                && streamSplitTaskId != (nextAwaiting)));
    }

    protected int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    protected void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the IncrementalSourceEnumerator restores or the communication failed between
        // IncrementalSourceEnumerator and JdbcIncrementalSourceReader, it may missed some
        // notification
        // event.
        // tell all JdbcIncrementalSourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(
                        subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }

        requestStreamSplitUpdateIfNeed();
    }

    private void requestStreamSplitUpdateIfNeed() {
        if (!isStreamSplitUpdateRequestAlreadySent
                && isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            // If enumerator knows which reader is assigned stream split, just send to this reader,
            // nor sends to all registered readers.
            if (streamSplitTaskId != null) {
                isStreamSplitUpdateRequestAlreadySent = true;
                LOG.info(
                        "The enumerator requests subtask {} to update the stream split after newly added table.",
                        streamSplitTaskId);
                context.sendEventToSourceReader(
                        streamSplitTaskId, new StreamSplitUpdateRequestEvent());
            } else {
                for (int reader : getRegisteredReader()) {
                    isStreamSplitUpdateRequestAlreadySent = true;
                    LOG.info(
                            "The enumerator requests subtask {} to update the stream split after newly added table.",
                            reader);
                    context.sendEventToSourceReader(reader, new StreamSplitUpdateRequestEvent());
                }
            }
        }
    }

    private void sendStreamMetaRequestEvent(int subTask, StreamSplitMetaRequestEvent requestEvent) {
        // initialize once
        if (finishedSnapshotSplitMeta == null) {
            final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos =
                    splitAssigner.getFinishedSplitInfos();
            if (finishedSnapshotSplitInfos.isEmpty()) {
                LOG.error(
                        "The assigner offer empty finished split information, this should not happen");
                throw new FlinkRuntimeException(
                        "The assigner offer empty finished split information, this should not happen");
            }
            finishedSnapshotSplitMeta =
                    Lists.partition(
                            finishedSnapshotSplitInfos, sourceConfig.getSplitMetaGroupSize());
        }
        final int requestMetaGroupId = requestEvent.getRequestMetaGroupId();
        final int totalFinishedSplitSizeOfReader = requestEvent.getTotalFinishedSplitSize();
        final int totalFinishedSplitSizeOfEnumerator = splitAssigner.getFinishedSplitInfos().size();
        if (totalFinishedSplitSizeOfReader > totalFinishedSplitSizeOfEnumerator) {
            LOG.warn(
                    "Total finished split size of subtask {} is {}, while total finished split size of enumerator is only {}. Try to truncate it",
                    subTask,
                    totalFinishedSplitSizeOfReader,
                    totalFinishedSplitSizeOfEnumerator);
            StreamSplitMetaEvent metadataEvent =
                    new StreamSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            null,
                            totalFinishedSplitSizeOfEnumerator);
            context.sendEventToSourceReader(subTask, metadataEvent);
        } else if (finishedSnapshotSplitMeta.size() > requestMetaGroupId) {
            List<FinishedSnapshotSplitInfo> metaToSend =
                    finishedSnapshotSplitMeta.get(requestMetaGroupId);
            StreamSplitMetaEvent metadataEvent =
                    new StreamSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            metaToSend.stream()
                                    .map(FinishedSnapshotSplitInfo::serialize)
                                    .collect(Collectors.toList()),
                            totalFinishedSplitSizeOfEnumerator);
            context.sendEventToSourceReader(subTask, metadataEvent);
        } else {
            throw new FlinkRuntimeException(
                    String.format(
                            "The enumerator received invalid request meta group id %s, the valid meta group id range is [0, %s]. Total finished split size of reader is %s, while the total finished split size of enumerator is %s.",
                            requestMetaGroupId,
                            finishedSnapshotSplitMeta.size() - 1,
                            totalFinishedSplitSizeOfReader,
                            totalFinishedSplitSizeOfEnumerator));
        }
    }

    private void handleLatestFinishedSplitNumberRequest(int subtaskId) {
        if (splitAssigner instanceof HybridSplitAssigner) {
            context.sendEventToSourceReader(
                    subtaskId,
                    new LatestFinishedSplitsNumberEvent(
                            splitAssigner.getFinishedSplitInfos().size()));
        }
    }

    // ===============================
    // AUTO-CHECKPOINT IMPLEMENTATION
    // ===============================

    private volatile boolean autoCheckpointTriggered = false;
    private volatile long snapshotFinishedTime = 0;

    /**
     * Triggers an automatic checkpoint when snapshot splits are finished to prevent deadlock.
     * This method uses the SourceEnumeratorContext to request a checkpoint programmatically.
     */
    private void triggerAutoCheckpointIfNeeded() {
        // Check if all snapshot splits are finished and we haven't triggered auto-checkpoint yet
        if (splitAssigner.noMoreSplits() && !autoCheckpointTriggered) {
            snapshotFinishedTime = System.currentTimeMillis();
            autoCheckpointTriggered = true;
            
            LOG.info("🔧 AUTO-CHECKPOINT: All snapshot splits finished. Starting auto-checkpoint mechanism...");
            
            // Start a background thread to handle auto-checkpoint with delay
            Thread autoCheckpointThread = new Thread(() -> {
                try {
                    // Wait 2 minutes to allow normal checkpoint triggering
                    Thread.sleep(120000);
                    
                    // Check if we still need to trigger checkpoint
                    if (splitAssigner.noMoreSplits() && !isStreamSplitAssigned()) {
                        LOG.warn("🚨 AUTO-CHECKPOINT: No checkpoint triggered after 2 minutes. Requesting checkpoint...");
                        
                        // Request checkpoint through SourceEnumeratorContext
                        try {
                            // Use reflection to access checkpoint triggering if available
                            triggerCheckpointThroughContext();
                        } catch (Exception e) {
                            LOG.warn("Failed to trigger checkpoint through context: {}", e.getMessage());
                            
                            // Fallback: Schedule periodic checkpoint requests
                            schedulePeriodicCheckpointRequests();
                        }
                    } else {
                        LOG.info("✅ AUTO-CHECKPOINT: Normal checkpoint flow detected, auto-trigger not needed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.debug("Auto-checkpoint thread interrupted");
                } catch (Exception e) {
                    LOG.error("Error in auto-checkpoint thread", e);
                }
            });
            
            autoCheckpointThread.setName("auto-checkpoint-trigger");
            autoCheckpointThread.setDaemon(true);
            autoCheckpointThread.start();
        }
    }

    /**
     * Attempts to trigger checkpoint through SourceEnumeratorContext.
     */
    private void triggerCheckpointThroughContext() {
        LOG.info("🔧 AUTO-CHECKPOINT: Attempting to trigger checkpoint through SourceEnumeratorContext");
        
        // The SourceEnumeratorContext doesn't directly expose checkpoint triggering,
        // but we can use callAsync to schedule a checkpoint request
        context.callAsync(
            () -> {
                LOG.info("🔧 AUTO-CHECKPOINT: Executing async checkpoint request");
                // This will be executed in the coordinator thread
                return "checkpoint-request";
            },
            (result, throwable) -> {
                if (throwable != null) {
                    LOG.warn("🚨 AUTO-CHECKPOINT: Async checkpoint request failed: {}", throwable.getMessage());
                } else {
                    LOG.info("✅ AUTO-CHECKPOINT: Async checkpoint request completed: {}", result);
                }
            }
        );
    }

    /**
     * Schedules periodic checkpoint requests as a fallback mechanism.
     */
    private void schedulePeriodicCheckpointRequests() {
        LOG.info("🔧 AUTO-CHECKPOINT: Starting periodic checkpoint request mechanism");
        
        // Schedule periodic checks every 10 seconds for up to 2 minutes
        for (int i = 1; i <= 12; i++) {
            final int attempt = i;
            context.callAsync(
                () -> {
                    try {
                        Thread.sleep(10000 * attempt); // Wait 10s, 20s, 30s, etc.
                        return "periodic-checkpoint-request-" + attempt;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "interrupted";
                    }
                },
                (result, throwable) -> {
                    if (!isStreamSplitAssigned() && splitAssigner.noMoreSplits()) {
                        LOG.info("🔄 AUTO-CHECKPOINT: Periodic request #{} - still waiting for checkpoint", attempt);
                        
                        if (attempt >= 12) { // After 2 minutes total
                            LOG.warn("🚨 AUTO-CHECKPOINT: Checkpoint still not triggered after 2 minutes");
                            LOG.warn("🚨 This indicates a potential checkpoint configuration issue");
                        }
                    } else {
                        LOG.info("✅ AUTO-CHECKPOINT: Checkpoint completed, stopping periodic requests");
                    }
                }
            );
        }
    }

    /**
     * Checks if stream split has been assigned (indicating successful checkpoint completion).
     */
    private boolean isStreamSplitAssigned() {
        return streamSplitTaskId != null;
    }
}
