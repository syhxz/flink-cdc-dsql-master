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

package org.apache.flink.cdc.connectors.postgres.source.assigner;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresScanFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * PostgreSQL-specific HybridSplitAssigner that uses snapshot begin LSN for CDC streaming start.
 * This ensures that CDC streaming starts from the exact point where the snapshot began,
 * providing better consistency and avoiding potential data gaps.
 */
public class PostgresHybridSplitAssigner extends HybridSplitAssigner<PostgresSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresHybridSplitAssigner.class);
    private static final String STREAM_SPLIT_ID = "stream-split";

    public PostgresHybridSplitAssigner(
            PostgresSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<PostgresSourceConfig> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        super(sourceConfig, currentParallelism, remainingTables, isTableIdCaseSensitive, dialect, offsetFactory, enumeratorContext);
    }

    public PostgresHybridSplitAssigner(
            PostgresSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            DataSourceDialect<PostgresSourceConfig> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        super(sourceConfig, currentParallelism, checkpoint, dialect, offsetFactory, enumeratorContext);
    }

    @Override
    public StreamSplit createStreamSplit() {
        final List<SchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, Offset> splitFinishedOffsets = snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        Offset minOffset = null, maxOffset = null;
        for (SchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min and max offset of change log
            Offset changeLogOffset = splitFinishedOffsets.get(split.splitId());
            if (minOffset == null || changeLogOffset.isBefore(minOffset)) {
                minOffset = changeLogOffset;
            }
            if (maxOffset == null || changeLogOffset.isAfter(maxOffset)) {
                maxOffset = changeLogOffset;
            }

            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            changeLogOffset,
                            offsetFactory));
        }

        // If the source is running in snapshot mode, we use the highest watermark among
        // snapshot splits as the ending offset to provide a consistent snapshot view at the moment
        // of high watermark.
        Offset stoppingOffset = offsetFactory.createNoStoppingOffset();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxOffset;
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        
        // Use snapshot begin offset for CDC streaming start
        Offset streamStartOffset = getSnapshotBeginOffset(minOffset);
        
        return new StreamSplit(
                STREAM_SPLIT_ID,
                streamStartOffset,
                stoppingOffset,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size(),
                false,
                true);
    }

    /**
     * Get the snapshot begin offset for CDC streaming start.
     * This method tries to get the actual snapshot begin LSN captured during snapshot execution.
     */
    private Offset getSnapshotBeginOffset(Offset fallbackOffset) {
        try {
            // Get the snapshot begin offset from PostgreSQL fetch task
            PostgresOffset postgresSnapshotBeginOffset = 
                PostgresScanFetchTask.PostgresSnapshotSplitReadTask.getGlobalSnapshotBeginOffset();
            
            if (postgresSnapshotBeginOffset != null) {
                // Convert PostgresOffset to generic Offset by creating a new PostgresOffset
                // that extends Offset and can be used directly
                Offset snapshotBeginOffset = postgresSnapshotBeginOffset;
                
                LOG.info("=== CDC STREAMING START FROM SNAPSHOT BEGIN LSN ===");
                LOG.info("Snapshot Begin LSN: {}", postgresSnapshotBeginOffset.getLsn());
                LOG.info("CDC streaming will start from snapshot begin instead of high watermark");
                LOG.info("This ensures no data loss and proper CDC continuity");
                
                return snapshotBeginOffset;
            } else {
                LOG.warn("Snapshot begin LSN not available, falling back to minimum finished offset");
                return fallbackOffset == null ? offsetFactory.createInitialOffset() : fallbackOffset;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get snapshot begin LSN, falling back to minimum finished offset: {}", e.getMessage());
            return fallbackOffset == null ? offsetFactory.createInitialOffset() : fallbackOffset;
        }
    }
}
