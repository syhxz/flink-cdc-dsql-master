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
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DataSink} for Amazon DSQL connector with batch processing support. */
public class DsqlSink implements DataSink {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlSink.class);

    private final Configuration config;

    public DsqlSink(Configuration config) {
        this.config = config;
        LOG.info(
                "Initialized DSQL sink with host: {}, database: {}, batch size: {}, schema-change policy: {}",
                config.get(DsqlSinkOptions.HOST),
                config.get(DsqlSinkOptions.DATABASE),
                config.get(DsqlSinkOptions.BATCH_SIZE),
                config.get(DsqlSinkOptions.SCHEMA_CHANGE_POLICY));
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkFunctionProvider.of(new DsqlBatchSinkFunction(config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DsqlMetadataApplier(config);
    }
}
