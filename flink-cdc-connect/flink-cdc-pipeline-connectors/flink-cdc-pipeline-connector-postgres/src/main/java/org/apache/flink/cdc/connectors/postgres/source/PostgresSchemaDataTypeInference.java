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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.event.SchemaDataTypeInference;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

/** Schema data type inference for PostgreSQL. */
@Internal
public class PostgresSchemaDataTypeInference implements SchemaDataTypeInference, Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaDataTypeInference.class);

    @Override
    public DataType infer(Object value, Schema schema) {
        return infer(schema);
    }

    public DataType infer(Schema schema) {
        // Handle logical types first
        if (schema.name() != null) {
            switch (schema.name()) {
                case Decimal.LOGICAL_NAME:
                    int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
                    int precision = Integer.parseInt(schema.parameters().get("connect.decimal.precision"));
                    return DataTypes.DECIMAL(precision, scale);
                case Date.SCHEMA_NAME:
                    return DataTypes.DATE();
                case Time.SCHEMA_NAME:
                case MicroTime.SCHEMA_NAME:
                case NanoTime.SCHEMA_NAME:
                    return DataTypes.TIME();
                case Timestamp.SCHEMA_NAME:
                case MicroTimestamp.SCHEMA_NAME:
                case NanoTimestamp.SCHEMA_NAME:
                    return DataTypes.TIMESTAMP();
                case ZonedTimestamp.SCHEMA_NAME:
                    return DataTypes.TIMESTAMP_LTZ();
                case Geometry.LOGICAL_NAME:
                case Point.LOGICAL_NAME:
                    return DataTypes.STRING();
                case "io.debezium.data.Json":
                    return DataTypes.STRING();
                case "io.debezium.data.Xml":
                    return DataTypes.STRING();
                case "io.debezium.data.Uuid":
                    return DataTypes.STRING();
                case "io.debezium.data.Ltree":
                    return DataTypes.STRING();
                case "io.debezium.data.Enum":
                    return DataTypes.STRING();
                case "io.debezium.data.EnumSet":
                    return DataTypes.STRING();
                default:
                    // Fall through to handle by schema type
                    break;
            }
        }

        // Handle by schema type
        switch (schema.type()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case INT8:
                return DataTypes.TINYINT();
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case STRING:
                return DataTypes.STRING();
            case BYTES:
                return DataTypes.BYTES();
            case ARRAY:
                DataType elementType = infer(schema.valueSchema());
                return DataTypes.ARRAY(elementType);
            case MAP:
                DataType keyType = infer(schema.keySchema());
                DataType valueType = infer(schema.valueSchema());
                return DataTypes.MAP(keyType, valueType);
            case STRUCT:
                // Build proper row type from all fields in the struct
                List<DataField> fields = new ArrayList<>();
                for (org.apache.kafka.connect.data.Field field : schema.fields()) {
                    DataType fieldType = infer(field.schema());
                    fields.add(DataTypes.FIELD(field.name(), fieldType));
                }
                return DataTypes.ROW(fields.toArray(new DataField[0]));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported schema type: " + schema.type());
        }
    }
}