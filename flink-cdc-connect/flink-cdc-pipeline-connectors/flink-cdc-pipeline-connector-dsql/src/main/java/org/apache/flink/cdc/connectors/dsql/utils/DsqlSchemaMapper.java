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

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for mapping schemas between source databases and Amazon DSQL.
 * DSQL is PostgreSQL-compatible, so we map to PostgreSQL data types.
 */
public class DsqlSchemaMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlSchemaMapper.class);

    /**
     * Generates a CREATE TABLE statement for DSQL based on the source schema.
     *
     * @param schema The source schema
     * @param tableName The target table name in DSQL
     * @return SQL CREATE TABLE statement
     */
    public static String generateCreateTableStatement(Schema schema, String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            sql.append("  ").append(column.getName()).append(" ");
            sql.append(mapDataTypeToDsql(column.getType()));

            if (i < columns.size() - 1) {
                sql.append(",");
            }
            sql.append("\n");
        }

        // Add primary key constraint if exists
        List<String> primaryKeys = schema.primaryKeys();
        if (!primaryKeys.isEmpty()) {
            sql.append(",  PRIMARY KEY (");
            sql.append(String.join(", ", primaryKeys));
            sql.append(")\n");
        }

        sql.append(")");

        LOG.debug("Generated CREATE TABLE statement: {}", sql.toString());
        return sql.toString();
    }

    /**
     * Generates an INSERT statement for DSQL.
     *
     * @param schema The source schema
     * @param tableName The target table name
     * @return SQL INSERT statement with placeholders
     */
    public static String generateInsertStatement(Schema schema, String tableName) {
        List<String> columnNames = schema.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");
        sql.append(String.join(", ", columnNames));
        sql.append(") VALUES (");

        // Add placeholders
        for (int i = 0; i < columnNames.size(); i++) {
            sql.append("?");
            if (i < columnNames.size() - 1) {
                sql.append(", ");
            }
        }
        sql.append(")");

        return sql.toString();
    }

    /**
     * Generates an UPDATE statement for DSQL.
     *
     * @param schema The source schema
     * @param tableName The target table name
     * @return SQL UPDATE statement with placeholders
     */
    public static String generateUpdateStatement(Schema schema, String tableName) {
        List<Column> columns = schema.getColumns();
        List<String> primaryKeys = schema.primaryKeys();

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");

        // Set clauses for non-primary key columns
        List<String> setClauses = columns.stream()
                .filter(col -> !primaryKeys.contains(col.getName()))
                .map(col -> col.getName() + " = ?")
                .collect(Collectors.toList());

        sql.append(String.join(", ", setClauses));
        sql.append(" WHERE ");

        // WHERE clauses for primary keys
        List<String> whereClauses = primaryKeys.stream()
                .map(key -> key + " = ?")
                .collect(Collectors.toList());

        sql.append(String.join(" AND ", whereClauses));

        return sql.toString();
    }

    /**
     * Generates a DELETE statement for DSQL.
     *
     * @param schema The source schema
     * @param tableName The target table name
     * @return SQL DELETE statement with placeholders
     */
    public static String generateDeleteStatement(Schema schema, String tableName) {
        List<String> primaryKeys = schema.primaryKeys();

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");

        List<String> whereClauses = primaryKeys.stream()
                .map(key -> key + " = ?")
                .collect(Collectors.toList());

        sql.append(String.join(" AND ", whereClauses));

        return sql.toString();
    }

    /**
     * Maps Flink CDC data types to DSQL (PostgreSQL) data types.
     *
     * @param dataType The source data type
     * @return The corresponding DSQL data type string
     */
    private static String mapDataTypeToDsql(DataType dataType) {
        if (dataType instanceof TinyIntType) {
            return "SMALLINT";
        } else if (dataType instanceof SmallIntType) {
            return "SMALLINT";
        } else if (dataType instanceof IntType) {
            return "INTEGER";
        } else if (dataType instanceof BigIntType) {
            return "BIGINT";
        } else if (dataType instanceof FloatType) {
            return "REAL";
        } else if (dataType instanceof DoubleType) {
            return "DOUBLE PRECISION";
        } else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return String.format("DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
        } else if (dataType instanceof BooleanType) {
            return "BOOLEAN";
        } else if (dataType instanceof DateType) {
            return "DATE";
        } else if (dataType instanceof TimeType) {
            return "TIME";
        } else if (dataType instanceof TimestampType) {
            return "TIMESTAMP";
        } else if (dataType instanceof LocalZonedTimestampType) {
            return "TIMESTAMPTZ";
        } else if (dataType instanceof CharType) {
            CharType charType = (CharType) dataType;
            return String.format("CHAR(%d)", charType.getLength());
        } else if (dataType instanceof VarCharType) {
            VarCharType varCharType = (VarCharType) dataType;
            if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                return "TEXT";
            } else {
                return String.format("VARCHAR(%d)", varCharType.getLength());
            }
        } else if (dataType instanceof BinaryType) {
            BinaryType binaryType = (BinaryType) dataType;
            return String.format("BYTEA");
        } else {
            // Default to TEXT for unknown types
            LOG.warn("Unknown data type: {}, mapping to TEXT", dataType.getClass().getSimpleName());
            return "TEXT";
        }
    }
}
