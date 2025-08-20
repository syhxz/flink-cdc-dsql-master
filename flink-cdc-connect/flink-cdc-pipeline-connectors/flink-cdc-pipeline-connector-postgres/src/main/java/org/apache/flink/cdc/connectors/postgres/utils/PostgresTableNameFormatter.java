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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Utility class for formatting PostgreSQL table names. */
@Internal
public class PostgresTableNameFormatter {

    /**
     * Format table list by adding schema prefix where needed.
     *
     * @param schemaName the default schema name
     * @param tables comma-separated table names
     * @return formatted table names array
     */
    public static String[] formatTableList(String schemaName, String tables) {
        if (StringUtils.isNullOrWhitespaceOnly(tables)) {
            throw new IllegalArgumentException("Tables configuration cannot be empty");
        }

        if (StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            throw new IllegalArgumentException("Schema name cannot be empty");
        }

        String[] tableArray = tables.split(",");
        String[] formattedTables = new String[tableArray.length];

        for (int i = 0; i < tableArray.length; i++) {
            String table = tableArray[i].trim();
            if (table.isEmpty()) {
                continue;
            }

            if (!table.contains(".")) {
                // Add schema prefix if not present
                formattedTables[i] = schemaName + "." + table;
            } else {
                // Use as-is if schema is already specified
                formattedTables[i] = table;
            }
        }

        return Arrays.stream(formattedTables)
                .filter(Objects::nonNull)
                .filter(s -> !s.trim().isEmpty())
                .toArray(String[]::new);
    }

    /**
     * Parse formatted table names into TableId objects.
     *
     * @param formattedTables array of formatted table names
     * @return list of TableId objects
     */
    public static List<TableId> parseTableIds(String[] formattedTables) {
        return Arrays.stream(formattedTables)
                .map(PostgresTableNameFormatter::parseTableId)
                .collect(Collectors.toList());
    }

    /**
     * Parse a single formatted table name into a TableId.
     *
     * @param fullTableName formatted table name (schema.table)
     * @return TableId object
     */
    public static TableId parseTableId(String fullTableName) {
        if (StringUtils.isNullOrWhitespaceOnly(fullTableName)) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        String[] parts = fullTableName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid table name format: " + fullTableName +
                            ". Expected format: schema.table");
        }

        String schema = parts[0].trim();
        String table = parts[1].trim();

        if (schema.isEmpty() || table.isEmpty()) {
            throw new IllegalArgumentException(
                    "Schema and table names cannot be empty in: " + fullTableName);
        }

        return TableId.tableId(schema, table);
    }

    /**
     * Validate that a table name follows PostgreSQL naming conventions.
     *
     * @param tableName the table name to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidTableName(String tableName) {
        if (StringUtils.isNullOrWhitespaceOnly(tableName)) {
            return false;
        }

        // PostgreSQL identifier rules: start with letter or underscore,
        // followed by letters, digits, underscores, or dollar signs
        return tableName.matches("^[a-zA-Z_][a-zA-Z0-9_$]*$");
    }

    /**
     * Validate that a schema name follows PostgreSQL naming conventions.
     *
     * @param schemaName the schema name to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidSchemaName(String schemaName) {
        return isValidTableName(schemaName); // Same rules apply
    }

    /**
     * Escape a PostgreSQL identifier if it contains special characters.
     *
     * @param identifier the identifier to escape
     * @return escaped identifier
     */
    public static String escapeIdentifier(String identifier) {
        if (StringUtils.isNullOrWhitespaceOnly(identifier)) {
            return identifier;
        }

        // If identifier is already quoted or doesn't need escaping, return as-is
        if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
            return identifier;
        }

        if (isValidTableName(identifier) && !isPostgresKeyword(identifier)) {
            return identifier;
        }

        // Escape with double quotes and escape any internal double quotes
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Check if a string is a PostgreSQL reserved keyword.
     *
     * @param word the word to check
     * @return true if it's a reserved keyword
     */
    private static boolean isPostgresKeyword(String word) {
        // Common PostgreSQL reserved keywords (not exhaustive)
        String[] keywords = {
                "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
                "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW",
                "DATABASE", "SCHEMA", "USER", "GROUP", "ORDER", "BY",
                "LIMIT", "OFFSET", "UNION", "JOIN", "INNER", "OUTER",
                "LEFT", "RIGHT", "FULL", "ON", "AS", "AND", "OR", "NOT",
                "NULL", "TRUE", "FALSE", "PRIMARY", "KEY", "FOREIGN",
                "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "CONSTRAINT"
        };

        return Arrays.stream(keywords)
                .anyMatch(keyword -> keyword.equalsIgnoreCase(word));
    }
}