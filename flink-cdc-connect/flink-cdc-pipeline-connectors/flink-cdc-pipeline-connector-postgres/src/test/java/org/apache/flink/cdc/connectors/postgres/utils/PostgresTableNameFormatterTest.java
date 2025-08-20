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

import org.apache.flink.cdc.common.event.TableId;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link PostgresTableNameFormatter}. */
class PostgresTableNameFormatterTest {

    @Test
    void testFormatTableListWithSingleTable() {
        String[] result = PostgresTableNameFormatter.formatTableList("public", "users");
        assertEquals(1, result.length);
        assertEquals("public.users", result[0]);
    }

    @Test
    void testFormatTableListWithMultipleTables() {
        String[] result = PostgresTableNameFormatter.formatTableList("public", "users,orders,products");
        assertEquals(3, result.length);
        assertEquals("public.users", result[0]);
        assertEquals("public.orders", result[1]);
        assertEquals("public.products", result[2]);
    }

    @Test
    void testFormatTableListWithSchemaPrefix() {
        String[] result = PostgresTableNameFormatter.formatTableList("public", "schema1.table1,table2");
        assertEquals(2, result.length);
        assertEquals("schema1.table1", result[0]);
        assertEquals("public.table2", result[1]);
    }

    @Test
    void testFormatTableListWithWhitespace() {
        String[] result = PostgresTableNameFormatter.formatTableList("public", " users , orders , products ");
        assertEquals(3, result.length);
        assertEquals("public.users", result[0]);
        assertEquals("public.orders", result[1]);
        assertEquals("public.products", result[2]);
    }

    @Test
    void testFormatTableListWithEmptyTables() {
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.formatTableList("public", ""));
    }

    @Test
    void testFormatTableListWithNullTables() {
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.formatTableList("public", null));
    }

    @Test
    void testFormatTableListWithEmptySchema() {
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.formatTableList("", "users"));
    }

    @Test
    void testFormatTableListWithNullSchema() {
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.formatTableList(null, "users"));
    }

    @Test
    void testParseTableId() {
        TableId tableId = PostgresTableNameFormatter.parseTableId("public.users");
        assertEquals("public", tableId.getSchemaName());
        assertEquals("users", tableId.getTableName());
    }

    @Test
    void testParseTableIdWithInvalidFormat() {
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.parseTableId("users"));
    }

    @Test
    void testParseTableIdWithEmptyParts() {
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.parseTableId("public."));
        
        assertThrows(IllegalArgumentException.class, () -> 
            PostgresTableNameFormatter.parseTableId(".users"));
    }

    @Test
    void testParseTableIds() {
        String[] formattedTables = {"public.users", "schema1.orders", "public.products"};
        List<TableId> tableIds = PostgresTableNameFormatter.parseTableIds(formattedTables);
        
        assertEquals(3, tableIds.size());
        assertEquals(TableId.tableId("public", "users"), tableIds.get(0));
        assertEquals(TableId.tableId("schema1", "orders"), tableIds.get(1));
        assertEquals(TableId.tableId("public", "products"), tableIds.get(2));
    }

    @Test
    void testIsValidTableName() {
        assertTrue(PostgresTableNameFormatter.isValidTableName("users"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("user_orders"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("_private_table"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("table123"));
        assertTrue(PostgresTableNameFormatter.isValidTableName("table$special"));
        
        assertFalse(PostgresTableNameFormatter.isValidTableName("123table"));
        assertFalse(PostgresTableNameFormatter.isValidTableName("table-name"));
        assertFalse(PostgresTableNameFormatter.isValidTableName("table name"));
        assertFalse(PostgresTableNameFormatter.isValidTableName(""));
        assertFalse(PostgresTableNameFormatter.isValidTableName(null));
    }

    @Test
    void testIsValidSchemaName() {
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("public"));
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("my_schema"));
        assertTrue(PostgresTableNameFormatter.isValidSchemaName("_private"));
        
        assertFalse(PostgresTableNameFormatter.isValidSchemaName("123schema"));
        assertFalse(PostgresTableNameFormatter.isValidSchemaName("schema-name"));
        assertFalse(PostgresTableNameFormatter.isValidSchemaName(""));
        assertFalse(PostgresTableNameFormatter.isValidSchemaName(null));
    }

    @Test
    void testEscapeIdentifier() {
        assertEquals("users", PostgresTableNameFormatter.escapeIdentifier("users"));
        assertEquals("\"user-table\"", PostgresTableNameFormatter.escapeIdentifier("user-table"));
        assertEquals("\"user table\"", PostgresTableNameFormatter.escapeIdentifier("user table"));
        assertEquals("\"SELECT\"", PostgresTableNameFormatter.escapeIdentifier("SELECT"));
        assertEquals("\"table\"\"name\"", PostgresTableNameFormatter.escapeIdentifier("table\"name"));
        assertEquals("\"already quoted\"", PostgresTableNameFormatter.escapeIdentifier("\"already quoted\""));
    }
}

