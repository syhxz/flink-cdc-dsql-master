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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Utility class for setting up PostgreSQL event triggers to capture DDL changes. Uses the pattern
 * from Alibaba DTS: DDL operations are captured by an event trigger and written to a
 * {@code flink_cdc_ddl_command} table. The CDC source then monitors this table and converts
 * INSERT events into SchemaChangeEvents.
 *
 * <p>This approach works because PostgreSQL's logical replication (used by Debezium/CDC) does not
 * natively propagate DDL statements. The event trigger bridges this gap by converting DDL into DML
 * (INSERT into the ddl command table), which is then captured by the standard CDC pipeline.
 */
public class PostgresDdlCaptureInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDdlCaptureInitializer.class);

    /** The table name used to store captured DDL commands. */
    public static final String DDL_COMMAND_TABLE = "flink_cdc_ddl_command";

    /** The schema where DDL capture objects are created. */
    public static final String DDL_CAPTURE_SCHEMA = "public";

    /** Full qualified table name for use in CDC table list. */
    public static final String DDL_COMMAND_TABLE_FULL = DDL_CAPTURE_SCHEMA + "." + DDL_COMMAND_TABLE;

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS public.flink_cdc_ddl_command (\n"
                    + "    id bigserial PRIMARY KEY,\n"
                    + "    event text,\n"
                    + "    tag text,\n"
                    + "    username varchar(128),\n"
                    + "    database_name varchar(128),\n"
                    + "    schema_name varchar(128),\n"
                    + "    object_type varchar(64),\n"
                    + "    object_name varchar(256),\n"
                    + "    ddl_text text,\n"
                    + "    event_time timestamptz DEFAULT current_timestamp,\n"
                    + "    txid bigint\n"
                    + ")";

    private static final String CREATE_FUNCTION_SQL =
            "CREATE OR REPLACE FUNCTION public.flink_cdc_capture_ddl()\n"
                    + "    RETURNS event_trigger\n"
                    + "    LANGUAGE plpgsql\n"
                    + "    SECURITY DEFINER\n"
                    + "AS $func$\n"
                    + "DECLARE\n"
                    + "    ddl_text text;\n"
                    + "    record_object record;\n"
                    + "    object_id text;\n"
                    + "    max_rows int := 10000;\n"
                    + "    current_rows int;\n"
                    + "BEGIN\n"
                    + "    SELECT current_query() INTO ddl_text;\n"
                    + "\n"
                    + "    -- For CREATE TABLE, also set REPLICA IDENTITY FULL and add to publication\n"
                    + "    IF TG_TAG = 'CREATE TABLE' THEN\n"
                    + "        FOR record_object IN (SELECT * FROM pg_event_trigger_ddl_commands()) LOOP\n"
                    + "            IF record_object.command_tag = 'CREATE TABLE' THEN\n"
                    + "                object_id := record_object.object_identity;\n"
                    + "                -- Set REPLICA IDENTITY FULL for new tables\n"
                    + "                EXECUTE 'ALTER TABLE ' || object_id || ' REPLICA IDENTITY FULL';\n"
                    + "            END IF;\n"
                    + "        END LOOP;\n"
                    + "    END IF;\n"
                    + "\n"
                    + "    -- Insert DDL record\n"
                    + "    INSERT INTO public.flink_cdc_ddl_command(\n"
                    + "        event, tag, username, database_name, schema_name,\n"
                    + "        object_type, object_name, ddl_text, txid\n"
                    + "    ) VALUES (\n"
                    + "        TG_EVENT, TG_TAG, current_user, current_database(),\n"
                    + "        current_schema, '', '', ddl_text, txid_current()\n"
                    + "    );\n"
                    + "\n"
                    + "    -- Prevent table from growing unbounded\n"
                    + "    SELECT count(*) INTO current_rows FROM public.flink_cdc_ddl_command;\n"
                    + "    IF current_rows > max_rows THEN\n"
                    + "        DELETE FROM public.flink_cdc_ddl_command\n"
                    + "        WHERE id <= (SELECT min(id) + 1000 FROM public.flink_cdc_ddl_command);\n"
                    + "    END IF;\n"
                    + "END;\n"
                    + "$func$";

    private static final String CREATE_TRIGGER_SQL =
            "CREATE EVENT TRIGGER flink_cdc_intercept_ddl ON ddl_command_end\n"
                    + "EXECUTE FUNCTION public.flink_cdc_capture_ddl()";

    private static final String SET_REPLICA_IDENTITY_SQL =
            "ALTER TABLE public.flink_cdc_ddl_command REPLICA IDENTITY FULL";

    /**
     * Initializes DDL capture infrastructure on the source PostgreSQL database. Creates the DDL
     * command table, capture function, and event trigger if they don't already exist.
     *
     * <p>This method is idempotent — safe to call multiple times.
     *
     * @param hostname PostgreSQL host
     * @param port PostgreSQL port
     * @param database Database name
     * @param username Username with event trigger creation privileges
     * @param password Password
     */
    public static void initialize(
            String hostname, int port, String database, String username, String password) {

        LOG.info(
                "Initializing DDL capture on source database {}:{}/{}",
                hostname,
                port,
                database);

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        if (password != null) {
            props.setProperty("password", password);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            // 1. Create DDL command table
            LOG.info("Creating DDL command table: {}", DDL_COMMAND_TABLE);
            executeIgnoreError(connection, CREATE_TABLE_SQL, "already exists");

            // 2. Set REPLICA IDENTITY FULL so CDC captures all columns
            LOG.info("Setting REPLICA IDENTITY FULL on {}", DDL_COMMAND_TABLE);
            executeIgnoreError(connection, SET_REPLICA_IDENTITY_SQL, null);

            // 3. Create capture function
            LOG.info("Creating DDL capture function");
            executeIgnoreError(connection, CREATE_FUNCTION_SQL, null);

            // 4. Create event trigger (only if not exists)
            if (!eventTriggerExists(connection, "flink_cdc_intercept_ddl")) {
                LOG.info("Creating event trigger: flink_cdc_intercept_ddl");
                executeIgnoreError(connection, CREATE_TRIGGER_SQL, "already exists");
            } else {
                LOG.info("Event trigger flink_cdc_intercept_ddl already exists, skipping");
            }

            // 5. Add DDL command table to existing publications (for pgoutput)
            addTableToPublications(connection, DDL_COMMAND_TABLE_FULL);

            LOG.info("DDL capture initialization completed successfully");

        } catch (SQLException e) {
            LOG.error("Failed to initialize DDL capture: {}", e.getMessage(), e);
            throw new RuntimeException(
                    "Failed to initialize DDL capture infrastructure: " + e.getMessage(), e);
        }
    }

    private static void executeIgnoreError(Connection conn, String sql, String ignoreContains)
            throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            if (ignoreContains != null && e.getMessage() != null
                    && e.getMessage().contains(ignoreContains)) {
                LOG.debug("Ignoring expected error: {}", e.getMessage());
            } else {
                throw e;
            }
        }
    }

    private static boolean eventTriggerExists(Connection conn, String triggerName)
            throws SQLException {
        String sql =
                "SELECT 1 FROM pg_event_trigger WHERE evtname = '" + triggerName + "'";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next();
        }
    }

    private static void addTableToPublications(Connection conn, String tableName) {
        // Find publications used by CDC and add our DDL table
        String findPubsSql =
                "SELECT pubname FROM pg_publication WHERE pubname LIKE 'dbz_%' OR pubname LIKE 'flink_%'";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(findPubsSql)) {
            while (rs.next()) {
                String pubName = rs.getString(1);
                String addSql = "ALTER PUBLICATION " + pubName + " ADD TABLE " + tableName;
                try (Statement addStmt = conn.createStatement()) {
                    addStmt.execute(addSql);
                    LOG.info("Added {} to publication {}", tableName, pubName);
                } catch (SQLException e) {
                    if (e.getMessage() != null && e.getMessage().contains("already member")) {
                        LOG.debug("Table {} already in publication {}", tableName, pubName);
                    } else {
                        LOG.warn(
                                "Failed to add {} to publication {}: {}",
                                tableName,
                                pubName,
                                e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to query publications: {}", e.getMessage());
        }
    }

    /**
     * Removes DDL capture infrastructure from the source database.
     *
     * @param hostname PostgreSQL host
     * @param port PostgreSQL port
     * @param database Database name
     * @param username Username
     * @param password Password
     */
    public static void cleanup(
            String hostname, int port, String database, String username, String password) {

        LOG.info("Cleaning up DDL capture on {}:{}/{}", hostname, port, database);

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        if (password != null) {
            props.setProperty("password", password);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            executeIgnoreError(
                    connection, "DROP EVENT TRIGGER IF EXISTS flink_cdc_intercept_ddl", null);
            executeIgnoreError(
                    connection, "DROP FUNCTION IF EXISTS public.flink_cdc_capture_ddl()", null);
            executeIgnoreError(
                    connection, "DROP TABLE IF EXISTS public.flink_cdc_ddl_command", null);
            LOG.info("DDL capture cleanup completed");
        } catch (SQLException e) {
            LOG.warn("Failed to cleanup DDL capture: {}", e.getMessage());
        }
    }
}
