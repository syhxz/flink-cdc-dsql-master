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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for managing test data setup and cleanup in PostgreSQL integration tests.
 * Provides methods for creating test schemas, inserting test data, and cleaning up after tests.
 */
public class TestDataManager {
    private static final Logger LOG = LoggerFactory.getLogger(TestDataManager.class);
    
    private final Connection connection;
    
    public TestDataManager(Connection connection) {
        this.connection = connection;
    }
    
    /**
     * Sets up the complete test schema with all required tables.
     * This method is idempotent and can be called multiple times safely.
     * 
     * @throws SQLException if schema setup fails
     */
    public void setupTestSchema() throws SQLException {
        LOG.info("Setting up test schema...");
        
        String schemaScript = 
            "-- Create test tables with proper constraints and indexes\n" +
            "CREATE TABLE IF NOT EXISTS users (\n" +
            "    id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100) NOT NULL,\n" +
            "    email VARCHAR(255) UNIQUE NOT NULL,\n" +
            "    age INTEGER CHECK (age >= 0 AND age <= 150),\n" +
            "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS orders (\n" +
            "    id SERIAL PRIMARY KEY,\n" +
            "    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,\n" +
            "    product_name VARCHAR(255) NOT NULL,\n" +
            "    quantity INTEGER NOT NULL CHECK (quantity > 0),\n" +
            "    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),\n" +
            "    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "    status VARCHAR(50) DEFAULT 'pending'\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS products (\n" +
            "    id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(255) NOT NULL,\n" +
            "    category VARCHAR(100),\n" +
            "    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),\n" +
            "    in_stock BOOLEAN DEFAULT true,\n" +
            "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS categories (\n" +
            "    id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100) UNIQUE NOT NULL,\n" +
            "    description TEXT,\n" +
            "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
            ");\n" +
            "\n" +
            "-- Create indexes for better query performance\n" +
            "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);\n" +
            "CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);\n" +
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);\n" +
            "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);";
        
        executeScript(schemaScript);
        LOG.info("Test schema setup completed successfully");
    }
    
    /**
     * Inserts comprehensive test data into all tables.
     * Uses ON CONFLICT clauses to make the operation idempotent.
     * 
     * @throws SQLException if data insertion fails
     */
    public void insertTestData() throws SQLException {
        LOG.info("Inserting test data...");
        
        String dataScript = 
            "-- Insert test categories\n" +
            "INSERT INTO categories (name, description) VALUES \n" +
            "    ('Electronics', 'Electronic devices and accessories'),\n" +
            "    ('Books', 'Physical and digital books'),\n" +
            "    ('Clothing', 'Apparel and accessories')\n" +
            "ON CONFLICT (name) DO NOTHING;\n" +
            "\n" +
            "-- Insert test users\n" +
            "INSERT INTO users (name, email, age) VALUES \n" +
            "    ('John Doe', 'john@example.com', 30),\n" +
            "    ('Jane Smith', 'jane@example.com', 25),\n" +
            "    ('Bob Johnson', 'bob@example.com', 35),\n" +
            "    ('Alice Brown', 'alice@example.com', 28),\n" +
            "    ('Charlie Wilson', 'charlie@example.com', 42)\n" +
            "ON CONFLICT (email) DO NOTHING;\n" +
            "\n" +
            "-- Insert test products\n" +
            "INSERT INTO products (name, category, price, in_stock) VALUES \n" +
            "    ('Laptop Pro', 'Electronics', 1299.99, true),\n" +
            "    ('Wireless Mouse', 'Electronics', 39.99, true),\n" +
            "    ('Mechanical Keyboard', 'Electronics', 129.99, true),\n" +
            "    ('Programming Book', 'Books', 49.99, true),\n" +
            "    ('T-Shirt', 'Clothing', 19.99, true),\n" +
            "    ('Smartphone', 'Electronics', 699.99, false),\n" +
            "    ('Headphones', 'Electronics', 199.99, true)\n" +
            "ON CONFLICT DO NOTHING;\n" +
            "\n" +
            "-- Insert test orders\n" +
            "INSERT INTO orders (user_id, product_name, quantity, price, status) VALUES \n" +
            "    (1, 'Laptop Pro', 1, 1299.99, 'completed'),\n" +
            "    (2, 'Wireless Mouse', 2, 39.99, 'pending'),\n" +
            "    (1, 'Mechanical Keyboard', 1, 129.99, 'shipped'),\n" +
            "    (3, 'Programming Book', 3, 49.99, 'completed'),\n" +
            "    (4, 'T-Shirt', 2, 19.99, 'pending'),\n" +
            "    (2, 'Headphones', 1, 199.99, 'completed'),\n" +
            "    (5, 'Smartphone', 1, 699.99, 'cancelled')\n" +
            "ON CONFLICT DO NOTHING;";
        
        executeScript(dataScript);
        LOG.info("Test data insertion completed successfully");
    }
    
    /**
     * Inserts minimal test data for faster test execution.
     * Use this method when full test data is not required.
     * 
     * @throws SQLException if data insertion fails
     */
    public void insertMinimalTestData() throws SQLException {
        LOG.info("Inserting minimal test data...");
        
        String minimalDataScript = 
            "-- Insert minimal test data for faster tests\n" +
            "INSERT INTO users (name, email) VALUES \n" +
            "    ('Test User', 'test@example.com')\n" +
            "ON CONFLICT (email) DO NOTHING;\n" +
            "\n" +
            "INSERT INTO products (name, category, price) VALUES \n" +
            "    ('Test Product', 'Electronics', 99.99)\n" +
            "ON CONFLICT DO NOTHING;\n" +
            "\n" +
            "INSERT INTO orders (user_id, product_name, quantity, price) VALUES \n" +
            "    (1, 'Test Product', 1, 99.99)\n" +
            "ON CONFLICT DO NOTHING;";
        
        executeScript(minimalDataScript);
        LOG.info("Minimal test data insertion completed successfully");
    }
    
    /**
     * Cleans up all test data from all tables.
     * Uses CASCADE to handle foreign key constraints properly.
     * 
     * @throws SQLException if cleanup fails
     */
    public void cleanupTestData() throws SQLException {
        LOG.info("Cleaning up test data...");
        
        String cleanupScript = 
            "-- Clean up test data in proper order to handle foreign key constraints\n" +
            "TRUNCATE TABLE orders CASCADE;\n" +
            "TRUNCATE TABLE products CASCADE;\n" +
            "TRUNCATE TABLE users CASCADE;\n" +
            "TRUNCATE TABLE categories CASCADE;";
        
        executeScript(cleanupScript);
        LOG.info("Test data cleanup completed successfully");
    }
    
    /**
     * Drops all test tables and related objects.
     * Use this method for complete cleanup when tables are no longer needed.
     * 
     * @throws SQLException if table dropping fails
     */
    public void dropTestTables() throws SQLException {
        LOG.info("Dropping test tables...");
        
        String dropScript = 
            "-- Drop tables in reverse dependency order\n" +
            "DROP TABLE IF EXISTS orders CASCADE;\n" +
            "DROP TABLE IF EXISTS products CASCADE;\n" +
            "DROP TABLE IF EXISTS users CASCADE;\n" +
            "DROP TABLE IF EXISTS categories CASCADE;";
        
        executeScript(dropScript);
        LOG.info("Test tables dropped successfully");
    }
    
    /**
     * Creates a specific table for testing edge cases.
     * 
     * @param tableName the name of the table to create
     * @param columns the column definitions
     * @throws SQLException if table creation fails
     */
    public void createCustomTable(String tableName, String columns) throws SQLException {
        LOG.info("Creating custom table: {}", tableName);
        
        String createTableScript = String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s);", 
            tableName, 
            columns
        );
        
        executeScript(createTableScript);
        LOG.info("Custom table {} created successfully", tableName);
    }
    
    /**
     * Inserts custom data into a specific table.
     * 
     * @param tableName the target table name
     * @param columns the column names
     * @param values the values to insert
     * @throws SQLException if data insertion fails
     */
    public void insertCustomData(String tableName, String columns, String values) throws SQLException {
        LOG.info("Inserting custom data into table: {}", tableName);
        
        String insertScript = String.format(
            "INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING;",
            tableName,
            columns,
            values
        );
        
        executeScript(insertScript);
        LOG.info("Custom data inserted into table {} successfully", tableName);
    }
    
    /**
     * Executes a SQL script by splitting it into individual statements.
     * Handles comments and empty statements gracefully.
     * 
     * @param script the SQL script to execute
     * @throws SQLException if script execution fails
     */
    private void executeScript(String script) throws SQLException {
        List<String> statements = Arrays.asList(script.split(";"));
        
        try (Statement stmt = connection.createStatement()) {
            for (String statement : statements) {
                String trimmed = statement.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("--")) {
                    LOG.debug("Executing SQL: {}", trimmed);
                    stmt.execute(trimmed);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to execute SQL script", e);
            throw e;
        }
    }
    
    /**
     * Checks if a table exists in the database.
     * 
     * @param tableName the table name to check
     * @return true if the table exists, false otherwise
     * @throws SQLException if the check fails
     */
    public boolean tableExists(String tableName) throws SQLException {
        String checkScript = 
            "SELECT EXISTS (\n" +
            "    SELECT FROM information_schema.tables \n" +
            "    WHERE table_schema = 'public' \n" +
            "    AND table_name = ?\n" +
            ");";
        
        try (java.sql.PreparedStatement stmt = connection.prepareStatement(checkScript)) {
            stmt.setString(1, tableName);
            try (java.sql.ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }
    
    /**
     * Gets the count of records in a specific table.
     * 
     * @param tableName the table name
     * @return the number of records in the table
     * @throws SQLException if the count query fails
     */
    public long getRecordCount(String tableName) throws SQLException {
        String countScript = String.format("SELECT COUNT(*) FROM %s", tableName);
        
        try (Statement stmt = connection.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(countScript)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
}