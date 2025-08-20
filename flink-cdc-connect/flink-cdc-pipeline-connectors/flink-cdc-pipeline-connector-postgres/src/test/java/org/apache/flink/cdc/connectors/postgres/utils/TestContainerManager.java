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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.DockerClientFactory;
import org.junit.jupiter.api.Assumptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

/**
 * Robust container management system for PostgreSQL integration tests.
 * Handles Docker availability detection, container lifecycle management,
 * and provides retry logic with exponential backoff.
 */
public class TestContainerManager {
    private static final Logger LOG = LoggerFactory.getLogger(TestContainerManager.class);
    private static final int MAX_STARTUP_ATTEMPTS = 3;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private static final int HEALTH_CHECK_MAX_ATTEMPTS = 30;
    private static final long HEALTH_CHECK_INTERVAL_MS = 1000;
    
    private PostgreSQLContainer<?> postgresContainer;
    private boolean dockerAvailable;
    
    public TestContainerManager() {
        this.dockerAvailable = checkDockerAvailability();
    }
    
    /**
     * Checks if Docker is available on the system.
     * 
     * @return true if Docker is available, false otherwise
     */
    public boolean isDockerAvailable() {
        return dockerAvailable;
    }
    
    /**
     * Gets or creates a PostgreSQL container instance.
     * If Docker is not available, skips the test using JUnit assumptions.
     * 
     * @return PostgreSQL container instance
     * @throws RuntimeException if container startup fails after all retry attempts
     */
    public PostgreSQLContainer<?> getOrCreateContainer() {
        Assumptions.assumeTrue(dockerAvailable, "Docker is not available - skipping integration tests");
        
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            postgresContainer = createContainer();
            startContainerWithRetry();
        }
        
        return postgresContainer;
    }
    
    /**
     * Checks Docker availability by attempting to create a Docker client.
     * 
     * @return true if Docker is available, false otherwise
     */
    private boolean checkDockerAvailability() {
        try {
            DockerClientFactory.instance().client();
            LOG.info("Docker is available for integration tests");
            return true;
        } catch (Exception e) {
            LOG.warn("Docker is not available: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Creates a new PostgreSQL container with optimized configuration.
     * 
     * @return configured PostgreSQL container
     */
    private PostgreSQLContainer<?> createContainer() {
        return new PostgreSQLContainer<>("postgres:13")
                .withDatabaseName("testdb")
                .withUsername("testuser")
                .withPassword("testpass")
                .withCommand("postgres", "-c", "wal_level=logical") // Enable logical replication
                .withStartupTimeout(STARTUP_TIMEOUT)
                .withReuse(true); // Enable container reuse for faster tests
    }
    
    /**
     * Starts the container with retry logic and exponential backoff.
     * 
     * @throws RuntimeException if all startup attempts fail
     */
    private void startContainerWithRetry() {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= MAX_STARTUP_ATTEMPTS; attempt++) {
            try {
                LOG.info("Starting PostgreSQL container (attempt {}/{})", attempt, MAX_STARTUP_ATTEMPTS);
                postgresContainer.start();
                
                // Verify container is actually ready
                waitForContainerReady();
                LOG.info("PostgreSQL container started successfully");
                return;
                
            } catch (Exception e) {
                lastException = e;
                LOG.warn("Container startup attempt {} failed: {}", attempt, e.getMessage());
                
                if (attempt < MAX_STARTUP_ATTEMPTS) {
                    try {
                        long backoffMs = 1000L * attempt; // Exponential backoff
                        LOG.info("Waiting {}ms before retry...", backoffMs);
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during container startup retry", ie);
                    }
                }
            }
        }
        
        throw new RuntimeException("Failed to start PostgreSQL container after " + 
                                 MAX_STARTUP_ATTEMPTS + " attempts", lastException);
    }
    
    /**
     * Performs additional health checks to ensure the container is ready for use.
     * This goes beyond Testcontainers' built-in readiness checks.
     * 
     * @throws Exception if health check fails
     */
    private void waitForContainerReady() throws Exception {
        String jdbcUrl = postgresContainer.getJdbcUrl();
        Properties props = new Properties();
        props.setProperty("user", postgresContainer.getUsername());
        props.setProperty("password", postgresContainer.getPassword());
        
        for (int i = 0; i < HEALTH_CHECK_MAX_ATTEMPTS; i++) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
                // Try a simple query to ensure database is ready
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1")) {
                    if (rs.next()) {
                        LOG.debug("Container health check passed");
                        return; // Container is ready
                    }
                }
            } catch (SQLException e) {
                if (i == HEALTH_CHECK_MAX_ATTEMPTS - 1) {
                    throw new RuntimeException("Container failed health check after " + 
                                             HEALTH_CHECK_MAX_ATTEMPTS + " attempts", e);
                }
                LOG.debug("Health check attempt {} failed, retrying...", i + 1);
                Thread.sleep(HEALTH_CHECK_INTERVAL_MS);
            }
        }
    }
    
    /**
     * Performs cleanup of container resources.
     * This method is safe to call multiple times and handles exceptions gracefully.
     */
    public void cleanup() {
        if (postgresContainer != null && postgresContainer.isRunning()) {
            try {
                LOG.info("Stopping PostgreSQL container...");
                postgresContainer.stop();
                LOG.info("PostgreSQL container stopped successfully");
            } catch (Exception e) {
                LOG.warn("Error stopping container: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Gets the JDBC URL for the running container.
     * 
     * @return JDBC URL
     * @throws IllegalStateException if container is not running
     */
    public String getJdbcUrl() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("Container is not running");
        }
        return postgresContainer.getJdbcUrl();
    }
    
    /**
     * Gets the mapped port for the PostgreSQL service.
     * 
     * @return mapped port number
     * @throws IllegalStateException if container is not running
     */
    public Integer getMappedPort() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("Container is not running");
        }
        return postgresContainer.getMappedPort(5432);
    }
    
    /**
     * Gets the container host.
     * 
     * @return container host
     * @throws IllegalStateException if container is not running
     */
    public String getHost() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("Container is not running");
        }
        return postgresContainer.getHost();
    }
    
    /**
     * Gets the database name.
     * 
     * @return database name
     */
    public String getDatabaseName() {
        if (postgresContainer == null) {
            throw new IllegalStateException("Container is not initialized");
        }
        return postgresContainer.getDatabaseName();
    }
    
    /**
     * Gets the username.
     * 
     * @return username
     */
    public String getUsername() {
        if (postgresContainer == null) {
            throw new IllegalStateException("Container is not initialized");
        }
        return postgresContainer.getUsername();
    }
    
    /**
     * Gets the password.
     * 
     * @return password
     */
    public String getPassword() {
        if (postgresContainer == null) {
            throw new IllegalStateException("Container is not initialized");
        }
        return postgresContainer.getPassword();
    }
}