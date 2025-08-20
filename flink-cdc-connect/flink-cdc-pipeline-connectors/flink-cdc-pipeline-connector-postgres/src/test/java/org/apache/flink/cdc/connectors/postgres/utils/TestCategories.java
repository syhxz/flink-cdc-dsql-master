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

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Test category annotations for organizing and filtering PostgreSQL connector tests.
 * These annotations allow for selective test execution based on environment capabilities
 * and test requirements.
 */
public class TestCategories {
    
    /**
     * Marks a test class or method as a unit test.
     * Unit tests should be fast, isolated, and not require external dependencies.
     * They typically use mocking and in-memory testing approaches.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("unit")
    public @interface UnitTest {
    }
    
    /**
     * Marks a test class or method as an integration test.
     * Integration tests verify the interaction between components and may
     * require external dependencies like databases or containers.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("integration")
    public @interface IntegrationTest {
    }
    
    /**
     * Marks a test class or method as requiring Docker.
     * Tests with this annotation will be skipped if Docker is not available
     * on the system. This is typically used with integration tests that
     * require Testcontainers.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("docker-required")
    public @interface DockerRequired {
    }
    
    /**
     * Marks a test class or method as a performance test.
     * Performance tests are typically longer-running and focus on
     * measuring throughput, latency, or resource usage.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("performance")
    public @interface PerformanceTest {
    }
    
    /**
     * Marks a test class or method as a slow test.
     * Slow tests may take longer to execute and might be excluded
     * from quick test runs during development.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("slow")
    public @interface SlowTest {
    }
    
    /**
     * Marks a test class or method as testing error scenarios.
     * These tests focus on error handling, edge cases, and
     * failure recovery mechanisms.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("error-scenarios")
    public @interface ErrorScenarios {
    }
    
    /**
     * Marks a test class or method as testing schema-related functionality.
     * These tests focus on schema discovery, data type mapping,
     * and schema evolution scenarios.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("schema")
    public @interface SchemaTest {
    }
    
    /**
     * Marks a test class or method as testing startup options.
     * These tests focus on different startup modes like snapshot,
     * latest-offset, timestamp, and specific-offset.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("startup-options")
    public @interface StartupOptionsTest {
    }
    
    /**
     * Marks a test class or method as testing configuration validation.
     * These tests focus on configuration parsing, validation,
     * and error reporting for invalid configurations.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("configuration")
    public @interface ConfigurationTest {
    }
    
    /**
     * Marks a test class or method as testing table name formatting.
     * These tests focus on table name parsing, schema handling,
     * and naming convention validation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("table-naming")
    public @interface TableNamingTest {
    }
    
    /**
     * Marks a test class or method as requiring a clean database state.
     * Tests with this annotation should start with a fresh database
     * and clean up after themselves to avoid affecting other tests.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("clean-database")
    public @interface CleanDatabase {
    }
    
    /**
     * Marks a test class or method as testing concurrent access scenarios.
     * These tests focus on thread safety, concurrent connections,
     * and parallel processing capabilities.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("concurrent")
    public @interface ConcurrentTest {
    }
    
    /**
     * Marks a test class or method as testing resource management.
     * These tests focus on proper resource allocation, cleanup,
     * and memory management.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("resource-management")
    public @interface ResourceManagementTest {
    }
    
    /**
     * Marks a test class or method as testing security-related functionality.
     * These tests focus on authentication, authorization, and
     * secure connection handling.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("security")
    public @interface SecurityTest {
    }
    
    /**
     * Marks a test class or method as a smoke test.
     * Smoke tests are basic tests that verify core functionality
     * and are typically run first to catch major issues quickly.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("smoke")
    public @interface SmokeTest {
    }
    
    /**
     * Marks a test class or method as a regression test.
     * Regression tests verify that previously fixed bugs
     * do not reoccur in new versions.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("regression")
    public @interface RegressionTest {
    }
}