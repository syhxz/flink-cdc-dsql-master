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

package org.apache.flink.cdc.connectors.postgres;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Integration test suite for PostgreSQL pipeline connector.
 * 
 * This suite runs all integration tests for the PostgreSQL connector to ensure
 * comprehensive testing of all functionality including:
 * - Basic connector functionality
 * - Schema discovery and table metadata
 * - Table name formatting and parsing
 * - Startup mode configurations
 * - Error scenarios and edge cases
 * 
 * To run all integration tests:
 * mvn test -Dtest=PostgresConnectorIntegrationTestSuite
 * 
 * To run individual test classes:
 * mvn test -Dtest=PostgresConnectorIntegrationTest
 * mvn test -Dtest=PostgresSchemaDiscoveryIntegrationTest
 * mvn test -Dtest=PostgresTableNameFormatterIntegrationTest
 * mvn test -Dtest=PostgresStartupOptionsIntegrationTest
 * mvn test -Dtest=PostgresErrorScenariosIntegrationTest
 */
@Suite
@SelectClasses({
    PostgresConnectorIntegrationTest.class,
    PostgresSchemaDiscoveryIntegrationTest.class,
    PostgresTableNameFormatterIntegrationTest.class,
    PostgresStartupOptionsIntegrationTest.class,
    PostgresErrorScenariosIntegrationTest.class
})
public class PostgresConnectorIntegrationTestSuite {
    // This class serves as a test suite runner
    // All test logic is in the individual test classes
}