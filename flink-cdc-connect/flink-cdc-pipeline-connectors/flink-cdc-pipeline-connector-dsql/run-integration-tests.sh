#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to run DSQL connector integration tests

set -e

echo "Starting DSQL Connector Integration Tests"
echo "========================================="

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed or not in PATH"
    exit 1
fi

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo "Error: Docker daemon is not running"
    exit 1
fi

# Set test properties
export MAVEN_OPTS="-Xmx2g -XX:MaxMetaspaceSize=512m"

# Run specific test or all tests
if [ $# -eq 0 ]; then
    echo "Running all DSQL integration tests..."
    mvn test -Dtest=DsqlE2eITCase -DfailIfNoTests=false
else
    echo "Running specific test: $1"
    mvn test -Dtest=DsqlE2eITCase#$1 -DfailIfNoTests=false
fi

echo ""
echo "Integration tests completed!"
echo ""
echo "Available test methods:"
echo "  testMysqlToMockDsqlFullLoad"
echo "  testPostgresToMockDsqlFullLoad"
echo "  testParallelFullLoad"
echo "  testMysqlToMockDsqlCdc"
echo "  testPostgresToMockDsqlCdc"
echo "  testCdcWithSchemaEvolution"
echo "  testConnectionFailureRecovery"
echo "  testAuthenticationFailure"
echo "  testDataTypeConversionErrors"
echo "  testNetworkInterruption"
echo "  testConnectionPoolExhaustion"
echo ""
echo "Usage examples:"
echo "  ./run-integration-tests.sh                           # Run all tests"
echo "  ./run-integration-tests.sh testMysqlToMockDsqlFullLoad  # Run specific test"