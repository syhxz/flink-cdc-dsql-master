#!/bin/bash

# Simple build script for DSQL connector
# This script builds only the DSQL connector and its dependencies

echo "Building DSQL Connector..."

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
DSQL_CONNECTOR_DIR="$PROJECT_ROOT/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dsql"

echo "Project root: $PROJECT_ROOT"
echo "DSQL connector directory: $DSQL_CONNECTOR_DIR"

cd "$PROJECT_ROOT"

echo ""
echo "Step 1: Building core dependencies..."

# Build core modules first
mvn clean install -DskipTests -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true \
    -pl flink-cdc-common \
    -am

if [ $? -ne 0 ]; then
    echo "Failed to build flink-cdc-common"
    exit 1
fi

echo ""
echo "Step 2: Building runtime..."

mvn clean install -DskipTests -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true \
    -pl flink-cdc-runtime \
    -am

if [ $? -ne 0 ]; then
    echo "Failed to build flink-cdc-runtime"
    exit 1
fi

echo ""
echo "Step 3: Building DSQL connector..."

mvn clean package -DskipTests -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true \
    -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dsql

if [ $? -ne 0 ]; then
    echo "Failed to build DSQL connector"
    exit 1
fi

echo ""
echo "Build completed successfully!"

# Check if JAR was created
DSQL_JAR="$DSQL_CONNECTOR_DIR/target/flink-cdc-pipeline-connector-dsql-3.5-SNAPSHOT.jar"
if [ -f "$DSQL_JAR" ]; then
    echo "✓ DSQL connector JAR created: $DSQL_JAR"
    echo "JAR size: $(du -h "$DSQL_JAR" | cut -f1)"
else
    echo "✗ DSQL connector JAR not found"
    exit 1
fi

echo ""
echo "Next steps:"
echo "1. Set up Apache Flink (download from https://flink.apache.org/downloads/)"
echo "2. Set FLINK_HOME environment variable"
echo "3. Copy the JAR to Flink's lib directory:"
echo "   cp '$DSQL_JAR' \$FLINK_HOME/lib/"
echo "4. Use the example configuration files to test the connector"

echo ""
echo "Example configurations available:"
ls -la "$DSQL_CONNECTOR_DIR"/*.yaml 2>/dev/null || echo "No YAML configuration files found"