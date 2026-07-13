#!/bin/bash

# PostgreSQL to PostgreSQL CDC Pipeline Runner
# Uses DSQL connector for PostgreSQL-compatible sink

set -e

# Configuration
CONFIG_FILE="${1:-postgres-to-postgres.yaml}"
FLINK_CDC_HOME="/root/flink-cdc-master"
FLINK_HOME="${FLINK_HOME:-$FLINK_CDC_HOME/flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/target/flink-1.20.1}"

echo "Starting PostgreSQL to PostgreSQL CDC sync..."
echo "Config file: $CONFIG_FILE"
echo "Flink Home: $FLINK_HOME"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file '$CONFIG_FILE' not found!"
    echo "Usage: $0 [config-file.yaml]"
    exit 1
fi

# Required JAR files for PostgreSQL to PostgreSQL CDC
JARS=(
    "$FLINK_HOME/lib/flink-cdc-dist-3.5-SNAPSHOT.jar"
    "$FLINK_HOME/lib/flink-cdc-runtime-3.5-SNAPSHOT.jar"
    "$FLINK_HOME/lib/flink-cdc-pipeline-connector-postgres-3.5-SNAPSHOT.jar"
    "$FLINK_HOME/lib/flink-cdc-pipeline-connector-dsql-3.5-SNAPSHOT.jar"
)

echo "Checking required JAR files..."
for jar in "${JARS[@]}"; do
    if [ -f "$jar" ]; then
        echo "✓ Found: $(basename "$jar")"
    else
        echo "✗ Missing: $(basename "$jar")"
        echo "Please build the project first with: mvn clean package -DskipTests"
        exit 1
    fi
done

# Set up classpath
CLASSPATH=""
for jar in "${JARS[@]}"; do
    CLASSPATH="$CLASSPATH:$jar"
done

# CLI JAR (for running the pipeline)
CLI_JAR="$FLINK_HOME/lib/flink-cdc-dist-3.5-SNAPSHOT.jar"

echo "Running PostgreSQL to PostgreSQL CDC job..."

# Run the Flink CDC job
$FLINK_HOME/bin/flink run \
  --class org.apache.flink.cdc.cli.CliFrontend \
  "$CLI_JAR" \
  "$CONFIG_FILE"

if [ $? -eq 0 ]; then
    echo "PostgreSQL to PostgreSQL CDC job submitted successfully!"
else
    echo "Failed to submit PostgreSQL to PostgreSQL CDC job!"
    exit 1
fi
