#!/bin/bash

# PostgreSQL to DSQL CDC Sync Script
# Usage: ./run-postgres-to-dsql.sh [config-file]

set -e

# Configuration
FLINK_CDC_HOME="/root/flink-cdc-master"
FLINK_HOME="$FLINK_CDC_HOME/flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/target/flink-1.20.1"
CLI_JAR="$FLINK_CDC_HOME/flink-cdc-cli/target/flink-cdc-cli-3.5-SNAPSHOT.jar"

# Default config file
CONFIG_FILE="${1:-postgres-to-dsql.yaml}"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file '$CONFIG_FILE' not found!"
    echo "Usage: $0 [config-file]"
    exit 1
fi

echo "Starting PostgreSQL to DSQL CDC sync..."
echo "Config file: $CONFIG_FILE"
echo "Flink Home: $FLINK_HOME"


echo "Running Flink CDC job..."

# Run the Flink job
$FLINK_HOME/bin/flink run -D flink.cdc.checkpoint.timeout=60000 \
    --class org.apache.flink.cdc.cli.CliFrontend \
    $CLASSPATH_ARGS \
    "$CLI_JAR" \
    --flink-home "$FLINK_HOME" \
    "$CONFIG_FILE"

echo "Job submitted successfully!"
