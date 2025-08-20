#!/bin/bash

# Test script for DSQL connector
# This script tests the DSQL connector components without requiring a full Flink CDC build

echo "Testing DSQL Connector Components..."

# Set up classpath for testing
CONNECTOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$CONNECTOR_DIR/src/main/java"

echo "Connector directory: $CONNECTOR_DIR"
echo "Source directory: $SRC_DIR"

# Check if Java source files exist
echo ""
echo "Checking DSQL connector source files..."

check_file() {
    local file="$1"
    if [ -f "$file" ]; then
        echo "✓ Found: $(basename "$file")"
    else
        echo "✗ Missing: $(basename "$file")"
    fi
}

# Check main components
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/sink/DsqlSinkProvider.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/sink/DsqlSink.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/sink/DsqlSinkOptions.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/factory/DsqlSinkProviderFactory.java"

# Check utility components
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/auth/DsqlAuthenticator.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/utils/DsqlRetryHelper.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/utils/DsqlSchemaMapper.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/utils/DsqlFullLoadCoordinator.java"
check_file "$SRC_DIR/org/apache/flink/cdc/connectors/dsql/utils/DsqlErrorReporter.java"

# Check configuration files
check_file "$CONNECTOR_DIR/pom.xml"
check_file "$CONNECTOR_DIR/src/main/resources/META-INF/services/org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator"

echo ""
echo "Checking documentation..."
check_file "$CONNECTOR_DIR/README.md"
check_file "$CONNECTOR_DIR/../../../../../docs/content/docs/connectors/pipeline-connectors/dsql.md"

echo ""
echo "DSQL Connector Component Check Complete!"

# Create a simple configuration example
echo ""
echo "Creating example configuration..."

cat > "$CONNECTOR_DIR/example-postgresql-to-dsql.yaml" << 'EOF'
################################################################################
# Description: Sync tables from PostgreSQL to Amazon DSQL
################################################################################

source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: postgres
  database-name: postgres
  schema-name: public
  table-name: orders,products,customers

sink:
  type: dsql
  host: my-dsql-cluster.amazonaws.com
  port: 5432
  database: my_database
  use-iam-auth: true
  region: us-west-2
  max-pool-size: 10
  min-pool-size: 2
  connection-max-lifetime-ms: 3540000  # 59 minutes
  connection-idle-timeout-ms: 300000   # 5 minutes
  enable-full-load: true
  parallelism: 4

pipeline:
  name: PostgreSQL to DSQL Pipeline
  parallelism: 2
EOF

echo "✓ Created example configuration: example-postgresql-to-dsql.yaml"

echo ""
echo "To test the DSQL connector:"
echo "1. Set up Apache Flink (download from https://flink.apache.org/downloads/)"
echo "2. Set FLINK_HOME environment variable"
echo "3. Build this connector: mvn clean package -DskipTests"
echo "4. Copy the JAR to Flink's lib directory"
echo "5. Run: flink-cdc.sh example-postgresql-to-dsql.yaml"

echo ""
echo "For troubleshooting build issues:"
echo "- Try: mvn clean package -DskipTests -Drat.skip=true -Dspotless.check.skip=true"
echo "- Or build only this module: mvn clean package -DskipTests -pl flink-cdc-pipeline-connector-dsql"