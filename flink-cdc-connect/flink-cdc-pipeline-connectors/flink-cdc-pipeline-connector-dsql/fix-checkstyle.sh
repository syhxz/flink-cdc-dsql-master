#!/bin/bash

# Script to fix common checkstyle issues in DSQL connector

echo "Fixing checkstyle issues in DSQL connector..."

CONNECTOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$CONNECTOR_DIR/src/main/java"

echo "Working directory: $CONNECTOR_DIR"

# Function to remove trailing whitespace from a file
remove_trailing_whitespace() {
    local file="$1"
    if [ -f "$file" ]; then
        echo "Removing trailing whitespace from: $(basename "$file")"
        # Use sed to remove trailing whitespace
        sed -i '' 's/[[:space:]]*$//' "$file"
    fi
}

# Function to ensure file ends with newline
ensure_newline() {
    local file="$1"
    if [ -f "$file" ]; then
        echo "Ensuring newline at end of: $(basename "$file")"
        # Add newline if file doesn't end with one
        if [ -n "$(tail -c1 "$file")" ]; then
            echo "" >> "$file"
        fi
    fi
}

# Find all Java files and fix them
find "$SRC_DIR" -name "*.java" | while read -r file; do
    echo "Processing: $file"
    remove_trailing_whitespace "$file"
    ensure_newline "$file"
done

echo ""
echo "Fixed common checkstyle issues!"
echo ""
echo "Remaining issues to fix manually:"
echo "1. Import order - organize imports properly"
echo "2. Missing Javadoc - add /** */ comments for public classes/enums"
echo "3. Unused imports - remove any unused import statements"
echo ""
echo "Try building again with:"
echo "mvn clean compile -DskipTests -Drat.skip=true -Dspotless.check.skip=true"