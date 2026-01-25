#!/bin/bash
# DishtaYantra Java Gateway Build Script
# Version: 1.6.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src"
LIB_DIR="$SCRIPT_DIR/lib"
BUILD_DIR="$SCRIPT_DIR/build"

echo "========================================"
echo "DishtaYantra Java Gateway Build"
echo "========================================"

# Create directories
mkdir -p "$LIB_DIR"
mkdir -p "$BUILD_DIR"

# Check for py4j.jar
PY4J_JAR="$LIB_DIR/py4j0.10.9.7.jar"
if [ ! -f "$PY4J_JAR" ]; then
    echo ""
    echo "Downloading Py4J library..."
    curl -L -o "$PY4J_JAR" \
        "https://repo1.maven.org/maven2/net/sf/py4j/py4j/0.10.9.7/py4j-0.10.9.7.jar" \
        2>/dev/null || wget -O "$PY4J_JAR" \
        "https://repo1.maven.org/maven2/net/sf/py4j/py4j/0.10.9.7/py4j-0.10.9.7.jar"
    
    if [ ! -f "$PY4J_JAR" ]; then
        echo "ERROR: Failed to download py4j.jar"
        echo "Please download manually from:"
        echo "  https://repo1.maven.org/maven2/net/sf/py4j/py4j/0.10.9.7/py4j-0.10.9.7.jar"
        echo "And place it in: $LIB_DIR/"
        exit 1
    fi
    echo "✓ Py4J library downloaded"
fi

# Check for Java compiler
if ! command -v javac &> /dev/null; then
    echo "ERROR: javac not found. Please install JDK 11 or higher."
    echo "  Ubuntu/Debian: sudo apt install openjdk-11-jdk"
    echo "  macOS: brew install openjdk@11"
    echo "  Windows: Download from https://adoptium.net/"
    exit 1
fi

JAVA_VERSION=$(javac -version 2>&1 | head -1)
echo "Using: $JAVA_VERSION"

# Find all Java source files
echo ""
echo "Compiling Java sources..."
JAVA_FILES=$(find "$SRC_DIR" -name "*.java")

if [ -z "$JAVA_FILES" ]; then
    echo "ERROR: No Java source files found in $SRC_DIR"
    exit 1
fi

# Compile
javac -d "$BUILD_DIR" \
    -cp "$LIB_DIR/*" \
    --release 11 \
    $JAVA_FILES

echo "✓ Compilation successful"

# Create JAR
echo ""
echo "Creating JAR file..."
JAR_FILE="$LIB_DIR/dishtayantra-gateway.jar"

cd "$BUILD_DIR"
jar cf "$JAR_FILE" com/

echo "✓ Created: $JAR_FILE"

# Create manifest for executable JAR (optional)
MANIFEST_FILE="$BUILD_DIR/MANIFEST.MF"
cat > "$MANIFEST_FILE" << EOF
Manifest-Version: 1.0
Main-Class: com.dishtayantra.gateway.DishtaYantraGateway
Class-Path: py4j0.10.9.7.jar
EOF

jar cfm "$LIB_DIR/dishtayantra-gateway-executable.jar" "$MANIFEST_FILE" com/

echo ""
echo "========================================"
echo "Build Complete!"
echo "========================================"
echo ""
echo "JAR files created in: $LIB_DIR/"
echo "  - dishtayantra-gateway.jar"
echo "  - dishtayantra-gateway-executable.jar"
echo ""
echo "To run the gateway manually:"
echo "  java -cp '$LIB_DIR/*' com.dishtayantra.gateway.DishtaYantraGateway --port 25333 --pool-size 4"
echo ""
echo "Or using the executable JAR:"
echo "  java -jar $LIB_DIR/dishtayantra-gateway-executable.jar --port 25333 --pool-size 4"
echo ""
