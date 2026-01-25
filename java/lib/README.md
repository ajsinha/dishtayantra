# DishtaYantra Java Libraries

This directory contains the compiled Java gateway and required dependencies.

## Quick Setup

Run the build script to compile Java code and download dependencies:

```bash
cd java
chmod +x build.sh
./build.sh
```

## Manual Setup

If the build script fails, you can manually:

1. **Download Py4J JAR** (required):
   ```bash
   curl -L -o lib/py4j0.10.9.7.jar \
       https://repo1.maven.org/maven2/net/sf/py4j/py4j/0.10.9.7/py4j-0.10.9.7.jar
   ```

2. **Compile Java sources**:
   ```bash
   mkdir -p build lib
   
   javac -d build \
       -cp "lib/*" \
       -source 11 -target 11 \
       src/com/dishtayantra/calculators/*.java \
       src/com/dishtayantra/calculators/examples/*.java \
       src/com/dishtayantra/gateway/*.java
   
   cd build
   jar cf ../lib/dishtayantra-gateway.jar com/
   ```

## Contents After Build

After running the build script, this directory should contain:

- `py4j0.10.9.7.jar` - Py4J library for Python-Java bridge
- `dishtayantra-gateway.jar` - Compiled DishtaYantra gateway and calculators
- `dishtayantra-gateway-executable.jar` - Standalone executable JAR

## Running Gateway Manually

```bash
java -cp "lib/*" com.dishtayantra.gateway.DishtaYantraGateway --port 25333 --pool-size 4
```

## Troubleshooting

1. **"ClassNotFoundException: py4j.GatewayServer"**
   - Ensure py4j.jar is in the lib directory

2. **"NoClassDefFoundError: com/dishtayantra/gateway/DishtaYantraGateway"**
   - Run the build script to compile Java sources

3. **"Connection refused" in Python**
   - Check if Java gateway is running: `ps aux | grep DishtaYantraGateway`
   - Check port availability: `netstat -ln | grep 25333`

4. **Java version issues**
   - Requires JDK 11 or higher
   - Check version: `java -version`
