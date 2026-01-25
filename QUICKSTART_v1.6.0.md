# DishtaYantra v1.6.0 - Quick Start Guide

## 🚀 Getting Started

### 1. Extract the Archive

```bash
# For zip
unzip dishtayantra_v1_6_0.zip

cd dishtayantra_enhanced
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. (Optional) Build Java Gateway

If you plan to use Java calculators:

```bash
cd java
chmod +x build.sh
./build.sh
cd ..
```

**Requirements**: JDK 11 or higher

### 4. Run the Server

```bash
python run_server.py
```

Access the web interface at: **http://localhost:5000**

Default credentials:
- Username: `admin`
- Password: `admin`

---

## 🆕 New in v1.6.0: JVM Manager & Py4J Integration

### What is the JVM Manager?

The JVM Manager provides **centralized management** of Java Virtual Machine instances for high-performance Java calculator integration:

- **Config-Based JVM Management**: Configure JVMs via `config/jvm_config.json`
- **Automatic JVM Lifecycle**: Start, stop, restart JVMs from the UI or API
- **Connection Pooling**: Multiple Py4J gateway connections for parallelism
- **Health Monitoring**: Auto-restart failed gateways
- **Config Hot-Reload**: Automatically restart JVMs when config changes

### Build Java Gateway (Required for Java Calculators)

```bash
cd java
./build.sh
```

This will:
1. Download Py4J library (py4j-0.10.9.7.jar)
2. Compile Java source code
3. Create dishtayantra-gateway.jar

### Configure JVM Settings

Edit `config/jvm_config.json`:

```json
{
    "jvm_manager": {
        "enabled": true,
        "auto_start_on_load": true
    },
    "gateways": [
        {
            "name": "primary",
            "enabled": true,
            "base_port": 25333,
            "pool_size": 4,
            "jvm": {
                "auto_start": true,
                "heap_size_mb": 512,
                "max_heap_size_mb": 2048
            }
        }
    ]
}
```

### Pre-Configured Java Calculators

Available calculators (defined in jvm_config.json):

| Calculator | Description |
|------------|-------------|
| `PassthruCalculator` | Pass data through unchanged |
| `MathCalculator` | Arithmetic operations with multiplier |
| `TradePricingCalculator` | Trade pricing with commission and tax |
| `RiskCalculator` | VaR and Greeks-based risk analysis |
| `StringTransformCalculator` | String transformations |
| `AggregationCalculator` | Sum, average, min, max operations |
| `PortfolioCalculator` | Portfolio valuation and metrics |
| `DataValidationCalculator` | Data validation with rules |

### Use Java Calculator in DAG

```json
{
    "name": "price_trade",
    "type": "CalculatorNode",
    "calculator": {
        "type": "java",
        "name": "TradePricingCalculator"
    }
}
```

### JVM Management UI

Access **Admin → JVM Management** (`/jvm`) to:
- View gateway status (connected/disconnected)
- Monitor JVM processes and PIDs
- Start/Stop/Restart JVMs
- View registered Java calculators
- Reload configuration

### JVM Management API

```bash
# Get JVM status
curl http://localhost:5000/api/jvm/status

# List gateways
curl http://localhost:5000/api/jvm/gateways

# Stop a JVM
curl -X POST http://localhost:5000/api/jvm/gateways/primary/stop

# Restart a JVM
curl -X POST http://localhost:5000/api/jvm/gateways/primary/restart

# Reload configuration
curl -X POST http://localhost:5000/api/jvm/reload-config
```

---

## 🔧 Troubleshooting JVM Issues

### "Connection refused" Error

**Cause**: Java gateway not running or not ready

**Solutions**:
1. Build Java code first: `cd java && ./build.sh`
2. Check if Java is installed: `java -version`
3. Wait longer for JVM startup (configured in jvm_config.json)
4. Check port availability: `netstat -ln | grep 25333`

### "ClassNotFoundException" Error

**Cause**: Missing JAR files

**Solution**:
```bash
cd java
./build.sh
```

### JVM Starts but Crashes

**Check logs**:
```bash
# Server logs
cat logs/dagserver.log | grep -i jvm
```

**Common causes**:
- Insufficient heap size (increase `max_heap_size_mb`)
- Port already in use (change `base_port`)

---

## 🆕 Also in v1.6.0

- **Config Monitoring**: JVMs auto-restart when jvm_config.json changes
- **Kill/Restart via UI**: Stop and restart JVMs from the web interface
- **Gateway Details Page**: Full gateway configuration and status view
- **Example DAGs**: Java calculator examples in `config/example/dags/`

---

## 📁 Key Files

| File | Description |
|------|-------------|
| `run_server.py` | Main entry point |
| `config/jvm_config.json` | JVM and Py4J configuration |
| `java/build.sh` | Java build script |
| `java/lib/` | Java libraries (after build) |
| `core/jvm/jvm_manager.py` | JVM Manager implementation |

---

## 📖 Documentation

- **Help Center**: http://localhost:5000/help
- **Py4J Integration Guide**: http://localhost:5000/help/py4j-integration
- **JVM Management**: http://localhost:5000/jvm

---

**Patent Pending - DishtaYantra Framework**
**Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.**
