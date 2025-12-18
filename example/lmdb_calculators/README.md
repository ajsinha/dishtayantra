# LMDB Zero-Copy Calculator Examples

**PATENT PENDING**: The LMDB Zero-Copy Data Exchange technology and Multi-Language Calculator Framework are subject to pending patent applications.

This directory contains example calculators demonstrating LMDB zero-copy data exchange across multiple programming languages.

## Overview

DishtaYantra v1.1.2 introduces **LMDB-based zero-copy data exchange** for native calculators. This patent-pending innovation enables 100-1000x faster data transfer for large payloads compared to traditional serialization.

## Performance

| Payload Size | JSON | MessagePack | LMDB Zero-Copy | Speedup |
|-------------|------|-------------|----------------|---------|
| 1 KB | 50 μs | 20 μs | **5 μs** | 10x |
| 100 KB | 5 ms | 2 ms | **50 μs** | 100x |
| 1 MB | 50 ms | 20 ms | **200 μs** | 250x |
| 10 MB | 500 ms | 200 ms | **2 ms** | 250x |

## Examples

### Python (`PythonLMDBCalculatorExample.py`) - STANDALONE

A complete, self-contained example that can be run directly:

```bash
# Install dependencies
pip install lmdb msgpack

# Run the example
python PythonLMDBCalculatorExample.py
```

Features demonstrated:
- `LMDBTransport` class for zero-copy data access
- `LMDBCalculator` base class with automatic LMDB routing
- `MatrixSumCalculator`, `RiskCalculator`, `SignalProcessor` examples
- Processing LMDB references from native calculators
- Performance comparison with/without LMDB

### Python (`python_lmdb_calculator.py`) - INTEGRATED

Direct usage of LMDB transport API and calculator wrapper within DishtaYantra:

```python
from example.lmdb_calculators.python_lmdb_calculator import (
    LargeMatrixProcessor,
    RiskCalculator,
    LMDBSimpleCalculator
)

# Create LMDB-enabled calculator
calc = LargeMatrixProcessor({
    'name': 'MatrixProcessor',
    'lmdb_enabled': True,
    'lmdb_min_size': 1024,  # Use LMDB for payloads > 1KB
})

# Process data (automatically uses LMDB for large payloads)
result = calc.calculate({'matrix': large_matrix, 'operation': 'sum'})
```

### Java (`JavaLMDBCalculator.java`)

LMDB-enabled Java calculators with lmdbjava:

```java
// Create calculator and initialize LMDB
MatrixProcessorCalculator calc = new MatrixProcessorCalculator();
calc.initLMDB("/tmp/dishtayantra_lmdb");

// Process LMDB reference from Python DAG Engine
Map<String, Object> lmdbRef = new HashMap<>();
lmdbRef.put("_lmdb_ref", true);
lmdbRef.put("_lmdb_input_key", "input:matrix:001");
lmdbRef.put("_lmdb_output_key", "output:matrix:001");
lmdbRef.put("_lmdb_db_path", "/tmp/dishtayantra_lmdb");

Map<String, Object> result = calc.calculate(lmdbRef);
```

**Dependencies (Maven):**
```xml
<dependency>
    <groupId>org.lmdbjava</groupId>
    <artifactId>lmdbjava</artifactId>
    <version>0.8.3</version>
</dependency>
<dependency>
    <groupId>org.msgpack</groupId>
    <artifactId>msgpack-core</artifactId>
    <version>0.9.6</version>
</dependency>
```

### C++ (`cpp_lmdb_calculator.cpp`)

High-performance C++ calculators with liblmdb and pybind11:

```cpp
// Create calculator
MatrixProcessorCalculator calc;
calc.init_lmdb("/tmp/dishtayantra_lmdb");

// Process data (with zero-copy LMDB access)
py::dict result = calc.calculate(data);
```

**Build:**
```bash
# Install LMDB
sudo apt-get install liblmdb-dev  # Ubuntu/Debian
brew install lmdb                  # macOS

# Compile
c++ -O3 -Wall -shared -std=c++17 -fPIC \
    $(python3 -m pybind11 --includes) \
    cpp_lmdb_calculator.cpp -o lmdb_calculator$(python3-config --extension-suffix) \
    -llmdb
```

### Rust (`rust_lmdb_calculator.rs`)

Memory-safe Rust calculators with lmdb-rs and PyO3:

```rust
// Create calculator
let mut calc = MatrixProcessorCalculator::new();
calc.init_lmdb("/tmp/dishtayantra_lmdb")?;

// Process data with zero-copy
let result = calc.calculate(py, data)?;
```

**Dependencies (Cargo.toml):**
```toml
[dependencies]
pyo3 = { version = "0.20", features = ["extension-module"] }
lmdb-rs = "0.8"
rmp-serde = "1.1"
serde = { version = "1.0", features = ["derive"] }
rayon = "1.8"
```

**Build:**
```bash
maturin develop --release
```

## How It Works

1. **Python DAG Engine** writes large payload to LMDB memory-mapped file
2. **LMDB Reference** dict is passed to native calculator:
   ```python
   {
       '_lmdb_ref': True,
       '_lmdb_input_key': 'input:calc:txn123',
       '_lmdb_output_key': 'output:calc:txn123',
       '_lmdb_db_path': '/tmp/dishtayantra_lmdb',
       '_lmdb_format': 'msgpack'
   }
   ```
3. **Native Calculator** reads data directly from memory map (zero-copy!)
4. **Output** written back to LMDB
5. **Python DAG Engine** reads result from LMDB

## Configuration

### application.properties
```properties
lmdb.db.path=${LMDB_DB_PATH:/tmp/dishtayantra_lmdb}
lmdb.map.size=1073741824
lmdb.ttl.seconds=300
```

### DAG Node Configuration
```json
{
    "name": "heavy_processor",
    "type": "com.example.HeavyProcessor",
    "calculator": "java",
    "lmdb_enabled": true,
    "lmdb_min_size": 10240,
    "lmdb_exchange_mode": "both",
    "lmdb_data_format": "msgpack"
}
```

## Calculator Classes

Each example includes:

| Calculator | Purpose |
|------------|---------|
| `MatrixProcessorCalculator` | Large matrix operations (sum, mean, transpose) |
| `RiskCalculator` | Financial risk metrics (VaR, exposure) |
| `SignalProcessorCalculator` | Signal statistics (C++/Rust with SIMD) |
| `PricingEngineCalculator` | Options pricing (Java) |

---

## Legal Information

### Patent Pending Technologies

The following technologies demonstrated in these examples are subject to pending patent applications:

**1. LMDB Zero-Copy Data Exchange System**
- Automatic payload size detection for LMDB routing decisions
- Unified reference protocol for heterogeneous language integration
- Transaction-based zero-copy data exchange
- Memory-mapped file transport with TTL-based cleanup

**2. Multi-Language Calculator Framework**
- Hot-swappable calculator integration architecture
- Unified interface across Python, Java, C++, and Rust
- Gateway pooling for JVM integration (Py4J)
- Native binding abstraction layer (pybind11/PyO3)

### Legal Notice

Unauthorized use, reproduction, or implementation of these technologies may constitute patent infringement.

### Copyright Notice

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.

### Trademark Notice

DishtaYantra™ is a trademark of Ashutosh Sinha.

---

**DishtaYantra v1.1.2** | Patent Pending | © 2025-2030 Ashutosh Sinha
