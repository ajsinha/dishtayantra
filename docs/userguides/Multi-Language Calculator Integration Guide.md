# Multi-Language Calculator Integration Guide

## Version 1.1.0+

© 2025-2030 Ashutosh Sinha

**PATENT PENDING**: The Multi-Language Calculator Framework is the subject of pending patent applications.

---

## Overview

DishtaYantra v1.2.0 introduces support for calculators written in multiple programming languages, enabling you to leverage the strengths of each language for different use cases.

| Language | Technology | Performance | Best For |
|----------|------------|-------------|----------|
| Python | Built-in | Baseline (~10K ops/s) | Rapid development, prototyping |
| Java | Py4J | 10-100x faster | JVM ecosystem, enterprise libraries |
| C++ | pybind11 | 50-100x faster (~1M ops/s) | Ultra-low latency, SIMD, numeric |
| Rust | PyO3 | 50-100x faster (~1M ops/s) | Memory safety, thread safety |
| REST | HTTP/JSON | Network dependent | Microservices, third-party APIs |

---

## Quick Start

### Calculator Configuration Pattern

All calculator configurations follow a similar pattern in your DAG JSON:

```json
{
  "calculators": [
    {
      "name": "my_calculator",
      "type": "CalculatorClassName",
      "config": {
        "calculator": "java|cpp|rust|rest",
        ...additional_config...
      }
    }
  ]
}
```

---

## Java Calculator (Py4J)

### Prerequisites

```bash
# Install Py4J
pip install py4j

# Java 8+ required
java -version
```

### Setup

1. **Compile Java Calculator**

```java
// src/com/company/MyCalculator.java
package com.company;

import com.dishtayantra.calculators.AbstractCalculator;
import java.util.Map;
import java.util.HashMap;

public class MyCalculator extends AbstractCalculator {
    @Override
    public Map<String, Object> calculate(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>(data);
        // Your calculation logic here
        double value = ((Number) data.get("input")).doubleValue();
        result.put("output", value * 2);
        return result;
    }
}
```

2. **Start Java Gateway**

```bash
# Compile
javac -cp .:py4j.jar src/com/company/*.java

# Start gateway
java -cp .:py4j.jar:src com.dishtayantra.gateway.DishtaYantraGateway
```

3. **Configure DAG**

```json
{
  "calculators": [
    {
      "name": "java_calc",
      "type": "com.company.MyCalculator",
      "config": {
        "calculator": "java",
        "gateway_port": 25333,
        "pool_size": 4
      }
    }
  ]
}
```

### Advanced Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `gateway_host` | localhost | Java gateway host |
| `gateway_port` | 25333 | Java gateway port |
| `pool_size` | 4 | Connection pool size |
| `timeout` | 30 | Call timeout (seconds) |

### Performance Tips

- Use connection pooling for high throughput
- Keep data structures simple (primitives, lists, maps)
- Consider batch processing for small operations

---

## C++ Calculator (pybind11)

### Prerequisites

```bash
# Install pybind11
pip install pybind11

# CMake 3.12+
cmake --version

# C++ compiler with C++17 support
g++ --version  # or clang++
```

### Setup

1. **Write C++ Calculator**

```cpp
// cpp/src/my_calculator.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <map>
#include <string>
#include <any>

namespace py = pybind11;

class MyCalculator {
public:
    py::dict calculate(py::dict data) {
        py::dict result(data);
        double value = data["input"].cast<double>();
        result["output"] = value * 2;
        return result;
    }
    
    py::dict details() {
        py::dict info;
        info["name"] = "MyCalculator";
        info["language"] = "C++";
        return info;
    }
};

PYBIND11_MODULE(my_calculator, m) {
    py::class_<MyCalculator>(m, "MyCalculator")
        .def(py::init<>())
        .def("calculate", &MyCalculator::calculate)
        .def("details", &MyCalculator::details);
}
```

2. **Build Module**

```bash
cd cpp
mkdir build && cd build
cmake ..
make
cp my_calculator*.so ../../
```

3. **Configure DAG**

```json
{
  "calculators": [
    {
      "name": "cpp_calc",
      "type": "MyCalculator",
      "config": {
        "calculator": "cpp",
        "module": "my_calculator"
      }
    }
  ]
}
```

### SIMD Optimization Example

```cpp
#include <immintrin.h>  // AVX2

class SimdCalculator {
public:
    py::dict calculate(py::dict data) {
        py::list values = data["values"].cast<py::list>();
        std::vector<double> vec;
        for (auto& v : values) vec.push_back(v.cast<double>());
        
        // SIMD processing (AVX2)
        size_t n = vec.size();
        size_t simd_n = n - (n % 4);
        
        for (size_t i = 0; i < simd_n; i += 4) {
            __m256d v = _mm256_loadu_pd(&vec[i]);
            __m256d two = _mm256_set1_pd(2.0);
            v = _mm256_mul_pd(v, two);
            _mm256_storeu_pd(&vec[i], v);
        }
        
        // Handle remainder
        for (size_t i = simd_n; i < n; i++) {
            vec[i] *= 2.0;
        }
        
        py::dict result(data);
        result["values"] = vec;
        return result;
    }
};
```

### Performance Tips

- Use zero-copy with NumPy arrays via `py::array`
- Enable compiler optimizations (-O3, -march=native)
- Batch small operations to amortize call overhead

---

## Rust Calculator (PyO3)

### Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install maturin
pip install maturin
```

### Setup

1. **Create Rust Project**

```bash
mkdir rust_calc && cd rust_calc
maturin init --bindings pyo3
```

2. **Write Rust Calculator**

```rust
// src/lib.rs
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

#[pyclass]
struct MyCalculator {
    name: String,
}

#[pymethods]
impl MyCalculator {
    #[new]
    fn new(name: String) -> Self {
        MyCalculator { name }
    }
    
    fn calculate(&self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let result = PyDict::new(py);
        
        // Copy input data
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }
        
        // Perform calculation
        if let Ok(value) = data.get_item("input")?.extract::<f64>() {
            result.set_item("output", value * 2.0)?;
        }
        
        Ok(result.into())
    }
    
    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("language", "Rust")?;
        Ok(dict.into())
    }
}

#[pymodule]
fn rust_calc(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<MyCalculator>()?;
    Ok(())
}
```

3. **Build Module**

```bash
maturin develop --release
```

4. **Configure DAG**

```json
{
  "calculators": [
    {
      "name": "rust_calc",
      "type": "MyCalculator",
      "config": {
        "calculator": "rust",
        "module": "rust_calc"
      }
    }
  ]
}
```

### Parallel Processing with Rayon

```rust
use rayon::prelude::*;

#[pymethods]
impl ParallelCalculator {
    fn calculate(&self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let values: Vec<f64> = data.get_item("values")?.extract()?;
        
        // Parallel processing
        let results: Vec<f64> = values
            .par_iter()
            .map(|&x| x * 2.0)
            .collect();
        
        let result = PyDict::new(py);
        result.set_item("values", results)?;
        Ok(result.into())
    }
}
```

### Performance Tips

- Use Rayon for CPU-bound parallel processing
- Minimize Python ↔ Rust data conversions
- Consider buffer protocol for large arrays

---

## REST Calculator

### Setup

No compilation required - uses HTTP endpoints.

```json
{
  "calculators": [
    {
      "name": "rest_calc",
      "config": {
        "calculator": "rest",
        "endpoint": "https://api.example.com/calculate",
        "auth_type": "api_key",
        "api_key": "${API_KEY}",
        "timeout": 10,
        "retries": 3
      }
    }
  ]
}
```

### Authentication Types

#### API Key
```json
{
  "auth_type": "api_key",
  "api_key": "${API_KEY}",
  "api_key_header": "X-API-Key"
}
```

#### Basic Auth
```json
{
  "auth_type": "basic",
  "username": "${USERNAME}",
  "password": "${PASSWORD}"
}
```

#### Bearer Token (OAuth2)
```json
{
  "auth_type": "bearer",
  "bearer_token": "${JWT_TOKEN}"
}
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `endpoint` | (required) | Full URL |
| `auth_type` | none | api_key, basic, bearer |
| `timeout` | 30 | Request timeout (seconds) |
| `retries` | 3 | Retry attempts |
| `retry_backoff` | 0.5 | Backoff multiplier |
| `verify_ssl` | true | SSL verification |
| `response_path` | null | JSONPath to extract result |
| `fallback_value` | null | Value on error |

### Example Server Implementation (Python/Flask)

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/calculate', methods=['POST'])
def calculate():
    api_key = request.headers.get('X-API-Key')
    if api_key != 'your-secret-key':
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.json
    result = data.copy()
    result['output'] = data.get('input', 0) * 2
    return jsonify(result)
```

---

## Choosing the Right Calculator Type

### Decision Matrix

| Requirement | Recommended |
|-------------|-------------|
| Rapid prototyping | Python |
| JVM library access | Java |
| Ultra-low latency | C++ |
| Memory safety critical | Rust |
| External API integration | REST |
| Numeric/SIMD processing | C++ or Rust |
| Thread-safe parallel | Rust |
| Enterprise ecosystem | Java |

### Performance Comparison

```
Latency (per call):
┌─────────────────────────────────────────────────────────┐
│ Python    │████████████████████████████████│ ~100μs    │
│ Java/Py4J │██████████████████░░░░░░░░░░░░░░│ ~200μs    │
│ C++       │█░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│ ~100ns    │
│ Rust      │█░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│ ~100ns    │
│ REST      │████████████████████████████████████████│ 10-1000ms │
└─────────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### Java Calculator Issues

| Issue | Solution |
|-------|----------|
| Gateway not found | Start Java gateway first |
| Class not found | Check classpath and compilation |
| Timeout | Increase timeout config |
| Pool exhausted | Increase pool_size |

### C++ Calculator Issues

| Issue | Solution |
|-------|----------|
| Module not found | Check .so file location |
| Symbol errors | Rebuild with correct Python version |
| Segmentation fault | Debug with gdb, check memory |

### Rust Calculator Issues

| Issue | Solution |
|-------|----------|
| Import error | Run `maturin develop` |
| Type mismatch | Check PyO3 type conversions |
| Panic | Add error handling |

### REST Calculator Issues

| Issue | Solution |
|-------|----------|
| Connection refused | Verify endpoint URL |
| 401/403 | Check authentication |
| Timeout | Increase timeout, check API |
| SSL error | Verify certificate or disable verification |

---

## Best Practices

1. **Start with Python** for prototyping, then optimize with other languages
2. **Use connection pooling** for Java calculators
3. **Enable compiler optimizations** for C++ (-O3)
4. **Use Rayon** for parallel Rust processing
5. **Set appropriate timeouts** for REST calculators
6. **Use environment variables** for credentials
7. **Monitor performance** with calculator statistics
8. **Implement fallbacks** for REST calculators

---

## See Also

- [Help: Py4J Java Integration](/help/py4j-integration)
- [Help: pybind11 C++ Integration](/help/pybind11-integration)
- [Help: Rust PyO3 Integration](/help/rust-integration)
- [Help: REST API Integration](/help/rest-integration)

---

## Legal Information

### Patent Notice

**PATENT PENDING**: The Multi-Language Calculator Framework implemented in DishtaYantra is the subject of one or more pending patent applications.

Protected innovations include:

1. **Hot-swappable calculator integration architecture**
2. **Unified calculator interface** across Python, Java, C++, and Rust
3. **Gateway pooling system** for Java/JVM integration (Py4J)
4. **Native binding abstraction layer** for C++ (pybind11) and Rust (PyO3)
5. **Automatic language detection and routing**
6. **Zero-copy data passing** between language boundaries

### Legal Notice

Unauthorized use, reproduction, or implementation of these technologies may constitute patent infringement.

### Copyright Notice

© 2025-2030 Ashutosh Sinha. All rights reserved.

### Trademark Notice

DishtaYantra™ is a trademark of Ashutosh Sinha.

---

**DishtaYantra v1.2.0** | Patent Pending | © 2025-2030 Ashutosh Sinha
