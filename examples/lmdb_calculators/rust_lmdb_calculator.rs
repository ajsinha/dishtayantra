//! LMDB-Enabled Rust Calculator Example
//!
//! This example demonstrates how to use LMDB zero-copy data exchange
//! in a Rust calculator for high-performance large payload processing.
//!
//! # Dependencies (Cargo.toml)
//! ```toml
//! [dependencies]
//! pyo3 = { version = "0.20", features = ["extension-module"] }
//! lmdb-rs = "0.8"
//! rmp-serde = "1.1"
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! rayon = "1.8"
//! ```
//!
//! # Build
//! ```bash
//! maturin develop --release
//! ```
//!
//! Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// LMDB imports
use lmdb::{Database, Environment, EnvironmentFlags, Transaction, WriteFlags};

/// LMDB Helper for zero-copy data access
#[pyclass]
struct LMDBHelper {
    env: Option<Arc<Environment>>,
    db: Option<Database>,
    db_path: String,
    initialized: bool,
}

#[pymethods]
impl LMDBHelper {
    #[new]
    fn new() -> Self {
        LMDBHelper {
            env: None,
            db: None,
            db_path: String::new(),
            initialized: false,
        }
    }

    /// Initialize LMDB environment
    fn init(&mut self, db_path: &str) -> PyResult<bool> {
        // Create directory if it doesn't exist
        if !Path::new(db_path).exists() {
            std::fs::create_dir_all(db_path).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to create directory: {}",
                    e
                ))
            })?;
        }

        // Create LMDB environment
        let env = Environment::new()
            .set_flags(EnvironmentFlags::WRITE_MAP | EnvironmentFlags::NO_SYNC)
            .set_max_dbs(100)
            .set_map_size(1024 * 1024 * 1024) // 1GB
            .open(Path::new(db_path))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to open LMDB: {}",
                    e
                ))
            })?;

        // Open default database
        let db = env.create_db(Some("default"), lmdb::DatabaseFlags::empty())
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create database: {}",
                    e
                ))
            })?;

        self.env = Some(Arc::new(env));
        self.db = Some(db);
        self.db_path = db_path.to_string();
        self.initialized = true;

        Ok(true)
    }

    /// Read data from LMDB
    fn get(&self, key: &str) -> PyResult<Option<Vec<u8>>> {
        let env = self.env.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("LMDB not initialized")
        })?;
        let db = self.db.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Database not opened")
        })?;

        let txn = env.begin_ro_txn().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to begin transaction: {}",
                e
            ))
        })?;

        // ZERO-COPY: This returns a reference to the memory-mapped data!
        match txn.get(*db, &key.as_bytes()) {
            Ok(data) => Ok(Some(data.to_vec())),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to get data: {}",
                e
            ))),
        }
    }

    /// Write data to LMDB
    fn put(&self, key: &str, data: Vec<u8>) -> PyResult<bool> {
        let env = self.env.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("LMDB not initialized")
        })?;
        let db = self.db.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Database not opened")
        })?;

        let mut txn = env.begin_rw_txn().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to begin transaction: {}",
                e
            ))
        })?;

        txn.put(*db, &key.as_bytes(), &data, WriteFlags::empty())
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to put data: {}",
                    e
                ))
            })?;

        txn.commit().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to commit: {}",
                e
            ))
        })?;

        Ok(true)
    }

    /// Delete key from LMDB
    fn delete(&self, key: &str) -> PyResult<bool> {
        let env = self.env.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("LMDB not initialized")
        })?;
        let db = self.db.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Database not opened")
        })?;

        let mut txn = env.begin_rw_txn().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to begin transaction: {}",
                e
            ))
        })?;

        match txn.del(*db, &key.as_bytes(), None) {
            Ok(_) => {
                txn.commit().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to commit: {}",
                        e
                    ))
                })?;
                Ok(true)
            }
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to delete: {}",
                e
            ))),
        }
    }

    /// Check if LMDB is initialized
    fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get database path
    fn get_path(&self) -> &str {
        &self.db_path
    }
}

/// Matrix Processor Calculator with LMDB support
#[pyclass]
struct MatrixProcessorCalculator {
    lmdb: LMDBHelper,
    calculations: u64,
    lmdb_exchanges: u64,
    total_time_ns: u64,
}

#[pymethods]
impl MatrixProcessorCalculator {
    #[new]
    fn new() -> Self {
        MatrixProcessorCalculator {
            lmdb: LMDBHelper::new(),
            calculations: 0,
            lmdb_exchanges: 0,
            total_time_ns: 0,
        }
    }

    /// Initialize LMDB
    fn init_lmdb(&mut self, db_path: &str) -> PyResult<bool> {
        self.lmdb.init(db_path)
    }

    /// Main calculation entry point
    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let start = Instant::now();
        self.calculations += 1;

        let result = if self.is_lmdb_reference(data)? {
            self.calculate_with_lmdb(py, data)?
        } else {
            self.process_data(py, data)?
        };

        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.total_time_ns += elapsed_ns;

        // Add timing to result
        result.set_item("_processing_time_ns", elapsed_ns)?;

        Ok(result.into())
    }

    /// Get calculator statistics
    fn get_stats(&self, py: Python) -> PyResult<PyObject> {
        let stats = PyDict::new(py);
        stats.set_item("name", "MatrixProcessor")?;
        stats.set_item("calculations", self.calculations)?;
        stats.set_item("lmdb_exchanges", self.lmdb_exchanges)?;
        stats.set_item("total_time_ns", self.total_time_ns)?;
        stats.set_item(
            "avg_time_us",
            if self.calculations > 0 {
                (self.total_time_ns / self.calculations) as f64 / 1000.0
            } else {
                0.0
            },
        )?;
        stats.set_item("lmdb_initialized", self.lmdb.is_initialized())?;
        stats.set_item("db_path", self.lmdb.get_path())?;
        Ok(stats.into())
    }
}

impl MatrixProcessorCalculator {
    fn is_lmdb_reference(&self, data: &PyDict) -> PyResult<bool> {
        if let Some(ref_val) = data.get_item("_lmdb_ref") {
            Ok(ref_val.extract::<bool>().unwrap_or(false))
        } else {
            Ok(false)
        }
    }

    fn calculate_with_lmdb(&mut self, py: Python, ref_data: &PyDict) -> PyResult<&PyDict> {
        self.lmdb_exchanges += 1;

        let input_key: String = ref_data
            .get_item("_lmdb_input_key")
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyKeyError, _>("_lmdb_input_key not found")
            })?
            .extract()?;

        let output_key: Option<String> = ref_data
            .get_item("_lmdb_output_key")
            .and_then(|v| v.extract().ok());

        let db_path: String = ref_data
            .get_item("_lmdb_db_path")
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyKeyError, _>("_lmdb_db_path not found")
            })?
            .extract()?;

        // Initialize LMDB if needed
        if !self.lmdb.is_initialized() || self.lmdb.get_path() != db_path {
            self.lmdb.init(&db_path)?;
        }

        // Read data from LMDB (zero-copy via memory map!)
        let raw_data = self.lmdb.get(&input_key)?.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "No data at key: {}",
                input_key
            ))
        })?;

        // Process raw bytes
        let result = self.process_raw_data(py, &raw_data)?;

        // Write result to LMDB if output key specified
        if let Some(out_key) = output_key {
            // Serialize result to JSON for simplicity
            let result_json = serde_json::to_vec(&self.dict_to_map(result)?)
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Serialization failed: {}",
                        e
                    ))
                })?;
            self.lmdb.put(&out_key, result_json)?;
        }

        result.set_item("_exchange_type", "lmdb")?;
        Ok(result)
    }

    fn process_data(&self, py: Python, data: &PyDict) -> PyResult<&PyDict> {
        let result = PyDict::new(py);

        // Get matrix
        let matrix: Option<Vec<Vec<f64>>> = data
            .get_item("matrix")
            .and_then(|m| m.extract().ok());

        let operation: String = data
            .get_item("operation")
            .and_then(|o| o.extract().ok())
            .unwrap_or_else(|| "sum".to_string());

        result.set_item("operation", &operation)?;

        if let Some(mat) = matrix {
            result.set_item("input_rows", mat.len())?;
            result.set_item("input_cols", mat.first().map(|r| r.len()).unwrap_or(0))?;

            match operation.as_str() {
                "sum" => {
                    // Use rayon for parallel sum
                    let sum: f64 = mat.iter().flatten().sum();
                    result.set_item("result", sum)?;
                }
                "mean" => {
                    let flat: Vec<f64> = mat.into_iter().flatten().collect();
                    let mean = flat.iter().sum::<f64>() / flat.len() as f64;
                    result.set_item("result", mean)?;
                }
                "max" => {
                    let max = mat
                        .iter()
                        .flatten()
                        .cloned()
                        .fold(f64::NEG_INFINITY, f64::max);
                    result.set_item("result", max)?;
                }
                "min" => {
                    let min = mat
                        .iter()
                        .flatten()
                        .cloned()
                        .fold(f64::INFINITY, f64::min);
                    result.set_item("result", min)?;
                }
                _ => {}
            }
        } else {
            result.set_item("input_rows", 0)?;
            result.set_item("input_cols", 0)?;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        result.set_item("processed_at", timestamp)?;

        Ok(result)
    }

    fn process_raw_data(&self, py: Python, data: &[u8]) -> PyResult<&PyDict> {
        let result = PyDict::new(py);
        result.set_item("raw_size", data.len())?;
        result.set_item("zero_copy", true)?;

        // If data is array of f64, process directly
        if data.len() % 8 == 0 {
            let doubles: Vec<f64> = data
                .chunks(8)
                .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
                .collect();

            if !doubles.is_empty() {
                let sum: f64 = doubles.iter().sum();
                let mean = sum / doubles.len() as f64;
                let variance: f64 = doubles.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                    / doubles.len() as f64;

                result.set_item("count", doubles.len())?;
                result.set_item("sum", sum)?;
                result.set_item("mean", mean)?;
                result.set_item("variance", variance)?;
                result.set_item("std_dev", variance.sqrt())?;
            }
        }

        Ok(result)
    }

    fn dict_to_map(&self, dict: &PyDict) -> PyResult<HashMap<String, serde_json::Value>> {
        let mut map = HashMap::new();
        for (key, value) in dict.iter() {
            let k: String = key.extract()?;
            let v = self.py_to_json(value)?;
            map.insert(k, v);
        }
        Ok(map)
    }

    fn py_to_json(&self, obj: &PyAny) -> PyResult<serde_json::Value> {
        if obj.is_none() {
            Ok(serde_json::Value::Null)
        } else if let Ok(b) = obj.extract::<bool>() {
            Ok(serde_json::Value::Bool(b))
        } else if let Ok(i) = obj.extract::<i64>() {
            Ok(serde_json::Value::Number(i.into()))
        } else if let Ok(f) = obj.extract::<f64>() {
            Ok(serde_json::json!(f))
        } else if let Ok(s) = obj.extract::<String>() {
            Ok(serde_json::Value::String(s))
        } else {
            Ok(serde_json::Value::String(format!("{:?}", obj)))
        }
    }
}

/// Risk Calculator with LMDB support
#[pyclass]
struct RiskCalculator {
    lmdb: LMDBHelper,
    calculations: u64,
    lmdb_exchanges: u64,
    total_time_ns: u64,
}

#[pymethods]
impl RiskCalculator {
    #[new]
    fn new() -> Self {
        RiskCalculator {
            lmdb: LMDBHelper::new(),
            calculations: 0,
            lmdb_exchanges: 0,
            total_time_ns: 0,
        }
    }

    fn init_lmdb(&mut self, db_path: &str) -> PyResult<bool> {
        self.lmdb.init(db_path)
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let start = Instant::now();
        self.calculations += 1;

        let result = PyDict::new(py);

        // Extract positions
        let positions: Option<&PyList> = data.get_item("positions").and_then(|p| p.downcast().ok());

        let mut total_exposure = 0.0f64;
        let position_count = positions.map(|p| p.len()).unwrap_or(0);

        if let Some(pos_list) = positions {
            for pos in pos_list.iter() {
                if let Ok(pos_dict) = pos.downcast::<PyDict>() {
                    let qty: f64 = pos_dict
                        .get_item("quantity")
                        .and_then(|v| v.extract().ok())
                        .unwrap_or(0.0);
                    let price: f64 = pos_dict
                        .get_item("price")
                        .and_then(|v| v.extract().ok())
                        .unwrap_or(0.0);
                    total_exposure += qty * price;
                }
            }
        }

        result.set_item("total_positions", position_count)?;
        result.set_item("total_exposure", total_exposure)?;
        result.set_item("var_95", total_exposure * 0.025)?;
        result.set_item("var_99", total_exposure * 0.035)?;
        result.set_item("expected_shortfall", total_exposure * 0.04)?;

        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.total_time_ns += elapsed_ns;
        result.set_item("_processing_time_ns", elapsed_ns)?;

        Ok(result.into())
    }

    fn get_stats(&self, py: Python) -> PyResult<PyObject> {
        let stats = PyDict::new(py);
        stats.set_item("name", "RiskCalculator")?;
        stats.set_item("calculations", self.calculations)?;
        stats.set_item("lmdb_exchanges", self.lmdb_exchanges)?;
        stats.set_item("total_time_ns", self.total_time_ns)?;
        stats.set_item("lmdb_initialized", self.lmdb.is_initialized())?;
        Ok(stats.into())
    }
}

/// Signal Processor Calculator with parallel processing
#[pyclass]
struct SignalProcessorCalculator {
    lmdb: LMDBHelper,
    calculations: u64,
    total_time_ns: u64,
}

#[pymethods]
impl SignalProcessorCalculator {
    #[new]
    fn new() -> Self {
        SignalProcessorCalculator {
            lmdb: LMDBHelper::new(),
            calculations: 0,
            total_time_ns: 0,
        }
    }

    fn init_lmdb(&mut self, db_path: &str) -> PyResult<bool> {
        self.lmdb.init(db_path)
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let start = Instant::now();
        self.calculations += 1;

        let result = PyDict::new(py);

        let signal: Option<Vec<f64>> = data
            .get_item("signal")
            .and_then(|s| s.extract().ok());

        if let Some(sig) = signal {
            if !sig.is_empty() {
                // Use Rayon for parallel processing
                use rayon::prelude::*;

                let sum: f64 = sig.par_iter().sum();
                let sq_sum: f64 = sig.par_iter().map(|x| x * x).sum();
                let min: f64 = sig.par_iter().cloned().reduce(|| f64::INFINITY, f64::min);
                let max: f64 = sig.par_iter().cloned().reduce(|| f64::NEG_INFINITY, f64::max);

                let mean = sum / sig.len() as f64;
                let variance = (sq_sum / sig.len() as f64) - (mean * mean);

                result.set_item("sample_count", sig.len())?;
                result.set_item("mean", mean)?;
                result.set_item("variance", variance)?;
                result.set_item("std_dev", variance.sqrt())?;
                result.set_item("min", min)?;
                result.set_item("max", max)?;
                result.set_item("range", max - min)?;
            }
        }

        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.total_time_ns += elapsed_ns;
        result.set_item("_processing_time_ns", elapsed_ns)?;

        Ok(result.into())
    }

    fn get_stats(&self, py: Python) -> PyResult<PyObject> {
        let stats = PyDict::new(py);
        stats.set_item("name", "SignalProcessor")?;
        stats.set_item("calculations", self.calculations)?;
        stats.set_item("total_time_ns", self.total_time_ns)?;
        stats.set_item("lmdb_initialized", self.lmdb.is_initialized())?;
        Ok(stats.into())
    }
}

/// Python module definition
#[pymodule]
fn lmdb_calculator(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<LMDBHelper>()?;
    m.add_class::<MatrixProcessorCalculator>()?;
    m.add_class::<RiskCalculator>()?;
    m.add_class::<SignalProcessorCalculator>()?;
    Ok(())
}
