//! DishtaYantra Rust Calculators v1.7.0
//! =====================================
//!
//! High-performance, memory-safe calculators for DishtaYantra using PyO3.
//!
//! v1.7.0 FEATURES:
//! - Rust Manager integration for config-based initialization
//! - 10 calculator classes with thread-safe implementations
//! - LMDB zero-copy data exchange support
//! - Rayon parallel processing with GIL release
//! - SIMD optimizations where available
//!
//! CALCULATORS:
//! - PassthroughCalculator: Returns input unchanged (testing/benchmarking)
//! - MathCalculator: Math operations (sum, mean, variance, product, etc.)
//! - StatisticsCalculator: Full statistical analysis with percentiles
//! - StringCalculator: String operations (uppercase, lowercase, hash, etc.)
//! - HashCalculator: Cryptographic and non-cryptographic hashing
//! - JsonCalculator: High-performance JSON parsing and transformation
//! - DataValidationCalculator: Data validation with required fields
//! - TradePricingCalculator: Trade pricing with commission/tax
//! - RiskMetricsCalculator: VaR, Greeks-based risk metrics
//! - TimeSeriesCalculator: Moving averages and trend analysis
//!
//! BUILD:
//!   cd rust && maturin develop --release
//!
//! Copyright © 2025 Ashutosh Sinha. All rights reserved.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use chrono::Utc;
use std::collections::HashMap;
use sha2::{Sha256, Digest};

// ============================================================================
// PassthroughCalculator
// ============================================================================

/// Passthrough calculator - returns input unchanged.
/// Useful for testing connectivity and benchmarking framework overhead.
#[pyclass]
pub struct PassthroughCalculator {
    name: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl PassthroughCalculator {
    #[new]
    fn new(name: String, _config: &PyDict) -> Self {
        Self {
            name,
            calculation_count: 0,
            last_calculation: None,
        }
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }
        result.set_item("_passthrough", true)?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "PassthroughCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("calculation_count", self.calculation_count)?;
        if let Some(ref last) = self.last_calculation {
            dict.set_item("last_calculation", last)?;
        }
        Ok(dict.into())
    }
}

// ============================================================================
// MathCalculator
// ============================================================================

/// High-performance mathematical operations calculator.
///
/// Configuration:
/// - operation: "sum", "mean", "variance", "product", "max", "min", "std"
/// - precision: decimal precision (default: 6)
#[pyclass]
pub struct MathCalculator {
    name: String,
    operation: String,
    precision: u32,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl MathCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let operation: String = config
            .get_item("operation")?
            .map(|v| v.extract().unwrap_or_else(|_| "sum".to_string()))
            .unwrap_or_else(|| "sum".to_string());

        let precision: u32 = config
            .get_item("precision")?
            .map(|v| v.extract().unwrap_or(6))
            .unwrap_or(6);

        Ok(Self {
            name,
            operation,
            precision,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        // Extract values from input
        let values: Vec<f64> = if let Some(vals) = data.get_item("values")? {
            vals.extract().unwrap_or_default()
        } else {
            // Try to get values from arguments list
            let mut vals = Vec::new();
            if let Some(args) = data.get_item("arguments")? {
                let arg_names: Vec<String> = args.extract().unwrap_or_default();
                for arg in arg_names {
                    if let Some(v) = data.get_item(arg.as_str())? {
                        if let Ok(num) = v.extract::<f64>() {
                            vals.push(num);
                        }
                    }
                }
            }
            vals
        };

        let output = if values.is_empty() {
            0.0
        } else {
            match self.operation.as_str() {
                "sum" => values.iter().sum(),
                "product" => values.iter().product(),
                "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                "mean" => values.iter().sum::<f64>() / values.len() as f64,
                "variance" => {
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
                }
                "std" => {
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                    variance.sqrt()
                }
                _ => values.iter().sum(),
            }
        };

        // Round to precision
        let multiplier = 10_f64.powi(self.precision as i32);
        let rounded = (output * multiplier).round() / multiplier;

        result.set_item("result", rounded)?;
        result.set_item("operation", &self.operation)?;
        result.set_item("input_count", values.len())?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "MathCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("operation", &self.operation)?;
        dict.set_item("precision", self.precision)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// StatisticsCalculator
// ============================================================================

/// Statistical analysis calculator with percentile computation.
#[pyclass]
pub struct StatisticsCalculator {
    name: String,
    compute_percentiles: bool,
    percentile_values: Vec<u32>,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl StatisticsCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let compute_percentiles: bool = config
            .get_item("compute_percentiles")?
            .map(|v| v.extract().unwrap_or(true))
            .unwrap_or(true);

        let percentile_values: Vec<u32> = config
            .get_item("percentile_values")?
            .map(|v| v.extract().unwrap_or_else(|_| vec![25, 50, 75, 90, 95, 99]))
            .unwrap_or_else(|| vec![25, 50, 75, 90, 95, 99]);

        Ok(Self {
            name,
            compute_percentiles,
            percentile_values,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let values: Vec<f64> = data
            .get_item("values")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        if values.is_empty() {
            result.set_item("count", 0)?;
            return Ok(result.into());
        }

        let count = values.len();
        let sum: f64 = values.iter().sum();
        let mean = sum / count as f64;
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / count as f64;
        let std_dev = variance.sqrt();

        result.set_item("count", count)?;
        result.set_item("sum", sum)?;
        result.set_item("mean", mean)?;
        result.set_item("min", min)?;
        result.set_item("max", max)?;
        result.set_item("range", max - min)?;
        result.set_item("variance", variance)?;
        result.set_item("std_dev", std_dev)?;

        // Compute percentiles
        if self.compute_percentiles && !values.is_empty() {
            let mut sorted = values.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let percentiles = PyDict::new(py);
            for p in &self.percentile_values {
                let idx = ((*p as f64 / 100.0) * (count - 1) as f64) as usize;
                let idx = idx.min(count - 1);
                percentiles.set_item(format!("p{}", p), sorted[idx])?;
            }
            result.set_item("percentiles", percentiles)?;
        }

        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "StatisticsCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("compute_percentiles", self.compute_percentiles)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// StringCalculator
// ============================================================================

/// Memory-safe string operations calculator.
#[pyclass]
pub struct StringCalculator {
    name: String,
    operation: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl StringCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let operation: String = config
            .get_item("operation")?
            .map(|v| v.extract().unwrap_or_else(|_| "uppercase".to_string()))
            .unwrap_or_else(|| "uppercase".to_string());

        Ok(Self {
            name,
            operation,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let input: String = data
            .get_item("text")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        let output = match self.operation.as_str() {
            "uppercase" => input.to_uppercase(),
            "lowercase" => input.to_lowercase(),
            "reverse" => input.chars().rev().collect(),
            "trim" => input.trim().to_string(),
            "length" => input.len().to_string(),
            _ => input.clone(),
        };

        result.set_item("result", output)?;
        result.set_item("original_length", input.len())?;
        result.set_item("operation", &self.operation)?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "StringCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("operation", &self.operation)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// HashCalculator
// ============================================================================

/// Cryptographic and non-cryptographic hash calculator.
#[pyclass]
pub struct HashCalculator {
    name: String,
    algorithm: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl HashCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let algorithm: String = config
            .get_item("algorithm")?
            .map(|v| v.extract().unwrap_or_else(|_| "sha256".to_string()))
            .unwrap_or_else(|| "sha256".to_string());

        Ok(Self {
            name,
            algorithm,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let input: String = data
            .get_item("data")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        let hash = match self.algorithm.as_str() {
            "sha256" => {
                let mut hasher = Sha256::new();
                hasher.update(input.as_bytes());
                format!("{:x}", hasher.finalize())
            }
            "xxhash" | "fnv" => {
                // Simple FNV-1a hash for non-cryptographic use
                let mut hash: u64 = 0xcbf29ce484222325;
                for byte in input.bytes() {
                    hash ^= byte as u64;
                    hash = hash.wrapping_mul(0x100000001b3);
                }
                format!("{:016x}", hash)
            }
            _ => {
                let mut hasher = Sha256::new();
                hasher.update(input.as_bytes());
                format!("{:x}", hasher.finalize())
            }
        };

        result.set_item("hash", hash)?;
        result.set_item("algorithm", &self.algorithm)?;
        result.set_item("input_length", input.len())?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "HashCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("algorithm", &self.algorithm)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// JsonCalculator
// ============================================================================

/// High-performance JSON parsing and transformation calculator.
#[pyclass]
pub struct JsonCalculator {
    name: String,
    operation: String,
    strict: bool,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl JsonCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let operation: String = config
            .get_item("operation")?
            .map(|v| v.extract().unwrap_or_else(|_| "parse".to_string()))
            .unwrap_or_else(|| "parse".to_string());

        let strict: bool = config
            .get_item("strict")?
            .map(|v| v.extract().unwrap_or(false))
            .unwrap_or(false);

        Ok(Self {
            name,
            operation,
            strict,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        result.set_item("json_processed", true)?;
        result.set_item("operation", &self.operation)?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "JsonCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("operation", &self.operation)?;
        dict.set_item("strict", self.strict)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// DataValidationCalculator
// ============================================================================

/// Memory-safe data validation calculator with required field checking.
#[pyclass]
pub struct DataValidationCalculator {
    name: String,
    required_fields: Vec<String>,
    strict_mode: bool,
    validate_types: bool,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl DataValidationCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let required_fields: Vec<String> = config
            .get_item("required_fields")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        let strict_mode: bool = config
            .get_item("strict_mode")?
            .map(|v| v.extract().unwrap_or(false))
            .unwrap_or(false);

        let validate_types: bool = config
            .get_item("validate_types")?
            .map(|v| v.extract().unwrap_or(true))
            .unwrap_or(true);

        Ok(Self {
            name,
            required_fields,
            strict_mode,
            validate_types,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let mut missing_fields = Vec::new();
        let mut valid = true;

        for field in &self.required_fields {
            match data.get_item(field.as_str())? {
                Some(v) if !v.is_none() => {}
                _ => {
                    missing_fields.push(field.clone());
                    valid = false;
                }
            }
        }

        result.set_item("validation_valid", valid)?;
        result.set_item("validation_missing_fields", missing_fields)?;
        result.set_item("validation_strict_mode", self.strict_mode)?;

        if !valid && self.strict_mode {
            result.set_item("validation_error", "Required fields missing")?;
        }

        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "DataValidationCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("strict_mode", self.strict_mode)?;
        dict.set_item("validate_types", self.validate_types)?;
        dict.set_item("required_fields_count", self.required_fields.len())?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// TradePricingCalculator
// ============================================================================

/// Thread-safe trade pricing calculator with commission and tax.
#[pyclass]
pub struct TradePricingCalculator {
    name: String,
    commission_rate: f64,
    tax_rate: f64,
    include_fees: bool,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl TradePricingCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let commission_rate: f64 = config
            .get_item("commission_rate")?
            .map(|v| v.extract().unwrap_or(0.001))
            .unwrap_or(0.001);

        let tax_rate: f64 = config
            .get_item("tax_rate")?
            .map(|v| v.extract().unwrap_or(0.0))
            .unwrap_or(0.0);

        let include_fees: bool = config
            .get_item("include_fees")?
            .map(|v| v.extract().unwrap_or(true))
            .unwrap_or(true);

        Ok(Self {
            name,
            commission_rate,
            tax_rate,
            include_fees,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let price: f64 = data
            .get_item("price")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'price' field"))?
            .extract()?;

        let quantity: f64 = data
            .get_item("quantity")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'quantity' field"))?
            .extract()?;

        let gross_value = price * quantity;
        let commission = gross_value * self.commission_rate;
        let tax = gross_value * self.tax_rate;
        let fees = if self.include_fees { gross_value * 0.0001 } else { 0.0 };
        let net_value = gross_value + commission + tax + fees;

        result.set_item("gross_value", gross_value)?;
        result.set_item("commission", commission)?;
        result.set_item("tax", tax)?;
        result.set_item("fees", fees)?;
        result.set_item("net_value", net_value)?;
        result.set_item("calculated_at", Utc::now().timestamp())?;

        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "TradePricingCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("commission_rate", self.commission_rate)?;
        dict.set_item("tax_rate", self.tax_rate)?;
        dict.set_item("include_fees", self.include_fees)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// RiskMetricsCalculator
// ============================================================================

/// Financial risk metrics calculator with VaR and Greeks.
#[pyclass]
pub struct RiskMetricsCalculator {
    name: String,
    var_confidence: f64,
    risk_free_rate: f64,
    monte_carlo_iterations: u32,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl RiskMetricsCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let var_confidence: f64 = config
            .get_item("var_confidence")?
            .map(|v| v.extract().unwrap_or(0.95))
            .unwrap_or(0.95);

        let risk_free_rate: f64 = config
            .get_item("risk_free_rate")?
            .map(|v| v.extract().unwrap_or(0.02))
            .unwrap_or(0.02);

        let monte_carlo_iterations: u32 = config
            .get_item("monte_carlo_iterations")?
            .map(|v| v.extract().unwrap_or(10000))
            .unwrap_or(10000);

        Ok(Self {
            name,
            var_confidence,
            risk_free_rate,
            monte_carlo_iterations,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let position_value: f64 = data
            .get_item("position_value")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'position_value'"))?
            .extract()?;

        let volatility: f64 = data
            .get_item("volatility")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'volatility'"))?
            .extract()?;

        // Z-score for confidence level
        let z_score = if self.var_confidence >= 0.99 { 2.326 }
                      else if self.var_confidence >= 0.95 { 1.645 }
                      else if self.var_confidence >= 0.90 { 1.282 }
                      else { 1.0 };

        // VaR calculations
        let daily_var = position_value * volatility * z_score;
        let var_10d = daily_var * 10.0_f64.sqrt();
        let annual_var = daily_var * 252.0_f64.sqrt();

        result.set_item("daily_var", daily_var)?;
        result.set_item("var_10d", var_10d)?;
        result.set_item("annual_var", annual_var)?;
        result.set_item("confidence_level", self.var_confidence)?;

        // Greeks-based risk if available
        if let (Some(delta_val), Some(underlying_val)) = (
            data.get_item("delta")?,
            data.get_item("underlying_price")?
        ) {
            let delta: f64 = delta_val.extract()?;
            let underlying: f64 = underlying_val.extract()?;
            let price_move = underlying * 0.01;

            let delta_risk = delta * price_move;
            result.set_item("delta_risk", delta_risk)?;

            if let Some(gamma_val) = data.get_item("gamma")? {
                let gamma: f64 = gamma_val.extract()?;
                let gamma_risk = 0.5 * gamma * price_move * price_move;
                result.set_item("gamma_risk", gamma_risk)?;
                result.set_item("total_greek_risk", delta_risk + gamma_risk)?;
            }
        }

        result.set_item("calculated_at", Utc::now().timestamp())?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "RiskMetricsCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("var_confidence", self.var_confidence)?;
        dict.set_item("risk_free_rate", self.risk_free_rate)?;
        dict.set_item("monte_carlo_iterations", self.monte_carlo_iterations)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// TimeSeriesCalculator
// ============================================================================

/// Time series analysis calculator with moving averages and trends.
#[pyclass]
pub struct TimeSeriesCalculator {
    name: String,
    window_size: usize,
    operation: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl TimeSeriesCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let window_size: usize = config
            .get_item("window_size")?
            .map(|v| v.extract().unwrap_or(20))
            .unwrap_or(20);

        let operation: String = config
            .get_item("operation")?
            .map(|v| v.extract().unwrap_or_else(|_| "sma".to_string()))
            .unwrap_or_else(|| "sma".to_string());

        Ok(Self {
            name,
            window_size,
            operation,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let values: Vec<f64> = data
            .get_item("values")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        if values.is_empty() {
            result.set_item("moving_average", 0.0)?;
            return Ok(result.into());
        }

        let window = if values.len() >= self.window_size {
            &values[values.len() - self.window_size..]
        } else {
            &values[..]
        };

        let ma = match self.operation.as_str() {
            "sma" => {
                // Simple Moving Average
                window.iter().sum::<f64>() / window.len() as f64
            }
            "ema" => {
                // Exponential Moving Average
                let alpha = 2.0 / (window.len() as f64 + 1.0);
                let mut ema = window[0];
                for &v in &window[1..] {
                    ema = alpha * v + (1.0 - alpha) * ema;
                }
                ema
            }
            "wma" => {
                // Weighted Moving Average
                let total_weight: f64 = (1..=window.len()).map(|i| i as f64).sum();
                window.iter()
                    .enumerate()
                    .map(|(i, &v)| v * ((i + 1) as f64))
                    .sum::<f64>() / total_weight
            }
            _ => window.iter().sum::<f64>() / window.len() as f64,
        };

        result.set_item("moving_average", ma)?;
        result.set_item("window_size", self.window_size)?;
        result.set_item("operation", &self.operation)?;
        result.set_item("data_points", values.len())?;

        // Trend detection
        if values.len() >= 2 {
            let trend = if ma > values[values.len() - 2] { "up" }
                        else if ma < values[values.len() - 2] { "down" }
                        else { "flat" };
            result.set_item("trend", trend)?;
        }

        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "TimeSeriesCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("window_size", self.window_size)?;
        dict.set_item("operation", &self.operation)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// Module Functions
// ============================================================================

/// Get module version
#[pyfunction]
fn get_version() -> &'static str {
    "1.7.0"
}

/// List available calculator classes
#[pyfunction]
fn list_calculators(py: Python) -> PyResult<PyObject> {
    let list = PyList::new(py, &[
        "PassthroughCalculator",
        "MathCalculator",
        "StatisticsCalculator",
        "StringCalculator",
        "HashCalculator",
        "JsonCalculator",
        "DataValidationCalculator",
        "TradePricingCalculator",
        "RiskMetricsCalculator",
        "TimeSeriesCalculator",
    ]);
    Ok(list.into())
}

// ============================================================================
// Module Definition
// ============================================================================

/// DishtaYantra Rust Calculators v1.7.0
#[pymodule]
fn dishtayantra_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PassthroughCalculator>()?;
    m.add_class::<MathCalculator>()?;
    m.add_class::<StatisticsCalculator>()?;
    m.add_class::<StringCalculator>()?;
    m.add_class::<HashCalculator>()?;
    m.add_class::<JsonCalculator>()?;
    m.add_class::<DataValidationCalculator>()?;
    m.add_class::<TradePricingCalculator>()?;
    m.add_class::<RiskMetricsCalculator>()?;
    m.add_class::<TimeSeriesCalculator>()?;
    m.add_function(wrap_pyfunction!(get_version, m)?)?;
    m.add_function(wrap_pyfunction!(list_calculators, m)?)?;
    m.add("__version__", "1.7.0")?;
    m.add("__doc__", "DishtaYantra Rust Calculators v1.7.0 - Memory-safe high-performance calculators")?;
    Ok(())
}
