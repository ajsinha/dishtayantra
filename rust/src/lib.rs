//! DishtaYantra Rust Calculators
//! ==============================
//!
//! High-performance, memory-safe calculators for DishtaYantra using PyO3.
//!
//! All calculators implement the same interface as Python's DataCalculator:
//! - Constructor: new(name: String, config: &PyDict) -> Self
//! - Method: calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject>
//! - Method: details(&self, py: Python) -> PyResult<PyObject>
//!
//! Build with maturin:
//! ```bash
//! maturin develop --release
//! ```
//!
//! @author DishtaYantra
//! @version 1.1.0

use pyo3::prelude::*;
use pyo3::types::PyDict;
use chrono::Utc;
use std::collections::HashMap;

// ============================================================================
// Passthrough Calculator
// ============================================================================

/// Passthrough calculator - returns input unchanged.
/// Useful for testing connectivity.
#[pyclass]
pub struct PassthruCalculator {
    name: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl PassthruCalculator {
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

        // Return a copy of the input
        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "PassthruCalculator")?;
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
// Math Calculator
// ============================================================================

/// Mathematical operations calculator.
///
/// Configuration:
/// - operation: "add", "sum", "multiply", "mul", "max", "min", "mean", "avg", "std"
/// - arguments: List of field names to operate on
/// - output_attribute: Name of output field (default: "result")
#[pyclass]
pub struct MathCalculator {
    name: String,
    operation: String,
    arguments: Vec<String>,
    output_attribute: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl MathCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let operation: String = config
            .get_item("operation")?
            .map(|v| v.extract().unwrap_or_else(|_| "add".to_string()))
            .unwrap_or_else(|| "add".to_string());

        let output_attribute: String = config
            .get_item("output_attribute")?
            .map(|v| v.extract().unwrap_or_else(|_| "result".to_string()))
            .unwrap_or_else(|| "result".to_string());

        let arguments: Vec<String> = config
            .get_item("arguments")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        Ok(Self {
            name,
            operation,
            arguments,
            output_attribute,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        // Copy input to result
        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        // Collect numeric values
        let mut values: Vec<f64> = Vec::new();
        for arg in &self.arguments {
            if let Some(value) = data.get_item(arg)? {
                if let Ok(num) = value.extract::<f64>() {
                    values.push(num);
                }
            }
        }

        // Perform calculation
        let output = if values.is_empty() {
            0.0
        } else {
            match self.operation.as_str() {
                "add" | "sum" => values.iter().sum(),
                "multiply" | "mul" => values.iter().product(),
                "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                "mean" | "avg" => values.iter().sum::<f64>() / values.len() as f64,
                "std" => {
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    let variance = values.iter()
                        .map(|v| (v - mean).powi(2))
                        .sum::<f64>() / values.len() as f64;
                    variance.sqrt()
                }
                _ => values.iter().sum(),
            }
        };

        result.set_item(&self.output_attribute, output)?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "MathCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("operation", &self.operation)?;
        dict.set_item("output_attribute", &self.output_attribute)?;
        dict.set_item("num_arguments", self.arguments.len())?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// Trade Pricing Calculator
// ============================================================================

/// Trade pricing calculator for financial applications.
///
/// Configuration:
/// - commission_rate: Commission rate (default: 0.001 = 0.1%)
/// - tax_rate: Tax rate (default: 0.0)
/// - include_vat: Whether to add VAT (default: false)
///
/// Required input fields: price, quantity
/// Output fields: gross_value, commission, tax, vat, net_value, calculated_at
#[pyclass]
pub struct TradePricingCalculator {
    name: String,
    commission_rate: f64,
    tax_rate: f64,
    include_vat: bool,
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

        let include_vat: bool = config
            .get_item("include_vat")?
            .map(|v| v.extract().unwrap_or(false))
            .unwrap_or(false);

        Ok(Self {
            name,
            commission_rate,
            tax_rate,
            include_vat,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        // Copy input to result
        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        // Extract trade values
        let price: f64 = data
            .get_item("price")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'price' field"))?
            .extract()?;

        let quantity: f64 = data
            .get_item("quantity")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'quantity' field"))?
            .extract()?;

        // Calculate values
        let gross_value = price * quantity;
        let commission = gross_value * self.commission_rate;
        let tax = if self.tax_rate > 0.0 { gross_value * self.tax_rate } else { 0.0 };
        let vat = if self.include_vat { (gross_value + commission) * 0.20 } else { 0.0 };
        let net_value = gross_value + commission + tax + vat;

        // Set results
        result.set_item("gross_value", gross_value)?;
        result.set_item("commission", commission)?;
        result.set_item("tax", tax)?;
        result.set_item("vat", vat)?;
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
        dict.set_item("include_vat", self.include_vat)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// Risk Calculator
// ============================================================================

/// Risk calculator with VaR computation.
///
/// Configuration:
/// - confidence_level: VaR confidence level (default: 0.95)
/// - lookback_period: Days for calculation (default: 252)
///
/// Required input fields: position_value, volatility
/// Optional: delta, gamma, underlying_price (for Greeks-based risk)
#[pyclass]
pub struct RiskCalculator {
    name: String,
    confidence_level: f64,
    lookback_period: i32,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl RiskCalculator {
    #[new]
    fn new(name: String, config: &PyDict) -> PyResult<Self> {
        let confidence_level: f64 = config
            .get_item("confidence_level")?
            .map(|v| v.extract().unwrap_or(0.95))
            .unwrap_or(0.95);

        let lookback_period: i32 = config
            .get_item("lookback_period")?
            .map(|v| v.extract().unwrap_or(252))
            .unwrap_or(252);

        Ok(Self {
            name,
            confidence_level,
            lookback_period,
            calculation_count: 0,
            last_calculation: None,
        })
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        // Copy input to result
        let result = PyDict::new(py);
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }

        let position_value: f64 = data.get_item("position_value")?.unwrap().extract()?;
        let volatility: f64 = data.get_item("volatility")?.unwrap().extract()?;

        // Z-score for confidence level
        let z_score = self.get_z_score();

        // Daily VaR
        let daily_var = position_value * volatility * z_score / (self.lookback_period as f64).sqrt();
        result.set_item("daily_var", daily_var)?;

        // 10-day VaR
        result.set_item("var_10d", daily_var * 10.0_f64.sqrt())?;

        // Annual VaR
        result.set_item("annual_var", daily_var * 252.0_f64.sqrt())?;

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
        dict.set_item("type", "RiskCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("confidence_level", self.confidence_level)?;
        dict.set_item("lookback_period", self.lookback_period)?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

impl RiskCalculator {
    fn get_z_score(&self) -> f64 {
        if self.confidence_level >= 0.99 { 2.326 }
        else if self.confidence_level >= 0.95 { 1.645 }
        else if self.confidence_level >= 0.90 { 1.282 }
        else { 1.0 }
    }
}

// ============================================================================
// Parallel Calculator (demonstrates rayon)
// ============================================================================

/// Parallel calculator using rayon for multi-threaded processing.
///
/// Required input: values (list of numbers)
/// Output: processed (transformed values), count, sum
#[pyclass]
pub struct ParallelCalculator {
    name: String,
    calculation_count: u64,
    last_calculation: Option<String>,
}

#[pymethods]
impl ParallelCalculator {
    #[new]
    fn new(name: String, _config: &PyDict) -> Self {
        Self {
            name,
            calculation_count: 0,
            last_calculation: None,
        }
    }

    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        use rayon::prelude::*;

        self.calculation_count += 1;
        self.last_calculation = Some(Utc::now().to_rfc3339());

        // Get input array
        let values: Vec<f64> = data
            .get_item("values")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'values' field"))?
            .extract()?;

        // Release GIL and process in parallel
        let results: Vec<f64> = py.allow_threads(|| {
            values.par_iter()
                .map(|&x| x.powi(2) + x.sqrt())
                .collect()
        });

        let sum: f64 = results.iter().sum();

        let result = PyDict::new(py);
        result.set_item("processed", results)?;
        result.set_item("count", values.len())?;
        result.set_item("sum", sum)?;
        Ok(result.into())
    }

    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("type", "ParallelCalculator")?;
        dict.set_item("language", "Rust")?;
        dict.set_item("binding", "PyO3")?;
        dict.set_item("parallelism", "rayon")?;
        dict.set_item("calculation_count", self.calculation_count)?;
        Ok(dict.into())
    }
}

// ============================================================================
// Module Definition
// ============================================================================

/// DishtaYantra Rust Calculators Module
#[pymodule]
fn dishtayantra_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PassthruCalculator>()?;
    m.add_class::<MathCalculator>()?;
    m.add_class::<TradePricingCalculator>()?;
    m.add_class::<RiskCalculator>()?;
    m.add_class::<ParallelCalculator>()?;
    Ok(())
}
