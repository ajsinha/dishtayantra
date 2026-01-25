/**
 * DishtaYantra C++ Calculators
 * ============================
 * 
 * High-performance calculators using pybind11 integration.
 * 
 * v1.7.0: CPP Manager Integration
 *   - Calculators now defined in config/cpp_config.json
 *   - Centralized module loading via CPP Manager
 *   - Automatic initialization on application startup
 *   - Web UI management at /cpp
 * 
 * v1.1.1: Added LMDB zero-copy data exchange support for large payloads
 * 
 * Calculator Classes Available:
 *   - PassthroughCalculator: Returns input unchanged (testing)
 *   - MathCalculator: Mathematical operations (sum, mean, variance, etc.)
 *   - StatisticsCalculator: Statistical analysis with percentiles
 *   - MatrixCalculator: Matrix operations
 *   - StringTransformCalculator: String operations
 *   - DataValidationCalculator: Data validation
 *   - TradePricingCalculator: Trade pricing with commissions
 *   - RiskMetricsCalculator: Financial risk metrics (VaR, Greeks)
 * 
 * Build instructions (CMake recommended):
 *   cd cpp && mkdir build && cd build && cmake .. && make
 * 
 * Manual build:
 *   g++ -O3 -Wall -shared -std=c++17 -fPIC \
 *       $(python3 -m pybind11 --includes) \
 *       dishtayantra_cpp.cpp \
 *       -o dishtayantra_cpp$(python3-config --extension-suffix)
 * 
 * @author DishtaYantra
 * @version 1.7.0
 * @copyright 2025 Ashutosh Sinha. All rights reserved.
 */

#include "calculator.hpp"
#include <cmath>
#include <vector>
#include <numeric>
#include <algorithm>
#include <functional>

namespace dishtayantra {

// ============================================================================
// PassthroughCalculator
// ============================================================================

/**
 * Passthrough calculator - returns input unchanged.
 * Useful for testing connectivity and benchmarking.
 */
class PassthroughCalculator : public Calculator {
public:
    using Calculator::Calculator;
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        return py::dict(data);
    }
    
protected:
    std::string get_type_name() const override {
        return "PassthroughCalculator";
    }
};


// ============================================================================
// MathCalculator
// ============================================================================

/**
 * Mathematical operations calculator.
 * 
 * Configuration:
 * - operation: "sum", "mean", "variance", "product", "max", "min", "std"
 * - precision: Number of decimal places (default: 6)
 * - arguments: List of field names to operate on (optional)
 */
class MathCalculator : public Calculator {
private:
    std::string operation_;
    std::vector<std::string> arguments_;
    std::string output_attr_;
    int precision_;
    
public:
    MathCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        operation_ = get_config<std::string>("operation", "sum");
        output_attr_ = get_config<std::string>("output_attribute", "result");
        precision_ = get_config<int>("precision", 6);
        
        if (config.contains("arguments")) {
            auto args = config["arguments"].cast<py::list>();
            for (auto& arg : args) {
                arguments_.push_back(arg.cast<std::string>());
            }
        }
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        std::vector<double> values;
        
        // Get values from arguments or 'values' field
        if (!arguments_.empty()) {
            for (const auto& arg : arguments_) {
                if (data.contains(arg.c_str())) {
                    try {
                        values.push_back(data[arg.c_str()].cast<double>());
                    } catch (...) {}
                }
            }
        } else if (data.contains("values")) {
            auto vals = data["values"].cast<py::list>();
            for (auto& v : vals) {
                values.push_back(v.cast<double>());
            }
        }
        
        if (values.empty()) {
            result[output_attr_.c_str()] = 0.0;
            return result;
        }
        
        double output = 0.0;
        
        if (operation_ == "sum") {
            output = std::accumulate(values.begin(), values.end(), 0.0);
        } else if (operation_ == "product") {
            output = std::accumulate(values.begin(), values.end(), 1.0,
                                     std::multiplies<double>());
        } else if (operation_ == "max") {
            output = *std::max_element(values.begin(), values.end());
        } else if (operation_ == "min") {
            output = *std::min_element(values.begin(), values.end());
        } else if (operation_ == "mean") {
            output = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
        } else if (operation_ == "variance") {
            double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
            double sq_sum = 0;
            for (double v : values) {
                sq_sum += (v - mean) * (v - mean);
            }
            output = sq_sum / values.size();
        } else if (operation_ == "std") {
            double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
            double sq_sum = 0;
            for (double v : values) {
                sq_sum += (v - mean) * (v - mean);
            }
            output = std::sqrt(sq_sum / values.size());
        }
        
        result[output_attr_.c_str()] = output;
        result["operation"] = operation_;
        result["input_count"] = static_cast<int>(values.size());
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["operation"] = operation_;
        d["precision"] = precision_;
        d["output_attribute"] = output_attr_;
        return d;
    }
    
protected:
    std::string get_type_name() const override {
        return "MathCalculator";
    }
};


// ============================================================================
// StatisticsCalculator
// ============================================================================

/**
 * Statistics calculator - advanced statistical analysis.
 * 
 * Configuration:
 * - compute_percentiles: Whether to compute percentiles (default: true)
 * - percentile_values: List of percentiles to compute (default: [25, 50, 75, 90, 95, 99])
 */
class StatisticsCalculator : public Calculator {
private:
    bool compute_percentiles_;
    std::vector<int> percentile_values_;
    
public:
    StatisticsCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        compute_percentiles_ = get_config<bool>("compute_percentiles", true);
        
        if (config.contains("percentile_values")) {
            auto pvals = config["percentile_values"].cast<py::list>();
            for (auto& p : pvals) {
                percentile_values_.push_back(p.cast<int>());
            }
        } else {
            percentile_values_ = {25, 50, 75, 90, 95, 99};
        }
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        std::vector<double> values;
        
        if (data.contains("values")) {
            auto vals = data["values"].cast<py::list>();
            for (auto& v : vals) {
                values.push_back(v.cast<double>());
            }
        }
        
        if (values.empty()) {
            result["error"] = "No values provided";
            return result;
        }
        
        // Basic statistics
        double sum = std::accumulate(values.begin(), values.end(), 0.0);
        double mean = sum / values.size();
        
        double sq_sum = 0, min_val = values[0], max_val = values[0];
        for (double v : values) {
            sq_sum += (v - mean) * (v - mean);
            min_val = std::min(min_val, v);
            max_val = std::max(max_val, v);
        }
        double variance = sq_sum / values.size();
        double std_dev = std::sqrt(variance);
        
        result["count"] = static_cast<int>(values.size());
        result["sum"] = sum;
        result["mean"] = mean;
        result["min"] = min_val;
        result["max"] = max_val;
        result["variance"] = variance;
        result["std_dev"] = std_dev;
        result["range"] = max_val - min_val;
        
        // Percentiles
        if (compute_percentiles_ && !values.empty()) {
            std::vector<double> sorted = values;
            std::sort(sorted.begin(), sorted.end());
            
            py::dict percentiles;
            for (int p : percentile_values_) {
                double index = (p / 100.0) * (sorted.size() - 1);
                size_t lower = static_cast<size_t>(index);
                size_t upper = std::min(lower + 1, sorted.size() - 1);
                double frac = index - lower;
                double value = sorted[lower] * (1 - frac) + sorted[upper] * frac;
                percentiles[("p" + std::to_string(p)).c_str()] = value;
            }
            result["percentiles"] = percentiles;
        }
        
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["compute_percentiles"] = compute_percentiles_;
        py::list pvals;
        for (int p : percentile_values_) pvals.append(p);
        d["percentile_values"] = pvals;
        return d;
    }
    
protected:
    std::string get_type_name() const override {
        return "StatisticsCalculator";
    }
};


// ============================================================================
// MatrixCalculator
// ============================================================================

/**
 * Matrix operations calculator.
 * 
 * Configuration:
 * - operation: "multiply", "transpose", "determinant", "trace"
 */
class MatrixCalculator : public Calculator {
private:
    std::string operation_;
    
public:
    MatrixCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        operation_ = get_config<std::string>("operation", "multiply");
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        
        if (operation_ == "transpose" && data.contains("matrix")) {
            auto matrix = data["matrix"].cast<py::list>();
            py::list transposed;
            
            if (matrix.size() > 0) {
                auto first_row = matrix[0].cast<py::list>();
                size_t cols = first_row.size();
                size_t rows = matrix.size();
                
                for (size_t j = 0; j < cols; ++j) {
                    py::list new_row;
                    for (size_t i = 0; i < rows; ++i) {
                        auto row = matrix[i].cast<py::list>();
                        new_row.append(row[j]);
                    }
                    transposed.append(new_row);
                }
            }
            result["result"] = transposed;
        } else if (operation_ == "trace" && data.contains("matrix")) {
            auto matrix = data["matrix"].cast<py::list>();
            double trace = 0.0;
            for (size_t i = 0; i < matrix.size(); ++i) {
                auto row = matrix[i].cast<py::list>();
                if (i < row.size()) {
                    trace += row[i].cast<double>();
                }
            }
            result["trace"] = trace;
        } else if (operation_ == "determinant" && data.contains("matrix")) {
            auto matrix = data["matrix"].cast<py::list>();
            if (matrix.size() == 2) {
                auto row0 = matrix[0].cast<py::list>();
                auto row1 = matrix[1].cast<py::list>();
                double det = row0[0].cast<double>() * row1[1].cast<double>() -
                            row0[1].cast<double>() * row1[0].cast<double>();
                result["determinant"] = det;
            }
        }
        
        result["operation"] = operation_;
        return result;
    }
    
protected:
    std::string get_type_name() const override {
        return "MatrixCalculator";
    }
};


// ============================================================================
// StringTransformCalculator
// ============================================================================

/**
 * String transformation calculator.
 * 
 * Configuration:
 * - operation: "uppercase", "lowercase", "reverse", "length", "hash"
 * - field: Input field name (default: "text")
 */
class StringTransformCalculator : public Calculator {
private:
    std::string operation_;
    std::string field_;
    
public:
    StringTransformCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        operation_ = get_config<std::string>("operation", "uppercase");
        field_ = get_config<std::string>("field", "text");
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        
        if (!data.contains(field_.c_str())) {
            result["error"] = "Field not found: " + field_;
            return result;
        }
        
        std::string text = data[field_.c_str()].cast<std::string>();
        
        if (operation_ == "uppercase") {
            std::transform(text.begin(), text.end(), text.begin(), ::toupper);
            result["result"] = text;
        } else if (operation_ == "lowercase") {
            std::transform(text.begin(), text.end(), text.begin(), ::tolower);
            result["result"] = text;
        } else if (operation_ == "reverse") {
            std::reverse(text.begin(), text.end());
            result["result"] = text;
        } else if (operation_ == "length") {
            result["length"] = static_cast<int>(text.size());
        } else if (operation_ == "hash") {
            std::hash<std::string> hasher;
            result["hash"] = static_cast<int64_t>(hasher(text));
        }
        
        result["operation"] = operation_;
        return result;
    }
    
protected:
    std::string get_type_name() const override {
        return "StringTransformCalculator";
    }
};


// ============================================================================
// DataValidationCalculator
// ============================================================================

/**
 * Data validation calculator.
 * 
 * Configuration:
 * - strict_mode: Fail on first error (default: false)
 * - validate_types: Check data types (default: true)
 * - required_fields: List of required field names
 */
class DataValidationCalculator : public Calculator {
private:
    bool strict_mode_;
    bool validate_types_;
    std::vector<std::string> required_fields_;
    
public:
    DataValidationCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        strict_mode_ = get_config<bool>("strict_mode", false);
        validate_types_ = get_config<bool>("validate_types", true);
        
        if (config.contains("required_fields")) {
            auto fields = config["required_fields"].cast<py::list>();
            for (auto& f : fields) {
                required_fields_.push_back(f.cast<std::string>());
            }
        }
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        py::list errors;
        py::list warnings;
        bool is_valid = true;
        
        for (const auto& field : required_fields_) {
            if (!data.contains(field.c_str())) {
                errors.append("Missing required field: " + field);
                is_valid = false;
                if (strict_mode_) break;
            }
        }
        
        result["is_valid"] = is_valid;
        result["errors"] = errors;
        result["warnings"] = warnings;
        result["error_count"] = static_cast<int>(errors.size());
        result["field_count"] = static_cast<int>(py::len(data));
        
        return result;
    }
    
protected:
    std::string get_type_name() const override {
        return "DataValidationCalculator";
    }
};


// ============================================================================
// TradePricingCalculator
// ============================================================================

/**
 * Trade pricing calculator for financial applications.
 * 
 * Configuration:
 * - commission_rate: Commission rate (default: 0.001 = 0.1%)
 * - tax_rate: Tax rate (default: 0.0)
 * - include_fees: Whether to include fees (default: true)
 */
class TradePricingCalculator : public Calculator {
private:
    double commission_rate_;
    double tax_rate_;
    bool include_fees_;
    
public:
    TradePricingCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        commission_rate_ = get_config<double>("commission_rate", 0.001);
        tax_rate_ = get_config<double>("tax_rate", 0.0);
        include_fees_ = get_config<bool>("include_fees", true);
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        
        double price = data.contains("price") ? data["price"].cast<double>() : 0.0;
        double quantity = data.contains("quantity") ? data["quantity"].cast<double>() : 0.0;
        
        double gross_value = price * quantity;
        double commission = gross_value * commission_rate_;
        double tax = tax_rate_ > 0 ? gross_value * tax_rate_ : 0.0;
        double fees = include_fees_ ? gross_value * 0.0001 : 0.0;  // 0.01% fee
        double net_value = gross_value + commission + tax + fees;
        
        result["gross_value"] = gross_value;
        result["commission"] = commission;
        result["tax"] = tax;
        result["fees"] = fees;
        result["net_value"] = net_value;
        result["calculated_at"] = std::time(nullptr);
        
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["commission_rate"] = commission_rate_;
        d["tax_rate"] = tax_rate_;
        d["include_fees"] = include_fees_;
        return d;
    }
    
protected:
    std::string get_type_name() const override {
        return "TradePricingCalculator";
    }
};


// ============================================================================
// RiskMetricsCalculator
// ============================================================================

/**
 * Risk metrics calculator with VaR computation.
 * 
 * Configuration:
 * - var_confidence: VaR confidence level (default: 0.95)
 * - risk_free_rate: Risk-free rate (default: 0.02)
 */
class RiskMetricsCalculator : public Calculator {
private:
    double var_confidence_;
    double risk_free_rate_;
    
public:
    RiskMetricsCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        var_confidence_ = get_config<double>("var_confidence", 0.95);
        risk_free_rate_ = get_config<double>("risk_free_rate", 0.02);
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        
        double position_value = data.contains("position_value") ? 
                               data["position_value"].cast<double>() : 0.0;
        double volatility = data.contains("volatility") ? 
                           data["volatility"].cast<double>() : 0.0;
        
        // Z-score for confidence level
        double z_score = get_z_score(var_confidence_);
        
        // Daily VaR
        double daily_var = position_value * volatility * z_score / std::sqrt(252.0);
        result["daily_var"] = daily_var;
        result["var_10d"] = daily_var * std::sqrt(10);
        result["annual_var"] = daily_var * std::sqrt(252);
        
        // Greeks-based risk if available
        if (data.contains("delta") && data.contains("underlying_price")) {
            double delta = data["delta"].cast<double>();
            double underlying = data["underlying_price"].cast<double>();
            double price_move = underlying * 0.01;
            
            double delta_risk = delta * price_move;
            result["delta_risk"] = delta_risk;
            
            if (data.contains("gamma")) {
                double gamma = data["gamma"].cast<double>();
                double gamma_risk = 0.5 * gamma * price_move * price_move;
                result["gamma_risk"] = gamma_risk;
                result["total_greek_risk"] = delta_risk + gamma_risk;
            }
        }
        
        result["var_confidence"] = var_confidence_;
        result["risk_free_rate"] = risk_free_rate_;
        result["calculated_at"] = std::time(nullptr);
        
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["var_confidence"] = var_confidence_;
        d["risk_free_rate"] = risk_free_rate_;
        return d;
    }
    
private:
    double get_z_score(double confidence) const {
        if (confidence >= 0.99) return 2.326;
        if (confidence >= 0.95) return 1.645;
        if (confidence >= 0.90) return 1.282;
        return 1.0;
    }
    
protected:
    std::string get_type_name() const override {
        return "RiskMetricsCalculator";
    }
};


} // namespace dishtayantra


// ============================================================================
// pybind11 Module Definition
// ============================================================================

PYBIND11_MODULE(dishtayantra_cpp, m) {
    m.doc() = "DishtaYantra C++ Calculators v1.7.0 - High-performance native calculators";
    
    using namespace dishtayantra;
    
    // Base Calculator class
    py::class_<Calculator>(m, "Calculator")
        .def("calculate", &Calculator::calculate)
        .def("details", &Calculator::details);
    
    // PassthroughCalculator
    py::class_<PassthroughCalculator, Calculator>(m, "PassthroughCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &PassthroughCalculator::calculate)
        .def("details", &PassthroughCalculator::details);
    
    // MathCalculator
    py::class_<MathCalculator, Calculator>(m, "MathCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &MathCalculator::calculate)
        .def("details", &MathCalculator::details);
    
    // StatisticsCalculator
    py::class_<StatisticsCalculator, Calculator>(m, "StatisticsCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &StatisticsCalculator::calculate)
        .def("details", &StatisticsCalculator::details);
    
    // MatrixCalculator
    py::class_<MatrixCalculator, Calculator>(m, "MatrixCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &MatrixCalculator::calculate)
        .def("details", &MatrixCalculator::details);
    
    // StringTransformCalculator
    py::class_<StringTransformCalculator, Calculator>(m, "StringTransformCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &StringTransformCalculator::calculate)
        .def("details", &StringTransformCalculator::details);
    
    // DataValidationCalculator
    py::class_<DataValidationCalculator, Calculator>(m, "DataValidationCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &DataValidationCalculator::calculate)
        .def("details", &DataValidationCalculator::details);
    
    // TradePricingCalculator
    py::class_<TradePricingCalculator, Calculator>(m, "TradePricingCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &TradePricingCalculator::calculate)
        .def("details", &TradePricingCalculator::details);
    
    // RiskMetricsCalculator
    py::class_<RiskMetricsCalculator, Calculator>(m, "RiskMetricsCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &RiskMetricsCalculator::calculate)
        .def("details", &RiskMetricsCalculator::details);
    
    // Module-level functions
    m.def("get_version", []() { return "1.7.0"; });
    m.def("list_calculators", []() {
        return py::make_tuple(
            "PassthroughCalculator",
            "MathCalculator",
            "StatisticsCalculator",
            "MatrixCalculator",
            "StringTransformCalculator",
            "DataValidationCalculator",
            "TradePricingCalculator",
            "RiskMetricsCalculator"
        );
    });
}
