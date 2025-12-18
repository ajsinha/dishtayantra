/**
 * DishtaYantra Example C++ Calculators
 * ====================================
 * 
 * Example calculators demonstrating pybind11 integration.
 * 
 * v1.1.1: Added LMDB zero-copy data exchange support for large payloads
 * 
 * LMDB Zero-Copy Exchange:
 * When lmdb_enabled=true in DAG config, large payloads are exchanged via
 * memory-mapped LMDB files instead of Python dict serialization. The calculator
 * receives a reference dict with:
 *   - _lmdb_ref: true
 *   - _lmdb_input_key: Key to read input from LMDB
 *   - _lmdb_output_key: Key to write output to LMDB
 *   - _lmdb_db_path: Path to LMDB database
 *   - _lmdb_db_name: Named database to use
 *   - _lmdb_format: Data format (msgpack, json, numpy, arrow)
 * 
 * To enable LMDB in C++ calculators:
 *   1. Install liblmdb: apt-get install liblmdb-dev
 *   2. Compile with -DUSE_LMDB -llmdb
 *   3. Use LMDBHelper class for read/write operations
 * 
 * Build instructions:
 * 
 * Linux/macOS (without LMDB):
 *   g++ -O3 -Wall -shared -std=c++17 -fPIC \
 *       $(python3 -m pybind11 --includes) \
 *       dishtayantra_cpp.cpp \
 *       -o dishtayantra_cpp$(python3-config --extension-suffix)
 * 
 * Linux/macOS (with LMDB):
 *   g++ -O3 -Wall -shared -std=c++17 -fPIC -DUSE_LMDB \
 *       $(python3 -m pybind11 --includes) \
 *       dishtayantra_cpp.cpp \
 *       -o dishtayantra_cpp$(python3-config --extension-suffix) -llmdb
 * 
 * Windows (MSVC):
 *   cl /O2 /LD /EHsc /std:c++17 \
 *       /I<python-include> /I<pybind11-include> \
 *       dishtayantra_cpp.cpp \
 *       /link /LIBPATH:<python-libs> python3.lib \
 *       /OUT:dishtayantra_cpp.pyd
 * 
 * @author DishtaYantra
 * @version 1.1.1
 */

#include "calculator.hpp"
#include <cmath>
#include <vector>
#include <numeric>
#include <algorithm>

// Optional LMDB support for zero-copy data exchange
#ifdef USE_LMDB
#include <lmdb.h>

namespace dishtayantra {

/**
 * LMDB Helper for zero-copy data exchange with Python DAG engine.
 * 
 * Usage:
 *   LMDBHelper lmdb;
 *   if (lmdb.open(db_path, db_name)) {
 *       auto data = lmdb.get(input_key);
 *       // Process data...
 *       lmdb.put(output_key, result_data);
 *   }
 */
class LMDBHelper {
public:
    LMDBHelper() : env_(nullptr), dbi_(0), opened_(false) {}
    
    ~LMDBHelper() { close(); }
    
    bool open(const std::string& db_path, const std::string& db_name = "default") {
        if (opened_) close();
        
        int rc = mdb_env_create(&env_);
        if (rc != 0) return false;
        
        mdb_env_set_mapsize(env_, 1UL * 1024UL * 1024UL * 1024UL);  // 1GB
        mdb_env_set_maxdbs(env_, 100);
        
        // Open in read-write mode
        rc = mdb_env_open(env_, db_path.c_str(), 0, 0664);
        if (rc != 0) { mdb_env_close(env_); return false; }
        
        MDB_txn* txn;
        rc = mdb_txn_begin(env_, nullptr, 0, &txn);
        if (rc != 0) { mdb_env_close(env_); return false; }
        
        rc = mdb_dbi_open(txn, db_name.c_str(), MDB_CREATE, &dbi_);
        if (rc != 0) { mdb_txn_abort(txn); mdb_env_close(env_); return false; }
        
        mdb_txn_commit(txn);
        opened_ = true;
        return true;
    }
    
    void close() {
        if (opened_) {
            mdb_dbi_close(env_, dbi_);
            mdb_env_close(env_);
            opened_ = false;
        }
    }
    
    bool is_open() const { return opened_; }
    
    /**
     * Read data from LMDB by key.
     * Returns empty vector if key not found.
     */
    std::vector<uint8_t> get(const std::string& key) {
        if (!opened_) return {};
        
        MDB_txn* txn;
        if (mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn) != 0) return {};
        
        MDB_val mdb_key, mdb_data;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = const_cast<char*>(key.data());
        
        int rc = mdb_get(txn, dbi_, &mdb_key, &mdb_data);
        if (rc != 0) {
            mdb_txn_abort(txn);
            return {};
        }
        
        std::vector<uint8_t> result(
            static_cast<uint8_t*>(mdb_data.mv_data),
            static_cast<uint8_t*>(mdb_data.mv_data) + mdb_data.mv_size
        );
        mdb_txn_abort(txn);
        return result;
    }
    
    /**
     * Write data to LMDB.
     * Returns true on success.
     */
    bool put(const std::string& key, const std::vector<uint8_t>& data) {
        if (!opened_) return false;
        
        MDB_txn* txn;
        if (mdb_txn_begin(env_, nullptr, 0, &txn) != 0) return false;
        
        MDB_val mdb_key, mdb_data;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = const_cast<char*>(key.data());
        mdb_data.mv_size = data.size();
        mdb_data.mv_data = const_cast<uint8_t*>(data.data());
        
        if (mdb_put(txn, dbi_, &mdb_key, &mdb_data, 0) != 0) {
            mdb_txn_abort(txn);
            return false;
        }
        
        return mdb_txn_commit(txn) == 0;
    }
    
    /**
     * Get raw pointer to memory-mapped data (zero-copy read).
     * WARNING: Pointer is only valid while LMDB environment is open.
     */
    std::pair<const uint8_t*, size_t> get_raw(const std::string& key) {
        if (!opened_) return {nullptr, 0};
        
        MDB_txn* txn;
        if (mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn) != 0) return {nullptr, 0};
        
        MDB_val mdb_key, mdb_data;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = const_cast<char*>(key.data());
        
        if (mdb_get(txn, dbi_, &mdb_key, &mdb_data) != 0) {
            mdb_txn_abort(txn);
            return {nullptr, 0};
        }
        
        // Note: We don't abort the transaction, so data remains valid
        // Caller must ensure to not use pointer after LMDB closes
        return {static_cast<const uint8_t*>(mdb_data.mv_data), mdb_data.mv_size};
    }
    
private:
    MDB_env* env_;
    MDB_dbi dbi_;
    bool opened_;
};

} // namespace dishtayantra
#endif // USE_LMDB

namespace dishtayantra {

/**
 * Passthrough calculator - returns input unchanged.
 * Useful for testing connectivity.
 */
class PassthruCalculator : public Calculator {
public:
    using Calculator::Calculator;
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        return py::dict(data);  // Return a copy
    }
    
protected:
    std::string get_type_name() const override {
        return "PassthruCalculator";
    }
};


/**
 * Mathematical operations calculator.
 * 
 * Configuration:
 * - operation: "add", "sum", "multiply", "mul", "max", "min", "mean", "avg", "std"
 * - arguments: List of field names to operate on
 * - output_attribute: Name of output field (default: "result")
 */
class MathCalculator : public Calculator {
private:
    std::string operation_;
    std::vector<std::string> arguments_;
    std::string output_attr_;
    
public:
    MathCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        operation_ = get_config<std::string>("operation", "add");
        output_attr_ = get_config<std::string>("output_attribute", "result");
        
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
        
        // Collect numeric values
        std::vector<double> values;
        for (const auto& arg : arguments_) {
            if (data.contains(arg.c_str())) {
                try {
                    values.push_back(data[arg.c_str()].cast<double>());
                } catch (...) {
                    // Skip non-numeric values
                }
            }
        }
        
        if (values.empty()) {
            result[output_attr_.c_str()] = 0.0;
            return result;
        }
        
        double output = values[0];
        
        if (operation_ == "add" || operation_ == "sum") {
            output = std::accumulate(values.begin(), values.end(), 0.0);
        } else if (operation_ == "multiply" || operation_ == "mul") {
            output = std::accumulate(values.begin(), values.end(), 1.0,
                                     std::multiplies<double>());
        } else if (operation_ == "max") {
            output = *std::max_element(values.begin(), values.end());
        } else if (operation_ == "min") {
            output = *std::min_element(values.begin(), values.end());
        } else if (operation_ == "mean" || operation_ == "avg") {
            output = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
        } else if (operation_ == "std") {
            double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
            double sq_sum = 0;
            for (double v : values) {
                sq_sum += (v - mean) * (v - mean);
            }
            output = std::sqrt(sq_sum / values.size());
        }
        
        result[output_attr_.c_str()] = output;
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["operation"] = operation_;
        d["output_attribute"] = output_attr_;
        d["num_arguments"] = arguments_.size();
        return d;
    }
    
protected:
    std::string get_type_name() const override {
        return "MathCalculator";
    }
};


/**
 * Trade pricing calculator for financial applications.
 * 
 * Configuration:
 * - commission_rate: Commission rate (default: 0.001 = 0.1%)
 * - tax_rate: Tax rate (default: 0.0)
 * - include_vat: Whether to add VAT (default: false)
 * 
 * Required input fields:
 * - price: Trade price
 * - quantity: Trade quantity
 * 
 * Output fields:
 * - gross_value, commission, tax, vat, net_value, calculated_at
 */
class TradePricingCalculator : public Calculator {
private:
    double commission_rate_;
    double tax_rate_;
    bool include_vat_;
    
public:
    TradePricingCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        commission_rate_ = get_config<double>("commission_rate", 0.001);
        tax_rate_ = get_config<double>("tax_rate", 0.0);
        include_vat_ = get_config<bool>("include_vat", false);
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        
        // Get trade values
        double price = data["price"].cast<double>();
        double quantity = data["quantity"].cast<double>();
        
        // Calculate
        double gross_value = price * quantity;
        double commission = gross_value * commission_rate_;
        double tax = tax_rate_ > 0 ? gross_value * tax_rate_ : 0.0;
        double vat = include_vat_ ? (gross_value + commission) * 0.20 : 0.0;
        double net_value = gross_value + commission + tax + vat;
        
        // Set results
        result["gross_value"] = gross_value;
        result["commission"] = commission;
        result["tax"] = tax;
        result["vat"] = vat;
        result["net_value"] = net_value;
        result["calculated_at"] = std::time(nullptr);
        
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["commission_rate"] = commission_rate_;
        d["tax_rate"] = tax_rate_;
        d["include_vat"] = include_vat_;
        return d;
    }
    
protected:
    std::string get_type_name() const override {
        return "TradePricingCalculator";
    }
};


/**
 * Risk calculator with VaR computation.
 * 
 * Configuration:
 * - confidence_level: VaR confidence level (default: 0.95)
 * - lookback_period: Days for calculation (default: 252)
 * 
 * Required input fields:
 * - position_value: Position value
 * - volatility: Annualized volatility
 * 
 * Optional input fields:
 * - delta, gamma, underlying_price (for Greeks-based risk)
 */
class RiskCalculator : public Calculator {
private:
    double confidence_level_;
    int lookback_period_;
    
public:
    RiskCalculator(const std::string& name, const py::dict& config)
        : Calculator(name, config) {
        confidence_level_ = get_config<double>("confidence_level", 0.95);
        lookback_period_ = get_config<int>("lookback_period", 252);
    }
    
    py::dict calculate(const py::dict& data) override {
        update_stats();
        
        py::dict result(data);
        
        double position_value = data["position_value"].cast<double>();
        double volatility = data["volatility"].cast<double>();
        
        // Z-score for confidence level
        double z_score = get_z_score(confidence_level_);
        
        // Daily VaR
        double daily_var = position_value * volatility * z_score / std::sqrt(lookback_period_);
        result["daily_var"] = daily_var;
        
        // 10-day VaR
        result["var_10d"] = daily_var * std::sqrt(10);
        
        // Annual VaR
        result["annual_var"] = daily_var * std::sqrt(252);
        
        // Greeks-based risk if available
        if (data.contains("delta") && data.contains("underlying_price")) {
            double delta = data["delta"].cast<double>();
            double underlying = data["underlying_price"].cast<double>();
            double price_move = underlying * 0.01;  // 1% move
            
            double delta_risk = delta * price_move;
            result["delta_risk"] = delta_risk;
            
            if (data.contains("gamma")) {
                double gamma = data["gamma"].cast<double>();
                double gamma_risk = 0.5 * gamma * price_move * price_move;
                result["gamma_risk"] = gamma_risk;
                result["total_greek_risk"] = delta_risk + gamma_risk;
            }
        }
        
        result["calculated_at"] = std::time(nullptr);
        return result;
    }
    
    py::dict details() override {
        py::dict d = Calculator::details();
        d["confidence_level"] = confidence_level_;
        d["lookback_period"] = lookback_period_;
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
        return "RiskCalculator";
    }
};

} // namespace dishtayantra


// ============================================================================
// pybind11 Module Definition
// ============================================================================

PYBIND11_MODULE(dishtayantra_cpp, m) {
    m.doc() = "DishtaYantra C++ Calculators - High-performance native calculators";
    
    using namespace dishtayantra;
    
    // Base Calculator class (for type checking)
    py::class_<Calculator>(m, "Calculator")
        .def("calculate", &Calculator::calculate)
        .def("details", &Calculator::details);
    
    // PassthruCalculator
    py::class_<PassthruCalculator, Calculator>(m, "PassthruCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &PassthruCalculator::calculate)
        .def("details", &PassthruCalculator::details);
    
    // MathCalculator
    py::class_<MathCalculator, Calculator>(m, "MathCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &MathCalculator::calculate)
        .def("details", &MathCalculator::details);
    
    // TradePricingCalculator
    py::class_<TradePricingCalculator, Calculator>(m, "TradePricingCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &TradePricingCalculator::calculate)
        .def("details", &TradePricingCalculator::details);
    
    // RiskCalculator
    py::class_<RiskCalculator, Calculator>(m, "RiskCalculator")
        .def(py::init<const std::string&, const py::dict&>(),
             py::arg("name"), py::arg("config"))
        .def("calculate", &RiskCalculator::calculate)
        .def("details", &RiskCalculator::details);
}
