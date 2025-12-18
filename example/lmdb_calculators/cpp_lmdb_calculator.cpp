/**
 * LMDB-Enabled C++ Calculator Example
 * 
 * This example demonstrates how to use LMDB zero-copy data exchange
 * in a C++ calculator for high-performance large payload processing.
 * 
 * Build Instructions:
 *   # Install LMDB
 *   sudo apt-get install liblmdb-dev  # Ubuntu/Debian
 *   brew install lmdb                  # macOS
 * 
 *   # Compile with pybind11
 *   c++ -O3 -Wall -shared -std=c++17 -fPIC \
 *       $(python3 -m pybind11 --includes) \
 *       cpp_lmdb_calculator.cpp -o lmdb_calculator$(python3-config --extension-suffix) \
 *       -llmdb
 * 
 * Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <lmdb.h>
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <cstring>
#include <cmath>
#include <stdexcept>
#include <iostream>
#include <sstream>

namespace py = pybind11;

/**
 * LMDB Helper class for zero-copy data access
 */
class LMDBHelper {
private:
    MDB_env* env = nullptr;
    MDB_dbi dbi = 0;
    std::string db_path;
    bool initialized = false;
    
    // Configuration
    size_t map_size = 1UL * 1024 * 1024 * 1024;  // 1GB
    unsigned int max_dbs = 100;
    
public:
    LMDBHelper() = default;
    
    ~LMDBHelper() {
        close();
    }
    
    /**
     * Initialize LMDB environment
     */
    bool init(const std::string& path) {
        if (initialized && path == db_path) {
            return true;
        }
        
        close();  // Close existing if any
        
        int rc;
        
        // Create environment
        rc = mdb_env_create(&env);
        if (rc != 0) {
            throw std::runtime_error("Failed to create LMDB env: " + std::string(mdb_strerror(rc)));
        }
        
        // Set map size
        rc = mdb_env_set_mapsize(env, map_size);
        if (rc != 0) {
            throw std::runtime_error("Failed to set map size: " + std::string(mdb_strerror(rc)));
        }
        
        // Set max databases
        rc = mdb_env_set_maxdbs(env, max_dbs);
        if (rc != 0) {
            throw std::runtime_error("Failed to set max dbs: " + std::string(mdb_strerror(rc)));
        }
        
        // Open environment
        rc = mdb_env_open(env, path.c_str(), MDB_WRITEMAP | MDB_NOSYNC, 0664);
        if (rc != 0) {
            throw std::runtime_error("Failed to open LMDB: " + std::string(mdb_strerror(rc)));
        }
        
        // Open database
        MDB_txn* txn;
        rc = mdb_txn_begin(env, nullptr, 0, &txn);
        if (rc != 0) {
            throw std::runtime_error("Failed to begin txn: " + std::string(mdb_strerror(rc)));
        }
        
        rc = mdb_dbi_open(txn, "default", MDB_CREATE, &dbi);
        if (rc != 0) {
            mdb_txn_abort(txn);
            throw std::runtime_error("Failed to open dbi: " + std::string(mdb_strerror(rc)));
        }
        
        mdb_txn_commit(txn);
        
        db_path = path;
        initialized = true;
        return true;
    }
    
    /**
     * Read data from LMDB (returns copy)
     */
    std::vector<uint8_t> get(const std::string& key) {
        if (!initialized) {
            throw std::runtime_error("LMDB not initialized");
        }
        
        MDB_txn* txn;
        int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        if (rc != 0) {
            throw std::runtime_error("Failed to begin read txn");
        }
        
        MDB_val mdb_key, mdb_data;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = (void*)key.data();
        
        rc = mdb_get(txn, dbi, &mdb_key, &mdb_data);
        
        std::vector<uint8_t> result;
        if (rc == 0) {
            result.assign((uint8_t*)mdb_data.mv_data, 
                         (uint8_t*)mdb_data.mv_data + mdb_data.mv_size);
        }
        
        mdb_txn_abort(txn);
        return result;
    }
    
    /**
     * Get raw pointer to data (ZERO-COPY!)
     * WARNING: Pointer is only valid while transaction is open
     */
    std::pair<const uint8_t*, size_t> get_raw(const std::string& key, MDB_txn*& txn_out) {
        if (!initialized) {
            throw std::runtime_error("LMDB not initialized");
        }
        
        int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn_out);
        if (rc != 0) {
            throw std::runtime_error("Failed to begin read txn");
        }
        
        MDB_val mdb_key, mdb_data;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = (void*)key.data();
        
        rc = mdb_get(txn_out, dbi, &mdb_key, &mdb_data);
        
        if (rc == 0) {
            // ZERO-COPY: Return pointer directly into memory-mapped file!
            return {(const uint8_t*)mdb_data.mv_data, mdb_data.mv_size};
        }
        
        mdb_txn_abort(txn_out);
        txn_out = nullptr;
        return {nullptr, 0};
    }
    
    void release_txn(MDB_txn* txn) {
        if (txn) {
            mdb_txn_abort(txn);
        }
    }
    
    /**
     * Write data to LMDB
     */
    bool put(const std::string& key, const std::vector<uint8_t>& data) {
        if (!initialized) {
            throw std::runtime_error("LMDB not initialized");
        }
        
        MDB_txn* txn;
        int rc = mdb_txn_begin(env, nullptr, 0, &txn);
        if (rc != 0) {
            throw std::runtime_error("Failed to begin write txn");
        }
        
        MDB_val mdb_key, mdb_data;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = (void*)key.data();
        mdb_data.mv_size = data.size();
        mdb_data.mv_data = (void*)data.data();
        
        rc = mdb_put(txn, dbi, &mdb_key, &mdb_data, 0);
        if (rc != 0) {
            mdb_txn_abort(txn);
            throw std::runtime_error("Failed to put data: " + std::string(mdb_strerror(rc)));
        }
        
        return mdb_txn_commit(txn) == 0;
    }
    
    /**
     * Delete key from LMDB
     */
    bool del(const std::string& key) {
        if (!initialized) {
            throw std::runtime_error("LMDB not initialized");
        }
        
        MDB_txn* txn;
        int rc = mdb_txn_begin(env, nullptr, 0, &txn);
        if (rc != 0) {
            return false;
        }
        
        MDB_val mdb_key;
        mdb_key.mv_size = key.size();
        mdb_key.mv_data = (void*)key.data();
        
        rc = mdb_del(txn, dbi, &mdb_key, nullptr);
        if (rc != 0) {
            mdb_txn_abort(txn);
            return false;
        }
        
        return mdb_txn_commit(txn) == 0;
    }
    
    void close() {
        if (dbi) {
            mdb_dbi_close(env, dbi);
            dbi = 0;
        }
        if (env) {
            mdb_env_close(env);
            env = nullptr;
        }
        initialized = false;
    }
    
    bool is_initialized() const { return initialized; }
    std::string get_path() const { return db_path; }
};


/**
 * Simple JSON-like serialization for demo purposes
 * In production, use nlohmann/json or msgpack-c
 */
class SimpleSerializer {
public:
    // Serialize map to bytes (simplified JSON)
    static std::vector<uint8_t> serialize(const std::map<std::string, py::object>& data) {
        std::stringstream ss;
        ss << "{";
        bool first = true;
        for (const auto& [key, value] : data) {
            if (!first) ss << ",";
            ss << "\"" << key << "\":";
            serialize_value(ss, value);
            first = false;
        }
        ss << "}";
        std::string str = ss.str();
        return std::vector<uint8_t>(str.begin(), str.end());
    }
    
private:
    static void serialize_value(std::stringstream& ss, const py::object& obj) {
        if (py::isinstance<py::none>(obj)) {
            ss << "null";
        } else if (py::isinstance<py::bool_>(obj)) {
            ss << (obj.cast<bool>() ? "true" : "false");
        } else if (py::isinstance<py::int_>(obj)) {
            ss << obj.cast<long long>();
        } else if (py::isinstance<py::float_>(obj)) {
            ss << obj.cast<double>();
        } else if (py::isinstance<py::str>(obj)) {
            ss << "\"" << obj.cast<std::string>() << "\"";
        } else if (py::isinstance<py::list>(obj)) {
            ss << "[";
            auto list = obj.cast<py::list>();
            for (size_t i = 0; i < list.size(); i++) {
                if (i > 0) ss << ",";
                serialize_value(ss, list[i]);
            }
            ss << "]";
        } else if (py::isinstance<py::dict>(obj)) {
            ss << "{";
            auto dict = obj.cast<py::dict>();
            bool first = true;
            for (auto& item : dict) {
                if (!first) ss << ",";
                ss << "\"" << py::str(item.first).cast<std::string>() << "\":";
                serialize_value(ss, py::reinterpret_borrow<py::object>(item.second));
                first = false;
            }
            ss << "}";
        }
    }
};


/**
 * Base class for LMDB-enabled C++ calculators
 */
class LMDBEnabledCalculator {
protected:
    LMDBHelper lmdb;
    std::string name;
    
    // Statistics
    uint64_t calculations = 0;
    uint64_t lmdb_exchanges = 0;
    uint64_t total_time_ns = 0;
    
public:
    LMDBEnabledCalculator(const std::string& calc_name = "CppCalculator") 
        : name(calc_name) {}
    
    virtual ~LMDBEnabledCalculator() = default;
    
    /**
     * Initialize LMDB for this calculator
     */
    bool init_lmdb(const std::string& db_path) {
        return lmdb.init(db_path);
    }
    
    /**
     * Main calculation entry point
     */
    py::dict calculate(py::dict data) {
        auto start = std::chrono::high_resolution_clock::now();
        calculations++;
        
        py::dict result;
        
        // Check if this is an LMDB reference
        if (is_lmdb_reference(data)) {
            result = calculate_with_lmdb(data);
        } else {
            result = process_data(data);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        total_time_ns += elapsed_ns;
        
        result["_processing_time_ns"] = elapsed_ns;
        
        return result;
    }
    
    /**
     * Get calculator statistics
     */
    py::dict get_stats() {
        py::dict stats;
        stats["name"] = name;
        stats["calculations"] = calculations;
        stats["lmdb_exchanges"] = lmdb_exchanges;
        stats["total_time_ns"] = total_time_ns;
        stats["avg_time_us"] = calculations > 0 ? (total_time_ns / calculations) / 1000.0 : 0;
        stats["lmdb_initialized"] = lmdb.is_initialized();
        stats["db_path"] = lmdb.get_path();
        return stats;
    }
    
protected:
    /**
     * Check if data is an LMDB reference
     */
    bool is_lmdb_reference(py::dict data) {
        if (data.contains("_lmdb_ref")) {
            return data["_lmdb_ref"].cast<bool>();
        }
        return false;
    }
    
    /**
     * Process via LMDB zero-copy exchange
     */
    py::dict calculate_with_lmdb(py::dict ref_data) {
        lmdb_exchanges++;
        
        std::string input_key = ref_data["_lmdb_input_key"].cast<std::string>();
        std::string output_key = ref_data.contains("_lmdb_output_key") ? 
            ref_data["_lmdb_output_key"].cast<std::string>() : "";
        std::string db_path = ref_data["_lmdb_db_path"].cast<std::string>();
        
        // Initialize LMDB if needed
        if (!lmdb.is_initialized() || lmdb.get_path() != db_path) {
            lmdb.init(db_path);
        }
        
        // ZERO-COPY: Get raw pointer to data in memory-mapped file
        MDB_txn* txn = nullptr;
        auto [data_ptr, data_size] = lmdb.get_raw(input_key, txn);
        
        if (!data_ptr) {
            throw std::runtime_error("No data found at LMDB key: " + input_key);
        }
        
        // Process the raw data directly (zero-copy!)
        py::dict result = process_raw_data(data_ptr, data_size);
        
        // Release read transaction
        lmdb.release_txn(txn);
        
        // Write result to LMDB if output key specified
        if (!output_key.empty()) {
            std::map<std::string, py::object> result_map;
            for (auto& item : result) {
                result_map[py::str(item.first).cast<std::string>()] = 
                    py::reinterpret_borrow<py::object>(item.second);
            }
            auto serialized = SimpleSerializer::serialize(result_map);
            lmdb.put(output_key, serialized);
        }
        
        result["_exchange_type"] = "lmdb";
        return result;
    }
    
    /**
     * Process raw bytes from LMDB (override in subclass)
     */
    virtual py::dict process_raw_data(const uint8_t* data, size_t size) {
        // Default: convert bytes to string and create simple result
        std::string str_data(reinterpret_cast<const char*>(data), size);
        py::dict result;
        result["raw_size"] = size;
        result["processed"] = true;
        return result;
    }
    
    /**
     * Process Python dict data (override in subclass)
     */
    virtual py::dict process_data(py::dict data) = 0;
};


/**
 * Example: Matrix Processor Calculator
 */
class MatrixProcessorCalculator : public LMDBEnabledCalculator {
public:
    MatrixProcessorCalculator() : LMDBEnabledCalculator("MatrixProcessor") {}
    
protected:
    py::dict process_data(py::dict data) override {
        py::dict result;
        
        // Get matrix and operation
        auto matrix = data.contains("matrix") ? 
            data["matrix"].cast<std::vector<std::vector<double>>>() : 
            std::vector<std::vector<double>>();
        
        std::string operation = data.contains("operation") ? 
            data["operation"].cast<std::string>() : "sum";
        
        result["input_rows"] = matrix.size();
        result["input_cols"] = matrix.empty() ? 0 : matrix[0].size();
        result["operation"] = operation;
        
        if (!matrix.empty()) {
            if (operation == "sum") {
                double sum = 0;
                for (const auto& row : matrix) {
                    for (double val : row) {
                        sum += val;
                    }
                }
                result["result"] = sum;
            } else if (operation == "mean") {
                double sum = 0;
                size_t count = 0;
                for (const auto& row : matrix) {
                    for (double val : row) {
                        sum += val;
                        count++;
                    }
                }
                result["result"] = count > 0 ? sum / count : 0;
            } else if (operation == "max") {
                double max_val = matrix[0][0];
                for (const auto& row : matrix) {
                    for (double val : row) {
                        if (val > max_val) max_val = val;
                    }
                }
                result["result"] = max_val;
            }
        }
        
        result["processed_at"] = std::chrono::system_clock::now().time_since_epoch().count();
        return result;
    }
    
    py::dict process_raw_data(const uint8_t* data, size_t size) override {
        // For raw data, we can use SIMD operations directly on the memory-mapped buffer!
        py::dict result;
        result["raw_size"] = size;
        result["zero_copy"] = true;
        
        // Example: If data is array of doubles, process directly
        if (size % sizeof(double) == 0) {
            const double* doubles = reinterpret_cast<const double*>(data);
            size_t count = size / sizeof(double);
            
            double sum = 0;
            // This could use SIMD intrinsics for maximum performance
            for (size_t i = 0; i < count; i++) {
                sum += doubles[i];
            }
            
            result["sum"] = sum;
            result["count"] = count;
        }
        
        return result;
    }
};


/**
 * Example: Risk Calculator
 */
class RiskCalculator : public LMDBEnabledCalculator {
public:
    RiskCalculator() : LMDBEnabledCalculator("RiskCalculator") {}
    
protected:
    py::dict process_data(py::dict data) override {
        py::dict result;
        
        double total_exposure = 0;
        int position_count = 0;
        
        if (data.contains("positions")) {
            auto positions = data["positions"].cast<py::list>();
            position_count = positions.size();
            
            for (auto& pos : positions) {
                auto pos_dict = pos.cast<py::dict>();
                double qty = pos_dict.contains("quantity") ? 
                    pos_dict["quantity"].cast<double>() : 0;
                double price = pos_dict.contains("price") ? 
                    pos_dict["price"].cast<double>() : 0;
                total_exposure += qty * price;
            }
        }
        
        result["total_positions"] = position_count;
        result["total_exposure"] = total_exposure;
        result["var_95"] = total_exposure * 0.025;
        result["var_99"] = total_exposure * 0.035;
        result["expected_shortfall"] = total_exposure * 0.04;
        
        return result;
    }
};


/**
 * Example: Signal Processor (using SIMD-friendly operations)
 */
class SignalProcessorCalculator : public LMDBEnabledCalculator {
public:
    SignalProcessorCalculator() : LMDBEnabledCalculator("SignalProcessor") {}
    
protected:
    py::dict process_data(py::dict data) override {
        py::dict result;
        
        auto signal = data.contains("signal") ? 
            data["signal"].cast<std::vector<double>>() : 
            std::vector<double>();
        
        if (!signal.empty()) {
            // Calculate signal statistics (SIMD-friendly operations)
            double sum = 0, sq_sum = 0;
            double min_val = signal[0], max_val = signal[0];
            
            for (double val : signal) {
                sum += val;
                sq_sum += val * val;
                if (val < min_val) min_val = val;
                if (val > max_val) max_val = val;
            }
            
            double mean = sum / signal.size();
            double variance = (sq_sum / signal.size()) - (mean * mean);
            
            result["sample_count"] = signal.size();
            result["mean"] = mean;
            result["variance"] = variance;
            result["std_dev"] = std::sqrt(variance);
            result["min"] = min_val;
            result["max"] = max_val;
            result["range"] = max_val - min_val;
        }
        
        return result;
    }
    
    py::dict process_raw_data(const uint8_t* data, size_t size) override {
        // Process raw signal data with zero-copy
        py::dict result;
        
        if (size % sizeof(double) == 0) {
            const double* signal = reinterpret_cast<const double*>(data);
            size_t count = size / sizeof(double);
            
            if (count > 0) {
                double sum = 0, sq_sum = 0;
                double min_val = signal[0], max_val = signal[0];
                
                // SIMD-friendly loop
                for (size_t i = 0; i < count; i++) {
                    double val = signal[i];
                    sum += val;
                    sq_sum += val * val;
                    if (val < min_val) min_val = val;
                    if (val > max_val) max_val = val;
                }
                
                double mean = sum / count;
                double variance = (sq_sum / count) - (mean * mean);
                
                result["sample_count"] = count;
                result["mean"] = mean;
                result["variance"] = variance;
                result["std_dev"] = std::sqrt(variance);
                result["min"] = min_val;
                result["max"] = max_val;
                result["zero_copy"] = true;
            }
        }
        
        return result;
    }
};


// =============================================================================
// Python Module Definition
// =============================================================================

PYBIND11_MODULE(lmdb_calculator, m) {
    m.doc() = "LMDB-enabled C++ calculators for DishtaYantra";
    
    // LMDBHelper class
    py::class_<LMDBHelper>(m, "LMDBHelper")
        .def(py::init<>())
        .def("init", &LMDBHelper::init)
        .def("get", &LMDBHelper::get)
        .def("put", &LMDBHelper::put)
        .def("del", &LMDBHelper::del)
        .def("close", &LMDBHelper::close)
        .def("is_initialized", &LMDBHelper::is_initialized)
        .def("get_path", &LMDBHelper::get_path);
    
    // MatrixProcessorCalculator
    py::class_<MatrixProcessorCalculator>(m, "MatrixProcessorCalculator")
        .def(py::init<>())
        .def("init_lmdb", &MatrixProcessorCalculator::init_lmdb)
        .def("calculate", &MatrixProcessorCalculator::calculate)
        .def("get_stats", &MatrixProcessorCalculator::get_stats);
    
    // RiskCalculator
    py::class_<RiskCalculator>(m, "RiskCalculator")
        .def(py::init<>())
        .def("init_lmdb", &RiskCalculator::init_lmdb)
        .def("calculate", &RiskCalculator::calculate)
        .def("get_stats", &RiskCalculator::get_stats);
    
    // SignalProcessorCalculator
    py::class_<SignalProcessorCalculator>(m, "SignalProcessorCalculator")
        .def(py::init<>())
        .def("init_lmdb", &SignalProcessorCalculator::init_lmdb)
        .def("calculate", &SignalProcessorCalculator::calculate)
        .def("get_stats", &SignalProcessorCalculator::get_stats);
}
