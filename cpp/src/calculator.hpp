/**
 * DishtaYantra C++ Calculator Framework
 * =====================================
 * 
 * This header provides the base infrastructure for creating C++ calculators
 * that integrate with DishtaYantra via pybind11.
 * 
 * All calculators must implement the same interface as Python's DataCalculator:
 * - Constructor: ClassName(std::string name, py::dict config)
 * - Method: py::dict calculate(py::dict data)
 * - Method: py::dict details()
 * 
 * @author DishtaYantra
 * @version 1.1.0
 */

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <map>
#include <chrono>
#include <ctime>
#include <atomic>
#include <mutex>

namespace py = pybind11;

namespace dishtayantra {

/**
 * Abstract base class for C++ calculators.
 * 
 * Provides common functionality like:
 * - Configuration management
 * - Calculation counting
 * - Timing statistics
 * 
 * Example usage:
 * @code
 * class MyCalculator : public Calculator {
 * public:
 *     using Calculator::Calculator;
 *     
 *     py::dict calculate(const py::dict& data) override {
 *         update_stats();
 *         py::dict result(data);
 *         // Your calculation logic
 *         return result;
 *     }
 * };
 * @endcode
 */
class Calculator {
protected:
    std::string name_;
    py::dict config_;
    std::atomic<uint64_t> calculation_count_{0};
    std::string last_calculation_;
    std::mutex stats_mutex_;
    
public:
    /**
     * Constructor - same signature as Python DataCalculator.
     * 
     * @param name Calculator name
     * @param config Configuration dictionary from Python
     */
    Calculator(const std::string& name, const py::dict& config)
        : name_(name), config_(config) {}
    
    virtual ~Calculator() = default;
    
    /**
     * Main calculation method - must be implemented by subclasses.
     * 
     * @param data Input data dictionary
     * @return Output data dictionary
     */
    virtual py::dict calculate(const py::dict& data) = 0;
    
    /**
     * Get calculator details and statistics.
     * 
     * @return Dictionary with name, type, calculation count, etc.
     */
    virtual py::dict details() {
        py::dict d;
        d["name"] = name_;
        d["type"] = get_type_name();
        d["language"] = "C++";
        d["binding"] = "pybind11";
        d["calculation_count"] = calculation_count_.load();
        
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            if (!last_calculation_.empty()) {
                d["last_calculation"] = last_calculation_;
            }
        }
        
        return d;
    }
    
protected:
    /**
     * Update statistics after each calculation.
     * Call this at the start of your calculate() method.
     */
    void update_stats() {
        calculation_count_++;
        
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        
        std::lock_guard<std::mutex> lock(stats_mutex_);
        last_calculation_ = std::ctime(&time_t);
        // Remove trailing newline
        if (!last_calculation_.empty() && last_calculation_.back() == '\n') {
            last_calculation_.pop_back();
        }
    }
    
    /**
     * Get a configuration value with default.
     * 
     * @tparam T Type of the configuration value
     * @param key Configuration key
     * @param default_value Default if key not found
     * @return Configuration value or default
     */
    template<typename T>
    T get_config(const std::string& key, const T& default_value) const {
        if (config_.contains(key)) {
            try {
                return config_[key.c_str()].cast<T>();
            } catch (...) {
                return default_value;
            }
        }
        return default_value;
    }
    
    /**
     * Get the type name for details().
     * Override in subclasses to provide custom names.
     */
    virtual std::string get_type_name() const {
        return "Calculator";
    }
};

} // namespace dishtayantra
