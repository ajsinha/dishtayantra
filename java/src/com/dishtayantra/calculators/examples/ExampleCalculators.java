package com.dishtayantra.calculators.examples;

import com.dishtayantra.calculators.AbstractCalculator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * Example calculators demonstrating Py4J integration with DishtaYantra.
 * 
 * These calculators show how to implement high-performance Java calculations
 * that can be invoked from Python via Py4J.
 * 
 * @author DishtaYantra
 * @version 1.1.0
 */
public class ExampleCalculators {
    
    /**
     * Simple passthrough calculator - returns input as-is.
     * Useful for testing connectivity.
     */
    public static class PassthruCalculator extends AbstractCalculator {
        
        public PassthruCalculator(String name, Map<String, Object> config) {
            super(name, config);
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            return new HashMap<>(data);
        }
    }
    
    /**
     * High-performance mathematical calculator.
     * Demonstrates CPU-intensive calculations that benefit from Java's speed.
     */
    public static class MathCalculator extends AbstractCalculator {
        
        private final String operation;
        private final List<String> arguments;
        private final String outputAttribute;
        
        public MathCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.operation = getConfig("operation", "add");
            this.arguments = getConfig("arguments", new ArrayList<>());
            this.outputAttribute = getConfig("output_attribute", "result");
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            double value = 0;
            boolean first = true;
            
            for (String arg : arguments) {
                if (data.containsKey(arg)) {
                    double num = toDouble(data.get(arg));
                    if (first) {
                        value = num;
                        first = false;
                    } else {
                        switch (operation.toLowerCase()) {
                            case "add":
                            case "sum":
                                value += num;
                                break;
                            case "subtract":
                            case "sub":
                                value -= num;
                                break;
                            case "multiply":
                            case "mul":
                                value *= num;
                                break;
                            case "divide":
                            case "div":
                                if (num != 0) value /= num;
                                break;
                            case "max":
                                value = Math.max(value, num);
                                break;
                            case "min":
                                value = Math.min(value, num);
                                break;
                        }
                    }
                }
            }
            
            result.put(outputAttribute, value);
            return result;
        }
        
        private double toDouble(Object obj) {
            if (obj instanceof Number) {
                return ((Number) obj).doubleValue();
            }
            try {
                return Double.parseDouble(obj.toString());
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }
    
    /**
     * Trade pricing calculator.
     * Demonstrates real-world financial calculations.
     */
    public static class TradePricingCalculator extends AbstractCalculator {
        
        private final double commissionRate;
        private final double taxRate;
        private final boolean includeVAT;
        
        public TradePricingCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.commissionRate = getConfig("commission_rate", 0.001); // 0.1% default
            this.taxRate = getConfig("tax_rate", 0.0);
            this.includeVAT = getConfig("include_vat", false);
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            // Get trade values
            double price = toDouble(data.get("price"));
            double quantity = toDouble(data.get("quantity"));
            
            // Calculate gross value
            double grossValue = price * quantity;
            result.put("gross_value", grossValue);
            
            // Calculate commission
            double commission = grossValue * commissionRate;
            result.put("commission", commission);
            
            // Calculate tax if applicable
            double tax = 0;
            if (taxRate > 0) {
                tax = grossValue * taxRate;
                result.put("tax", tax);
            }
            
            // Calculate VAT if applicable
            double vat = 0;
            if (includeVAT) {
                vat = (grossValue + commission) * 0.20; // 20% VAT
                result.put("vat", vat);
            }
            
            // Calculate net value
            double netValue = grossValue + commission + tax + vat;
            result.put("net_value", netValue);
            
            // Add timestamp
            result.put("calculated_at", System.currentTimeMillis());
            
            return result;
        }
        
        private double toDouble(Object obj) {
            if (obj == null) return 0.0;
            if (obj instanceof Number) return ((Number) obj).doubleValue();
            try {
                return Double.parseDouble(obj.toString());
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        
        @Override
        protected Map<String, Object> getCustomDetails() {
            Map<String, Object> details = new HashMap<>();
            details.put("commission_rate", commissionRate);
            details.put("tax_rate", taxRate);
            details.put("include_vat", includeVAT);
            return details;
        }
    }
    
    /**
     * Risk calculation calculator.
     * Demonstrates complex financial risk calculations.
     */
    public static class RiskCalculator extends AbstractCalculator {
        
        private final double varConfidenceLevel;
        private final int lookbackPeriod;
        
        public RiskCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.varConfidenceLevel = getConfig("var_confidence", 0.95);
            this.lookbackPeriod = getConfig("lookback_period", 252); // Trading days
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            // Get position data
            double positionValue = toDouble(data.get("position_value"));
            double volatility = toDouble(data.get("volatility"));
            double delta = toDouble(data.get("delta"));
            double gamma = toDouble(data.get("gamma"));
            
            // Calculate Value at Risk (VaR)
            // Using parametric VaR: VaR = Position * Volatility * Z-score
            double zScore = getZScore(varConfidenceLevel);
            double dailyVaR = positionValue * volatility * zScore / Math.sqrt(lookbackPeriod);
            result.put("daily_var", dailyVaR);
            
            // Calculate annual VaR
            double annualVaR = dailyVaR * Math.sqrt(252);
            result.put("annual_var", annualVaR);
            
            // Calculate Greeks-based risk
            if (delta != 0 || gamma != 0) {
                double underlyingPrice = toDouble(data.get("underlying_price"));
                double priceMove = underlyingPrice * 0.01; // 1% move
                
                // Delta risk
                double deltaRisk = delta * priceMove;
                result.put("delta_risk", deltaRisk);
                
                // Gamma risk
                double gammaRisk = 0.5 * gamma * priceMove * priceMove;
                result.put("gamma_risk", gammaRisk);
                
                // Total risk
                result.put("total_greek_risk", deltaRisk + gammaRisk);
            }
            
            result.put("calculated_at", System.currentTimeMillis());
            return result;
        }
        
        private double getZScore(double confidence) {
            // Approximate Z-score for common confidence levels
            if (confidence >= 0.99) return 2.326;
            if (confidence >= 0.95) return 1.645;
            if (confidence >= 0.90) return 1.282;
            return 1.0;
        }
        
        private double toDouble(Object obj) {
            if (obj == null) return 0.0;
            if (obj instanceof Number) return ((Number) obj).doubleValue();
            try {
                return Double.parseDouble(obj.toString());
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }
    
    /**
     * String manipulation calculator.
     * Demonstrates data transformation capabilities.
     */
    public static class StringTransformCalculator extends AbstractCalculator {
        
        private final String operation;
        private final String sourceField;
        private final String targetField;
        
        public StringTransformCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.operation = getConfig("operation", "uppercase");
            this.sourceField = getConfig("source_field", "value");
            this.targetField = getConfig("target_field", "transformed");
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            Object sourceValue = data.get(sourceField);
            if (sourceValue != null) {
                String str = sourceValue.toString();
                String transformed;
                
                switch (operation.toLowerCase()) {
                    case "uppercase":
                        transformed = str.toUpperCase();
                        break;
                    case "lowercase":
                        transformed = str.toLowerCase();
                        break;
                    case "trim":
                        transformed = str.trim();
                        break;
                    case "reverse":
                        transformed = new StringBuilder(str).reverse().toString();
                        break;
                    case "length":
                        result.put(targetField, str.length());
                        return result;
                    default:
                        transformed = str;
                }
                
                result.put(targetField, transformed);
            }
            
            return result;
        }
    }
    
    /**
     * Aggregation calculator.
     * Maintains running aggregates across multiple calculations.
     * Note: This calculator maintains state - use with caution in multi-threaded scenarios.
     */
    public static class AggregationCalculator extends AbstractCalculator {
        
        private final String aggregateField;
        private final String aggregateType;
        
        // Running aggregates (thread-safe)
        private volatile double sum = 0;
        private volatile long count = 0;
        private volatile double min = Double.MAX_VALUE;
        private volatile double max = Double.MIN_VALUE;
        
        public AggregationCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.aggregateField = getConfig("aggregate_field", "value");
            this.aggregateType = getConfig("aggregate_type", "all");
        }
        
        @Override
        protected synchronized Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            Object value = data.get(aggregateField);
            if (value != null) {
                double numValue = toDouble(value);
                
                // Update aggregates
                sum += numValue;
                count++;
                min = Math.min(min, numValue);
                max = Math.max(max, numValue);
                
                // Add aggregates to result based on type
                if (aggregateType.equals("all") || aggregateType.equals("sum")) {
                    result.put("running_sum", sum);
                }
                if (aggregateType.equals("all") || aggregateType.equals("count")) {
                    result.put("running_count", count);
                }
                if (aggregateType.equals("all") || aggregateType.equals("avg")) {
                    result.put("running_avg", sum / count);
                }
                if (aggregateType.equals("all") || aggregateType.equals("min")) {
                    result.put("running_min", min);
                }
                if (aggregateType.equals("all") || aggregateType.equals("max")) {
                    result.put("running_max", max);
                }
            }
            
            return result;
        }
        
        private double toDouble(Object obj) {
            if (obj instanceof Number) return ((Number) obj).doubleValue();
            try {
                return Double.parseDouble(obj.toString());
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        
        /**
         * Reset aggregates.
         */
        public synchronized void reset() {
            sum = 0;
            count = 0;
            min = Double.MAX_VALUE;
            max = Double.MIN_VALUE;
        }
        
        @Override
        protected Map<String, Object> getCustomDetails() {
            Map<String, Object> details = new HashMap<>();
            details.put("aggregate_field", aggregateField);
            details.put("aggregate_type", aggregateType);
            details.put("current_sum", sum);
            details.put("current_count", count);
            return details;
        }
    }
}
