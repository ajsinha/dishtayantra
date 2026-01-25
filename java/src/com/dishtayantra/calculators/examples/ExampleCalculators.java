package com.dishtayantra.calculators.examples;

import com.dishtayantra.calculators.AbstractCalculator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

/**
 * Example calculators demonstrating Py4J integration with DishtaYantra.
 * 
 * v1.6.0: Enhanced calculator library with:
 * - PortfolioCalculator for NAV and P&L calculations
 * - DataValidationCalculator for data quality checks
 * - Improved error handling and statistics
 * 
 * These calculators show how to implement high-performance Java calculations
 * that can be invoked from Python via Py4J.
 * 
 * @author DishtaYantra
 * @version 1.6.0
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
            Map<String, Object> result = new HashMap<>(data);
            result.put("_passthru", true);
            result.put("_calculator", getName());
            return result;
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
        
        @SuppressWarnings("unchecked")
        public MathCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.operation = getConfig("operation", "add");
            
            Object args = config.get("arguments");
            if (args instanceof List) {
                this.arguments = (List<String>) args;
            } else {
                this.arguments = new ArrayList<>();
            }
            
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
                            case "pow":
                                value = Math.pow(value, num);
                                break;
                        }
                    }
                }
            }
            
            result.put(outputAttribute, value);
            result.put("_operation", operation);
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
        private final double vatRate;
        
        public TradePricingCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.commissionRate = getConfig("commission_rate", 0.001);
            this.taxRate = getConfig("tax_rate", 0.0);
            this.includeVAT = getConfig("include_vat", false);
            this.vatRate = getConfig("vat_rate", 0.20);
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            double price = toDouble(data.get("price"));
            double quantity = toDouble(data.get("quantity"));
            
            double grossValue = price * quantity;
            result.put("gross_value", grossValue);
            
            double commission = grossValue * commissionRate;
            result.put("commission", commission);
            
            double tax = 0;
            if (taxRate > 0) {
                tax = grossValue * taxRate;
                result.put("tax", tax);
            }
            
            double vat = 0;
            if (includeVAT) {
                vat = (grossValue + commission) * vatRate;
                result.put("vat", vat);
            }
            
            double netValue = grossValue + commission + tax + vat;
            result.put("net_value", netValue);
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
            details.put("vat_rate", vatRate);
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
            this.lookbackPeriod = getConfig("lookback_period", 252);
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            double positionValue = toDouble(data.get("position_value"));
            double volatility = toDouble(data.get("volatility"));
            double delta = toDouble(data.get("delta"));
            double gamma = toDouble(data.get("gamma"));
            
            double zScore = getZScore(varConfidenceLevel);
            double dailyVaR = positionValue * volatility * zScore / Math.sqrt(lookbackPeriod);
            result.put("daily_var", dailyVaR);
            
            double annualVaR = dailyVaR * Math.sqrt(252);
            result.put("annual_var", annualVaR);
            
            if (delta != 0 || gamma != 0) {
                double underlyingPrice = toDouble(data.get("underlying_price"));
                double priceMove = underlyingPrice * 0.01;
                
                double deltaRisk = delta * priceMove;
                result.put("delta_risk", deltaRisk);
                
                double gammaRisk = 0.5 * gamma * priceMove * priceMove;
                result.put("gamma_risk", gammaRisk);
                
                result.put("total_greek_risk", deltaRisk + gammaRisk);
            }
            
            result.put("var_confidence", varConfidenceLevel);
            result.put("calculated_at", System.currentTimeMillis());
            return result;
        }
        
        private double getZScore(double confidence) {
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
     * Aggregation calculator with running statistics.
     */
    public static class AggregationCalculator extends AbstractCalculator {
        
        private final String aggregateField;
        private final String aggregateType;
        
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
                
                sum += numValue;
                count++;
                min = Math.min(min, numValue);
                max = Math.max(max, numValue);
                
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
    
    /**
     * Portfolio calculator for NAV and P&L calculations.
     * v1.6.0: New calculator for portfolio analytics.
     */
    public static class PortfolioCalculator extends AbstractCalculator {
        
        private final boolean includeUnrealized;
        private final String baseCurrency;
        
        public PortfolioCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.includeUnrealized = getConfig("include_unrealized", true);
            this.baseCurrency = getConfig("base_currency", "USD");
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            double quantity = toDouble(data.get("quantity"));
            double currentPrice = toDouble(data.get("current_price"));
            double avgCost = toDouble(data.get("avg_cost"));
            double realizedPnL = toDouble(data.get("realized_pnl"));
            
            // Calculate market value
            double marketValue = quantity * currentPrice;
            result.put("market_value", marketValue);
            
            // Calculate cost basis
            double costBasis = quantity * avgCost;
            result.put("cost_basis", costBasis);
            
            // Calculate unrealized P&L
            double unrealizedPnL = marketValue - costBasis;
            result.put("unrealized_pnl", unrealizedPnL);
            
            // Calculate total P&L
            double totalPnL = realizedPnL;
            if (includeUnrealized) {
                totalPnL += unrealizedPnL;
            }
            result.put("total_pnl", totalPnL);
            
            // Calculate return percentage
            if (costBasis != 0) {
                double returnPct = (unrealizedPnL / costBasis) * 100;
                result.put("return_pct", returnPct);
            }
            
            result.put("base_currency", baseCurrency);
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
            details.put("include_unrealized", includeUnrealized);
            details.put("base_currency", baseCurrency);
            return details;
        }
    }
    
    /**
     * Data validation calculator.
     * v1.6.0: New calculator for data quality validation.
     */
    public static class DataValidationCalculator extends AbstractCalculator {
        
        private final boolean strictMode;
        private final Set<String> requiredFields;
        
        @SuppressWarnings("unchecked")
        public DataValidationCalculator(String name, Map<String, Object> config) {
            super(name, config);
            this.strictMode = getConfig("strict_mode", false);
            
            this.requiredFields = new HashSet<>();
            Object fields = config.get("required_fields");
            if (fields instanceof List) {
                for (Object field : (List<?>) fields) {
                    requiredFields.add(field.toString());
                }
            }
        }
        
        @Override
        protected Map<String, Object> doCalculate(Map<String, Object> data) {
            Map<String, Object> result = new HashMap<>(data);
            
            List<String> errors = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            boolean isValid = true;
            
            // Check required fields
            for (String field : requiredFields) {
                if (!data.containsKey(field) || data.get(field) == null) {
                    errors.add("Missing required field: " + field);
                    isValid = false;
                } else if (data.get(field).toString().trim().isEmpty()) {
                    if (strictMode) {
                        errors.add("Empty required field: " + field);
                        isValid = false;
                    } else {
                        warnings.add("Empty field: " + field);
                    }
                }
            }
            
            // Validate numeric fields if present
            String[] numericFields = {"price", "quantity", "value", "amount"};
            for (String field : numericFields) {
                if (data.containsKey(field)) {
                    Object val = data.get(field);
                    if (val != null && !(val instanceof Number)) {
                        try {
                            Double.parseDouble(val.toString());
                        } catch (NumberFormatException e) {
                            warnings.add("Non-numeric value in field: " + field);
                        }
                    }
                }
            }
            
            result.put("_validation_valid", isValid);
            result.put("_validation_errors", errors);
            result.put("_validation_warnings", warnings);
            result.put("_validation_error_count", errors.size());
            result.put("_validation_warning_count", warnings.size());
            result.put("_validated_at", System.currentTimeMillis());
            
            return result;
        }
        
        @Override
        protected Map<String, Object> getCustomDetails() {
            Map<String, Object> details = new HashMap<>();
            details.put("strict_mode", strictMode);
            details.put("required_fields", new ArrayList<>(requiredFields));
            return details;
        }
    }
}
