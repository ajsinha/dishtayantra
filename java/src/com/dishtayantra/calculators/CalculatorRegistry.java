package com.dishtayantra.calculators;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Calculator Registry for DishtaYantra.
 * 
 * v1.6.0: Provides centralized registration and discovery of Java calculators.
 * This class allows dynamic registration of calculator classes and lookup by name.
 * 
 * @author DishtaYantra
 * @version 1.6.0
 */
public class CalculatorRegistry {
    
    private static final Logger logger = Logger.getLogger(CalculatorRegistry.class.getName());
    
    // Singleton instance
    private static CalculatorRegistry instance;
    
    // Registry maps calculator name to class
    private final Map<String, Class<? extends Calculator>> registry = new ConcurrentHashMap<>();
    
    // Calculator metadata
    private final Map<String, Map<String, Object>> metadata = new ConcurrentHashMap<>();
    
    /**
     * Private constructor for singleton.
     */
    private CalculatorRegistry() {
        // Register default calculators
        registerDefaultCalculators();
    }
    
    /**
     * Get the singleton instance.
     */
    public static synchronized CalculatorRegistry getInstance() {
        if (instance == null) {
            instance = new CalculatorRegistry();
        }
        return instance;
    }
    
    /**
     * Register default calculators from the examples package.
     */
    @SuppressWarnings("unchecked")
    private void registerDefaultCalculators() {
        try {
            register("PassthruCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$PassthruCalculator"),
                "Passes input through unchanged", "testing");
            
            register("MathCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$MathCalculator"),
                "Mathematical operations (sum, multiply, etc.)", "computation");
            
            register("TradePricingCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$TradePricingCalculator"),
                "Trade pricing with commission, tax, and VAT", "finance");
            
            register("RiskCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$RiskCalculator"),
                "VaR and Greeks-based risk calculations", "finance");
            
            register("StringTransformCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$StringTransformCalculator"),
                "String transformations", "transformation");
            
            register("AggregationCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$AggregationCalculator"),
                "Running aggregates (sum, avg, min, max)", "aggregation");
            
            register("PortfolioCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$PortfolioCalculator"),
                "NAV and P&L calculations", "finance");
            
            register("DataValidationCalculator", 
                (Class<? extends Calculator>) Class.forName(
                    "com.dishtayantra.calculators.examples.ExampleCalculators$DataValidationCalculator"),
                "Data quality validation", "validation");
            
            logger.info("Registered " + registry.size() + " default calculators");
            
        } catch (ClassNotFoundException e) {
            logger.warning("Could not register default calculators: " + e.getMessage());
        }
    }
    
    /**
     * Register a calculator class.
     */
    public void register(String name, Class<? extends Calculator> calculatorClass, 
                        String description, String category) {
        registry.put(name, calculatorClass);
        
        Map<String, Object> meta = new HashMap<>();
        meta.put("name", name);
        meta.put("class", calculatorClass.getName());
        meta.put("description", description);
        meta.put("category", category);
        metadata.put(name, meta);
        
        logger.info("Registered calculator: " + name + " -> " + calculatorClass.getName());
    }
    
    /**
     * Register a calculator class (simple version).
     */
    public void register(String name, Class<? extends Calculator> calculatorClass) {
        register(name, calculatorClass, "", "custom");
    }
    
    /**
     * Get a calculator class by name.
     */
    public Class<? extends Calculator> get(String name) {
        return registry.get(name);
    }
    
    /**
     * Check if a calculator is registered.
     */
    public boolean isRegistered(String name) {
        return registry.containsKey(name);
    }
    
    /**
     * Get list of all registered calculator names.
     */
    public List<String> getAvailableCalculators() {
        return new ArrayList<>(registry.keySet());
    }
    
    /**
     * Get metadata for a calculator.
     */
    public Map<String, Object> getMetadata(String name) {
        return metadata.get(name);
    }
    
    /**
     * Get all calculator metadata.
     */
    public List<Map<String, Object>> getAllMetadata() {
        return new ArrayList<>(metadata.values());
    }
    
    /**
     * Create a calculator instance.
     */
    public Calculator create(String name, String instanceName, Map<String, Object> config) 
            throws Exception {
        Class<? extends Calculator> clazz = registry.get(name);
        
        if (clazz == null) {
            // Try to load by full class name
            clazz = (Class<? extends Calculator>) Class.forName(name);
        }
        
        return clazz.getConstructor(String.class, Map.class).newInstance(instanceName, config);
    }
    
    /**
     * Get calculators by category.
     */
    public List<String> getByCategory(String category) {
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> entry : metadata.entrySet()) {
            if (category.equals(entry.getValue().get("category"))) {
                result.add(entry.getKey());
            }
        }
        return result;
    }
    
    /**
     * Get the total count of registered calculators.
     */
    public int getCount() {
        return registry.size();
    }
}
