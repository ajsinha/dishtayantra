package com.dishtayantra.calculators;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Instant;

/**
 * Abstract base class for DishtaYantra calculators.
 * 
 * Provides common functionality like calculation counting and timing.
 * Extend this class to create your own calculators.
 * 
 * Example:
 * <pre>
 * public class MyCalculator extends AbstractCalculator {
 *     public MyCalculator(String name, Map<String, Object> config) {
 *         super(name, config);
 *     }
 *     
 *     @Override
 *     protected Map<String, Object> doCalculate(Map<String, Object> data) {
 *         Map<String, Object> result = new HashMap<>(data);
 *         // Your calculation logic here
 *         return result;
 *     }
 * }
 * </pre>
 * 
 * @author DishtaYantra
 * @version 1.1.0
 */
public abstract class AbstractCalculator implements Calculator {
    
    protected final String name;
    protected final Map<String, Object> config;
    
    // Statistics
    private final AtomicLong calculationCount = new AtomicLong(0);
    private volatile String lastCalculationTime;
    private final AtomicLong totalCalculationTimeNanos = new AtomicLong(0);
    
    /**
     * Constructor.
     * 
     * @param name Calculator name
     * @param config Configuration map
     */
    public AbstractCalculator(String name, Map<String, Object> config) {
        this.name = name;
        this.config = config != null ? new HashMap<>(config) : new HashMap<>();
    }
    
    /**
     * Get a configuration value with default.
     */
    @SuppressWarnings("unchecked")
    protected <T> T getConfig(String key, T defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        return (T) value;
    }
    
    /**
     * Perform calculation with timing and counting.
     */
    @Override
    public final Map<String, Object> calculate(Map<String, Object> data) {
        long startTime = System.nanoTime();
        
        try {
            Map<String, Object> result = doCalculate(data);
            return result;
        } finally {
            long elapsed = System.nanoTime() - startTime;
            calculationCount.incrementAndGet();
            totalCalculationTimeNanos.addAndGet(elapsed);
            lastCalculationTime = Instant.now().toString();
        }
    }
    
    /**
     * Override this method to implement your calculation logic.
     * 
     * @param data Input data
     * @return Calculated result
     */
    protected abstract Map<String, Object> doCalculate(Map<String, Object> data);
    
    /**
     * Get calculator details.
     */
    @Override
    public Map<String, Object> details() {
        Map<String, Object> details = new HashMap<>();
        details.put("name", name);
        details.put("type", this.getClass().getSimpleName());
        details.put("java_class", this.getClass().getName());
        details.put("calculation_count", calculationCount.get());
        details.put("last_calculation", lastCalculationTime);
        
        // Calculate average time in milliseconds
        long count = calculationCount.get();
        if (count > 0) {
            double avgTimeMs = (totalCalculationTimeNanos.get() / count) / 1_000_000.0;
            details.put("avg_calculation_time_ms", avgTimeMs);
        }
        
        // Add custom details from subclass
        Map<String, Object> customDetails = getCustomDetails();
        if (customDetails != null) {
            details.putAll(customDetails);
        }
        
        return details;
    }
    
    /**
     * Override to add custom details.
     */
    protected Map<String, Object> getCustomDetails() {
        return null;
    }
    
    /**
     * Get calculator name.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get calculator configuration.
     */
    public Map<String, Object> getConfig() {
        return new HashMap<>(config);
    }
}
