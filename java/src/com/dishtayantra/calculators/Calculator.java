package com.dishtayantra.calculators;

import java.util.Map;

/**
 * Interface for DishtaYantra calculators.
 * 
 * All Java calculators must implement this interface to be compatible
 * with DishtaYantra's Py4J integration.
 * 
 * @author DishtaYantra
 * @version 1.1.0
 */
public interface Calculator {
    
    /**
     * Perform calculation on input data.
     * 
     * @param data Input data as a Map
     * @return Calculated result as a Map
     */
    Map<String, Object> calculate(Map<String, Object> data);
    
    /**
     * Get calculator details and statistics.
     * 
     * @return Details as a Map including name, type, calculation count, etc.
     */
    Map<String, Object> details();
}
