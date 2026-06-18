/**
 * LMDB-Enabled Java Calculator Example
 * 
 * This example demonstrates how to use LMDB zero-copy data exchange
 * in a Java calculator for high-performance large payload processing.
 * 
 * Dependencies (Maven):
 *   <dependency>
 *     <groupId>org.lmdbjava</groupId>
 *     <artifactId>lmdbjava</artifactId>
 *     <version>0.8.3</version>
 *   </dependency>
 *   <dependency>
 *     <groupId>org.msgpack</groupId>
 *     <artifactId>msgpack-core</artifactId>
 *     <version>0.9.6</version>
 *   </dependency>
 * 
 * Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
 */

package com.dishtayantra.calculators.lmdb;

import org.lmdbjava.*;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Abstract base class for LMDB-enabled Java calculators.
 * Provides zero-copy data exchange with Python DAG Engine.
 */
public abstract class LMDBEnabledCalculator {
    
    private static final Logger logger = Logger.getLogger(LMDBEnabledCalculator.class.getName());
    
    // LMDB environment and database
    protected Env<ByteBuffer> env;
    protected Dbi<ByteBuffer> dbi;
    protected String dbPath;
    protected boolean lmdbInitialized = false;
    
    // Configuration
    protected long mapSize = 1024L * 1024L * 1024L; // 1GB default
    protected int maxDbs = 100;
    
    // Statistics
    protected long calculations = 0;
    protected long lmdbExchanges = 0;
    protected long totalProcessingTimeNs = 0;
    
    /**
     * Initialize LMDB environment.
     * 
     * @param dbPath Path to LMDB database directory
     * @return true if initialization successful
     */
    public boolean initLMDB(String dbPath) {
        try {
            this.dbPath = dbPath;
            
            // Create directory if needed
            File dbDir = new File(dbPath);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
            
            // Create LMDB environment
            env = Env.create()
                .setMapSize(mapSize)
                .setMaxDbs(maxDbs)
                .open(dbDir, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_NOSYNC);
            
            // Open default database
            dbi = env.openDbi("default", DbiFlags.MDB_CREATE);
            
            lmdbInitialized = true;
            logger.info("LMDB initialized at: " + dbPath);
            return true;
            
        } catch (Exception e) {
            logger.severe("Failed to initialize LMDB: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Main calculation entry point.
     * Automatically detects LMDB reference and handles accordingly.
     * 
     * @param data Input data (may be LMDB reference or direct data)
     * @return Calculation result
     */
    public Map<String, Object> calculate(Map<String, Object> data) {
        long startTime = System.nanoTime();
        calculations++;
        
        Map<String, Object> result;
        
        // Check if this is an LMDB reference
        if (isLMDBReference(data)) {
            result = calculateWithLMDB(data);
        } else {
            result = processData(data);
        }
        
        long elapsedNs = System.nanoTime() - startTime;
        totalProcessingTimeNs += elapsedNs;
        result.put("_processing_time_ns", elapsedNs);
        
        return result;
    }
    
    /**
     * Check if data is an LMDB reference from Python.
     */
    protected boolean isLMDBReference(Map<String, Object> data) {
        Object ref = data.get("_lmdb_ref");
        return ref != null && (Boolean) ref;
    }
    
    /**
     * Process data via LMDB zero-copy exchange.
     */
    protected Map<String, Object> calculateWithLMDB(Map<String, Object> refData) {
        lmdbExchanges++;
        
        String inputKey = (String) refData.get("_lmdb_input_key");
        String outputKey = (String) refData.get("_lmdb_output_key");
        String lmdbPath = (String) refData.get("_lmdb_db_path");
        String format = (String) refData.getOrDefault("_lmdb_format", "msgpack");
        
        logger.fine("Processing LMDB reference: " + inputKey);
        
        // Initialize LMDB if needed
        if (!lmdbInitialized || !lmdbPath.equals(this.dbPath)) {
            initLMDB(lmdbPath);
        }
        
        // Read input data from LMDB (zero-copy via memory map!)
        Map<String, Object> actualData = readFromLMDB(inputKey, format);
        
        if (actualData == null) {
            throw new RuntimeException("No data found at LMDB key: " + inputKey);
        }
        
        // Process the data
        Map<String, Object> result = processData(actualData);
        
        // Write result back to LMDB if output key specified
        if (outputKey != null && !outputKey.isEmpty()) {
            writeToLMDB(outputKey, result, format);
            logger.fine("Wrote result to LMDB: " + outputKey);
        }
        
        result.put("_exchange_type", "lmdb");
        return result;
    }
    
    /**
     * Read data from LMDB (zero-copy via memory-mapped file).
     */
    protected Map<String, Object> readFromLMDB(String key, String format) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(key.length());
            keyBuf.put(key.getBytes(StandardCharsets.UTF_8)).flip();
            
            // ZERO-COPY: This returns a view into the memory-mapped file!
            ByteBuffer dataBuf = dbi.get(txn, keyBuf);
            
            if (dataBuf == null) {
                return null;
            }
            
            // Deserialize based on format
            if ("msgpack".equals(format)) {
                return deserializeMsgPack(dataBuf);
            } else {
                return deserializeJson(dataBuf);
            }
        }
    }
    
    /**
     * Write data to LMDB.
     */
    protected void writeToLMDB(String key, Map<String, Object> data, String format) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(key.length());
            keyBuf.put(key.getBytes(StandardCharsets.UTF_8)).flip();
            
            byte[] serialized;
            if ("msgpack".equals(format)) {
                serialized = serializeMsgPack(data);
            } else {
                serialized = serializeJson(data);
            }
            
            ByteBuffer dataBuf = ByteBuffer.allocateDirect(serialized.length);
            dataBuf.put(serialized).flip();
            
            dbi.put(txn, keyBuf, dataBuf);
            txn.commit();
        }
    }
    
    /**
     * Abstract method for actual data processing.
     * Override this in concrete calculator implementations.
     */
    protected abstract Map<String, Object> processData(Map<String, Object> data);
    
    /**
     * Get calculator statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("calculations", calculations);
        stats.put("lmdb_exchanges", lmdbExchanges);
        stats.put("total_processing_time_ns", totalProcessingTimeNs);
        stats.put("avg_processing_time_us", calculations > 0 ? 
            (totalProcessingTimeNs / calculations) / 1000.0 : 0);
        stats.put("lmdb_initialized", lmdbInitialized);
        stats.put("db_path", dbPath);
        return stats;
    }
    
    /**
     * Close LMDB environment.
     */
    public void close() {
        if (dbi != null) dbi.close();
        if (env != null) env.close();
        lmdbInitialized = false;
    }
    
    // =========================================================================
    // Serialization Helpers
    // =========================================================================
    
    protected byte[] serializeMsgPack(Map<String, Object> data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             MessagePacker packer = MessagePack.newDefaultPacker(out)) {
            packMap(packer, data);
            packer.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("MsgPack serialization failed", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    protected void packMap(MessagePacker packer, Map<String, Object> map) throws Exception {
        packer.packMapHeader(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            packer.packString(entry.getKey());
            packValue(packer, entry.getValue());
        }
    }
    
    @SuppressWarnings("unchecked")
    protected void packValue(MessagePacker packer, Object value) throws Exception {
        if (value == null) {
            packer.packNil();
        } else if (value instanceof Boolean) {
            packer.packBoolean((Boolean) value);
        } else if (value instanceof Integer) {
            packer.packInt((Integer) value);
        } else if (value instanceof Long) {
            packer.packLong((Long) value);
        } else if (value instanceof Double) {
            packer.packDouble((Double) value);
        } else if (value instanceof Float) {
            packer.packFloat((Float) value);
        } else if (value instanceof String) {
            packer.packString((String) value);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            packer.packArrayHeader(list.size());
            for (Object item : list) {
                packValue(packer, item);
            }
        } else if (value instanceof Map) {
            packMap(packer, (Map<String, Object>) value);
        } else {
            packer.packString(value.toString());
        }
    }
    
    protected Map<String, Object> deserializeMsgPack(ByteBuffer buffer) {
        try {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            
            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
                return unpackMap(unpacker);
            }
        } catch (Exception e) {
            throw new RuntimeException("MsgPack deserialization failed", e);
        }
    }
    
    protected Map<String, Object> unpackMap(MessageUnpacker unpacker) throws Exception {
        int size = unpacker.unpackMapHeader();
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = unpacker.unpackString();
            Object value = unpackValue(unpacker);
            map.put(key, value);
        }
        return map;
    }
    
    protected Object unpackValue(MessageUnpacker unpacker) throws Exception {
        switch (unpacker.getNextFormat().getValueType()) {
            case NIL:
                unpacker.unpackNil();
                return null;
            case BOOLEAN:
                return unpacker.unpackBoolean();
            case INTEGER:
                return unpacker.unpackLong();
            case FLOAT:
                return unpacker.unpackDouble();
            case STRING:
                return unpacker.unpackString();
            case ARRAY:
                int arrSize = unpacker.unpackArrayHeader();
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < arrSize; i++) {
                    list.add(unpackValue(unpacker));
                }
                return list;
            case MAP:
                return unpackMap(unpacker);
            default:
                unpacker.skipValue();
                return null;
        }
    }
    
    protected byte[] serializeJson(Map<String, Object> data) {
        // Simple JSON serialization (use Jackson or Gson in production)
        StringBuilder sb = new StringBuilder();
        serializeJsonObject(sb, data);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    @SuppressWarnings("unchecked")
    protected void serializeJsonObject(StringBuilder sb, Map<String, Object> map) {
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":");
            serializeJsonValue(sb, entry.getValue());
            first = false;
        }
        sb.append("}");
    }
    
    @SuppressWarnings("unchecked")
    protected void serializeJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else if (value instanceof String) {
            sb.append("\"").append(value).append("\"");
        } else if (value instanceof List) {
            sb.append("[");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(",");
                serializeJsonValue(sb, item);
                first = false;
            }
            sb.append("]");
        } else if (value instanceof Map) {
            serializeJsonObject(sb, (Map<String, Object>) value);
        }
    }
    
    protected Map<String, Object> deserializeJson(ByteBuffer buffer) {
        // Simple JSON deserialization (use Jackson or Gson in production)
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String json = new String(bytes, StandardCharsets.UTF_8);
        // This is a placeholder - use a proper JSON library
        return new HashMap<>();
    }
}


/**
 * Example: Matrix Processor with LMDB support
 */
class MatrixProcessorCalculator extends LMDBEnabledCalculator {
    
    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> processData(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>();
        
        List<List<Number>> matrix = (List<List<Number>>) data.get("matrix");
        String operation = (String) data.getOrDefault("operation", "sum");
        
        result.put("input_rows", matrix != null ? matrix.size() : 0);
        result.put("operation", operation);
        result.put("processed_at", System.currentTimeMillis());
        
        if (matrix != null && !matrix.isEmpty()) {
            if ("sum".equals(operation)) {
                double sum = 0;
                for (List<Number> row : matrix) {
                    for (Number val : row) {
                        sum += val.doubleValue();
                    }
                }
                result.put("result", sum);
            } else if ("determinant".equals(operation) && matrix.size() == matrix.get(0).size()) {
                // Simplified determinant for 2x2
                if (matrix.size() == 2) {
                    double det = matrix.get(0).get(0).doubleValue() * matrix.get(1).get(1).doubleValue()
                               - matrix.get(0).get(1).doubleValue() * matrix.get(1).get(0).doubleValue();
                    result.put("result", det);
                }
            }
        }
        
        return result;
    }
}


/**
 * Example: Risk Calculator with LMDB support
 */
class RiskCalculator extends LMDBEnabledCalculator {
    
    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> processData(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>();
        
        List<Map<String, Object>> positions = (List<Map<String, Object>>) data.get("positions");
        
        double totalExposure = 0;
        if (positions != null) {
            for (Map<String, Object> pos : positions) {
                double qty = ((Number) pos.getOrDefault("quantity", 0)).doubleValue();
                double price = ((Number) pos.getOrDefault("price", 0)).doubleValue();
                totalExposure += qty * price;
            }
        }
        
        result.put("total_positions", positions != null ? positions.size() : 0);
        result.put("total_exposure", totalExposure);
        result.put("var_95", totalExposure * 0.025);
        result.put("var_99", totalExposure * 0.035);
        result.put("calculated_at", System.currentTimeMillis());
        
        return result;
    }
}


/**
 * Example: Pricing Engine with LMDB support
 */
class PricingEngineCalculator extends LMDBEnabledCalculator {
    
    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> processData(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>();
        
        String instrument = (String) data.getOrDefault("instrument", "unknown");
        double spot = ((Number) data.getOrDefault("spot", 100)).doubleValue();
        double strike = ((Number) data.getOrDefault("strike", 100)).doubleValue();
        double volatility = ((Number) data.getOrDefault("volatility", 0.2)).doubleValue();
        double rate = ((Number) data.getOrDefault("rate", 0.05)).doubleValue();
        double time = ((Number) data.getOrDefault("time", 1.0)).doubleValue();
        
        // Simplified Black-Scholes approximation
        double d1 = (Math.log(spot / strike) + (rate + 0.5 * volatility * volatility) * time) 
                    / (volatility * Math.sqrt(time));
        double d2 = d1 - volatility * Math.sqrt(time);
        
        // Approximate normal CDF
        double nd1 = 0.5 * (1 + Math.tanh(0.8 * d1));
        double nd2 = 0.5 * (1 + Math.tanh(0.8 * d2));
        
        double callPrice = spot * nd1 - strike * Math.exp(-rate * time) * nd2;
        double putPrice = strike * Math.exp(-rate * time) * (1 - nd2) - spot * (1 - nd1);
        
        result.put("instrument", instrument);
        result.put("call_price", callPrice);
        result.put("put_price", putPrice);
        result.put("delta", nd1);
        result.put("calculated_at", System.currentTimeMillis());
        
        return result;
    }
}


// =============================================================================
// Main class for testing
// =============================================================================
class LMDBCalculatorExample {
    
    public static void main(String[] args) {
        System.out.println("=== Java LMDB Calculator Examples ===\n");
        
        // Example 1: Matrix Processor
        MatrixProcessorCalculator matrixCalc = new MatrixProcessorCalculator();
        matrixCalc.initLMDB("/tmp/dishtayantra_lmdb");
        
        // Direct calculation (no LMDB)
        Map<String, Object> directData = new HashMap<>();
        directData.put("matrix", Arrays.asList(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6),
            Arrays.asList(7, 8, 9)
        ));
        directData.put("operation", "sum");
        
        Map<String, Object> result = matrixCalc.calculate(directData);
        System.out.println("Direct calculation result: " + result);
        
        // LMDB reference calculation (simulating Python DAG Engine call)
        Map<String, Object> lmdbRef = new HashMap<>();
        lmdbRef.put("_lmdb_ref", true);
        lmdbRef.put("_lmdb_input_key", "input:matrix:001");
        lmdbRef.put("_lmdb_output_key", "output:matrix:001");
        lmdbRef.put("_lmdb_db_path", "/tmp/dishtayantra_lmdb");
        lmdbRef.put("_lmdb_format", "msgpack");
        
        // Note: In real usage, Python DAG Engine would write to LMDB first
        // For this example, we'll skip the LMDB reference test
        
        System.out.println("\nCalculator stats: " + matrixCalc.getStats());
        
        // Example 2: Risk Calculator
        RiskCalculator riskCalc = new RiskCalculator();
        riskCalc.initLMDB("/tmp/dishtayantra_lmdb");
        
        Map<String, Object> riskData = new HashMap<>();
        List<Map<String, Object>> positions = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> pos = new HashMap<>();
            pos.put("symbol", "STOCK" + i);
            pos.put("quantity", 1000);
            pos.put("price", 50.0 + Math.random() * 100);
            positions.add(pos);
        }
        riskData.put("positions", positions);
        
        Map<String, Object> riskResult = riskCalc.calculate(riskData);
        System.out.println("\nRisk calculation result: " + riskResult);
        
        // Cleanup
        matrixCalc.close();
        riskCalc.close();
        
        System.out.println("\n=== Done ===");
    }
}
