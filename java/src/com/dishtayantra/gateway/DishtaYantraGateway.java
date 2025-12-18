package com.dishtayantra.gateway;

import py4j.GatewayServer;
import py4j.GatewayServerListener;
import py4j.Py4JServerConnection;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * DishtaYantra Gateway Server for Py4J Integration.
 * 
 * This server allows Python code to invoke Java calculators with high performance.
 * It supports multiple concurrent connections and provides calculator registration.
 * 
 * Usage:
 * <pre>
 * java -cp "dishtayantra.jar:py4j.jar" com.dishtayantra.gateway.DishtaYantraGateway
 * 
 * Options:
 *   -p, --port PORT      Gateway port (default: 25333)
 *   -h, --host HOST      Bind address (default: localhost)
 *   --pool-size N        Number of gateway instances for pooling (default: 1)
 * </pre>
 * 
 * @author DishtaYantra
 * @version 1.1.0
 */
public class DishtaYantraGateway {
    
    private static final Logger logger = Logger.getLogger(DishtaYantraGateway.class.getName());
    
    // Gateway instances for connection pooling
    private final List<GatewayServer> gatewayServers = new ArrayList<>();
    
    // Calculator registry
    private final Map<String, Class<?>> calculatorRegistry = new ConcurrentHashMap<>();
    
    // Connection statistics
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    
    private final String host;
    private final int basePort;
    private final int poolSize;
    
    /**
     * Constructor.
     */
    public DishtaYantraGateway(String host, int basePort, int poolSize) {
        this.host = host;
        this.basePort = basePort;
        this.poolSize = poolSize;
        
        // Register default calculators
        registerDefaultCalculators();
    }
    
    /**
     * Register default calculators from the examples package.
     */
    private void registerDefaultCalculators() {
        try {
            // Register example calculators
            registerCalculator("PassthruCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$PassthruCalculator"));
            registerCalculator("MathCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$MathCalculator"));
            registerCalculator("TradePricingCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$TradePricingCalculator"));
            registerCalculator("RiskCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$RiskCalculator"));
            registerCalculator("StringTransformCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$StringTransformCalculator"));
            registerCalculator("AggregationCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$AggregationCalculator"));
            
            logger.info("Registered " + calculatorRegistry.size() + " default calculators");
        } catch (ClassNotFoundException e) {
            logger.warning("Could not register default calculators: " + e.getMessage());
        }
    }
    
    /**
     * Register a calculator class.
     */
    public void registerCalculator(String name, Class<?> calculatorClass) {
        calculatorRegistry.put(name, calculatorClass);
        logger.info("Registered calculator: " + name + " -> " + calculatorClass.getName());
    }
    
    /**
     * Get list of registered calculators.
     */
    public List<String> getRegisteredCalculators() {
        return new ArrayList<>(calculatorRegistry.keySet());
    }
    
    /**
     * Create a calculator instance.
     */
    public Object createCalculator(String className, String name, Map<String, Object> config) throws Exception {
        // First check registry
        Class<?> clazz = calculatorRegistry.get(className);
        
        // If not in registry, try to load by full class name
        if (clazz == null) {
            clazz = Class.forName(className);
        }
        
        // Create instance using reflection
        return clazz.getConstructor(String.class, Map.class).newInstance(name, config);
    }
    
    /**
     * Start the gateway server(s).
     */
    public void start() {
        logger.info("Starting DishtaYantra Gateway on " + host + ":" + basePort + 
                   " with pool size " + poolSize);
        
        for (int i = 0; i < poolSize; i++) {
            int port = basePort + i;
            
            GatewayServer server = new GatewayServer.GatewayServerBuilder(this)
                .javaAddress(java.net.InetAddress.getLoopbackAddress())
                .javaPort(port)
                .build();
            
            // Add listener for connection tracking
            server.addListener(new GatewayServerListener() {
                @Override
                public void connectionStarted(Py4JServerConnection conn) {
                    int active = activeConnections.incrementAndGet();
                    int total = totalConnections.incrementAndGet();
                    logger.fine("Connection started on port " + port + 
                               ". Active: " + active + ", Total: " + total);
                }
                
                @Override
                public void connectionStopped(Py4JServerConnection conn) {
                    int active = activeConnections.decrementAndGet();
                    logger.fine("Connection stopped on port " + port + 
                               ". Active: " + active);
                }
                
                @Override
                public void serverStarted() {
                    logger.info("Gateway server started on port " + port);
                }
                
                @Override
                public void serverStopped() {
                    logger.info("Gateway server stopped on port " + port);
                }
                
                @Override
                public void serverError(Exception e) {
                    logger.log(Level.SEVERE, "Gateway server error on port " + port, e);
                }
                
                @Override
                public void connectionError(Exception e) {
                    logger.log(Level.WARNING, "Connection error on port " + port, e);
                }
            });
            
            server.start();
            gatewayServers.add(server);
        }
        
        logger.info("DishtaYantra Gateway started successfully with " + 
                   gatewayServers.size() + " server(s)");
        
        // Print usage info
        System.out.println("\n" +
            "╔═══════════════════════════════════════════════════════════════╗\n" +
            "║          DishtaYantra Java Gateway Server v1.1.0              ║\n" +
            "╠═══════════════════════════════════════════════════════════════╣\n" +
            "║  Listening on: " + String.format("%-46s", host + ":" + basePort + 
                (poolSize > 1 ? "-" + (basePort + poolSize - 1) : "")) + " ║\n" +
            "║  Pool Size:    " + String.format("%-46s", poolSize) + " ║\n" +
            "║  Calculators:  " + String.format("%-46s", calculatorRegistry.size() + " registered") + " ║\n" +
            "╠═══════════════════════════════════════════════════════════════╣\n" +
            "║  Press Ctrl+C to stop                                         ║\n" +
            "╚═══════════════════════════════════════════════════════════════╝\n");
    }
    
    /**
     * Stop all gateway servers.
     */
    public void shutdown() {
        logger.info("Shutting down DishtaYantra Gateway...");
        for (GatewayServer server : gatewayServers) {
            server.shutdown();
        }
        gatewayServers.clear();
        logger.info("Gateway shutdown complete");
    }
    
    /**
     * Get gateway statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("active_connections", activeConnections.get());
        stats.put("total_connections", totalConnections.get());
        stats.put("pool_size", poolSize);
        stats.put("registered_calculators", calculatorRegistry.size());
        stats.put("base_port", basePort);
        return stats;
    }
    
    /**
     * Health check - returns true if gateway is operational.
     */
    public boolean healthCheck() {
        return !gatewayServers.isEmpty();
    }
    
    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        String host = "localhost";
        int port = 25333;
        int poolSize = 1;
        
        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                case "--port":
                    if (i + 1 < args.length) {
                        port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-h":
                case "--host":
                    if (i + 1 < args.length) {
                        host = args[++i];
                    }
                    break;
                case "--pool-size":
                    if (i + 1 < args.length) {
                        poolSize = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--help":
                    printHelp();
                    return;
            }
        }
        
        final DishtaYantraGateway gateway = new DishtaYantraGateway(host, port, poolSize);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gateway.shutdown();
        }));
        
        gateway.start();
        
        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            gateway.shutdown();
        }
    }
    
    private static void printHelp() {
        System.out.println(
            "DishtaYantra Java Gateway Server\n" +
            "\n" +
            "Usage: java -cp \"dishtayantra.jar:py4j.jar\" " +
            "com.dishtayantra.gateway.DishtaYantraGateway [options]\n" +
            "\n" +
            "Options:\n" +
            "  -p, --port PORT      Gateway port (default: 25333)\n" +
            "  -h, --host HOST      Bind address (default: localhost)\n" +
            "  --pool-size N        Number of gateway instances (default: 1)\n" +
            "  --help               Show this help message\n" +
            "\n" +
            "For high-performance scenarios, use --pool-size to create multiple\n" +
            "gateway instances. Python can then connect to different ports for\n" +
            "true parallel execution.\n"
        );
    }
}
