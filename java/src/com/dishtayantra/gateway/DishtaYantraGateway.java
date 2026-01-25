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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.time.Instant;
import java.time.Duration;

/**
 * DishtaYantra Gateway Server for Py4J Integration.
 * 
 * v1.6.0: Enhanced gateway server with:
 * - Config-based initialization via jvm_config.json
 * - Multiple calculator registry support
 * - Enhanced statistics and monitoring
 * - Health check endpoints
 * - Graceful shutdown handling
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
 *   --pool-size N        Number of gateway instances for pooling (default: 4)
 *   --config FILE        Path to jvm_config.json (optional)
 * </pre>
 * 
 * @author DishtaYantra
 * @version 1.6.0
 */
public class DishtaYantraGateway {
    
    private static final Logger logger = Logger.getLogger(DishtaYantraGateway.class.getName());
    private static final String VERSION = "1.6.0";
    
    // Gateway instances for connection pooling
    private final List<GatewayServer> gatewayServers = new ArrayList<>();
    
    // Calculator registry - maps short name to Class
    private final Map<String, Class<?>> calculatorRegistry = new ConcurrentHashMap<>();
    
    // Calculator instances cache - for singleton calculators
    private final Map<String, Object> calculatorInstances = new ConcurrentHashMap<>();
    
    // Connection statistics
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicLong totalConnections = new AtomicLong(0);
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    
    // Configuration
    private final String host;
    private final int basePort;
    private final int poolSize;
    private final Instant startTime;
    
    // Gateway name (from config)
    private String gatewayName = "primary";
    
    /**
     * Constructor.
     */
    public DishtaYantraGateway(String host, int basePort, int poolSize) {
        this.host = host;
        this.basePort = basePort;
        this.poolSize = poolSize;
        this.startTime = Instant.now();
        
        // Register default calculators
        registerDefaultCalculators();
    }
    
    /**
     * Set the gateway name (from config).
     */
    public void setGatewayName(String name) {
        this.gatewayName = name;
    }
    
    /**
     * Get the gateway name.
     */
    public String getGatewayName() {
        return gatewayName;
    }
    
    /**
     * Register default calculators from the examples package.
     */
    private void registerDefaultCalculators() {
        try {
            // Register example calculators with short names
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
            registerCalculator("PortfolioCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$PortfolioCalculator"));
            registerCalculator("DataValidationCalculator", 
                Class.forName("com.dishtayantra.calculators.examples.ExampleCalculators$DataValidationCalculator"));
            
            logger.info("Registered " + calculatorRegistry.size() + " default calculators");
        } catch (ClassNotFoundException e) {
            logger.warning("Could not register default calculators: " + e.getMessage());
        }
    }
    
    /**
     * Register a calculator class by short name.
     */
    public void registerCalculator(String name, Class<?> calculatorClass) {
        calculatorRegistry.put(name, calculatorClass);
        logger.info("Registered calculator: " + name + " -> " + calculatorClass.getName());
    }
    
    /**
     * Register a calculator class by full class name.
     */
    public void registerCalculatorByClassName(String className) throws ClassNotFoundException {
        Class<?> clazz = Class.forName(className);
        String shortName = clazz.getSimpleName();
        calculatorRegistry.put(shortName, clazz);
        calculatorRegistry.put(className, clazz);
        logger.info("Registered calculator: " + shortName + " -> " + className);
    }
    
    /**
     * Get list of registered calculators.
     */
    public List<String> getRegisteredCalculators() {
        return new ArrayList<>(calculatorRegistry.keySet());
    }
    
    /**
     * Get calculator details for a registered calculator.
     */
    public Map<String, Object> getCalculatorInfo(String name) {
        Map<String, Object> info = new HashMap<>();
        Class<?> clazz = calculatorRegistry.get(name);
        
        if (clazz != null) {
            info.put("name", name);
            info.put("class", clazz.getName());
            info.put("simple_name", clazz.getSimpleName());
            info.put("package", clazz.getPackage() != null ? clazz.getPackage().getName() : "default");
            info.put("registered", true);
        } else {
            info.put("name", name);
            info.put("registered", false);
        }
        
        return info;
    }
    
    /**
     * Create a calculator instance.
     */
    public Object createCalculator(String className, String name, Map<String, Object> config) throws Exception {
        totalRequests.incrementAndGet();
        
        try {
            Class<?> clazz = calculatorRegistry.get(className);
            
            if (clazz == null) {
                clazz = Class.forName(className);
                calculatorRegistry.put(className, clazz);
            }
            
            Object instance = clazz.getConstructor(String.class, Map.class).newInstance(name, config);
            logger.fine("Created calculator instance: " + name + " (" + className + ")");
            return instance;
            
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            logger.log(Level.WARNING, "Failed to create calculator: " + className, e);
            throw e;
        }
    }
    
    /**
     * Get or create a singleton calculator instance.
     */
    public Object getOrCreateCalculator(String className, String name, Map<String, Object> config) throws Exception {
        String cacheKey = className + ":" + name;
        
        return calculatorInstances.computeIfAbsent(cacheKey, k -> {
            try {
                return createCalculator(className, name, config);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create calculator: " + e.getMessage(), e);
            }
        });
    }
    
    /**
     * Start the gateway server(s).
     */
    public void start() {
        logger.info("Starting DishtaYantra Gateway v" + VERSION + " on " + host + ":" + basePort + 
                   " with pool size " + poolSize);
        
        for (int i = 0; i < poolSize; i++) {
            int port = basePort + i;
            
            GatewayServer server = new GatewayServer.GatewayServerBuilder(this)
                .javaAddress(java.net.InetAddress.getLoopbackAddress())
                .javaPort(port)
                .build();
            
            final int serverPort = port;
            
            server.addListener(new GatewayServerListener() {
                @Override
                public void connectionStarted(Py4JServerConnection conn) {
                    int active = activeConnections.incrementAndGet();
                    long total = totalConnections.incrementAndGet();
                    logger.fine("Connection started on port " + serverPort + 
                               ". Active: " + active + ", Total: " + total);
                }
                
                @Override
                public void connectionStopped(Py4JServerConnection conn) {
                    int active = activeConnections.decrementAndGet();
                    logger.fine("Connection stopped on port " + serverPort + ". Active: " + active);
                }
                
                @Override
                public void serverStarted() {
                    logger.info("Gateway server started on port " + serverPort);
                }
                
                @Override
                public void serverStopped() {
                    logger.info("Gateway server stopped on port " + serverPort);
                }
                
                @Override
                public void serverPreShutdown() {
                    logger.info("Gateway server pre-shutdown on port " + serverPort);
                }
                
                @Override
                public void serverPostShutdown() {
                    logger.info("Gateway server post-shutdown on port " + serverPort);
                }
                
                @Override
                public void serverError(Exception e) {
                    totalErrors.incrementAndGet();
                    logger.log(Level.SEVERE, "Gateway server error on port " + serverPort, e);
                }
                
                @Override
                public void connectionError(Exception e) {
                    totalErrors.incrementAndGet();
                    logger.log(Level.WARNING, "Connection error on port " + serverPort, e);
                }
            });
            
            server.start();
            gatewayServers.add(server);
        }
        
        logger.info("DishtaYantra Gateway started with " + gatewayServers.size() + " server(s)");
        
        String portRange = poolSize > 1 ? basePort + "-" + (basePort + poolSize - 1) : String.valueOf(basePort);
        System.out.println("\n" +
            "╔═══════════════════════════════════════════════════════════════════╗\n" +
            "║           DishtaYantra Java Gateway Server v" + VERSION + "               ║\n" +
            "╠═══════════════════════════════════════════════════════════════════╣\n" +
            "║  Gateway Name: " + String.format("%-52s", gatewayName) + " ║\n" +
            "║  Listening on: " + String.format("%-52s", host + ":" + portRange) + " ║\n" +
            "║  Pool Size:    " + String.format("%-52s", poolSize + " server(s)") + " ║\n" +
            "║  Calculators:  " + String.format("%-52s", calculatorRegistry.size() + " registered") + " ║\n" +
            "╠═══════════════════════════════════════════════════════════════════╣\n" +
            "║  Press Ctrl+C to stop                                             ║\n" +
            "╚═══════════════════════════════════════════════════════════════════╝\n");
    }
    
    /**
     * Stop all gateway servers.
     */
    public void shutdown() {
        logger.info("Shutting down DishtaYantra Gateway...");
        calculatorInstances.clear();
        
        for (GatewayServer server : gatewayServers) {
            try {
                server.shutdown();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error shutting down gateway server", e);
            }
        }
        gatewayServers.clear();
        logger.info("Gateway shutdown complete");
    }
    
    /**
     * Get gateway statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("version", VERSION);
        stats.put("gateway_name", gatewayName);
        stats.put("host", host);
        stats.put("base_port", basePort);
        stats.put("pool_size", poolSize);
        stats.put("active_servers", gatewayServers.size());
        stats.put("active_connections", activeConnections.get());
        stats.put("total_connections", totalConnections.get());
        stats.put("total_requests", totalRequests.get());
        stats.put("total_errors", totalErrors.get());
        stats.put("registered_calculators", calculatorRegistry.size());
        stats.put("cached_instances", calculatorInstances.size());
        stats.put("start_time", startTime.toString());
        stats.put("uptime_seconds", Duration.between(startTime, Instant.now()).getSeconds());
        
        Runtime runtime = Runtime.getRuntime();
        stats.put("memory_used_mb", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024));
        stats.put("memory_total_mb", runtime.totalMemory() / (1024 * 1024));
        stats.put("memory_max_mb", runtime.maxMemory() / (1024 * 1024));
        
        return stats;
    }
    
    /**
     * Health check.
     */
    public boolean healthCheck() {
        return !gatewayServers.isEmpty();
    }
    
    /**
     * Ping test.
     */
    public String ping() {
        return "pong";
    }
    
    /**
     * Get JVM system info.
     */
    public Map<String, Object> getSystemInfo() {
        Map<String, Object> info = new HashMap<>();
        Runtime runtime = Runtime.getRuntime();
        info.put("java_version", System.getProperty("java.version"));
        info.put("java_vendor", System.getProperty("java.vendor"));
        info.put("os_name", System.getProperty("os.name"));
        info.put("os_arch", System.getProperty("os.arch"));
        info.put("available_processors", runtime.availableProcessors());
        info.put("memory_free_mb", runtime.freeMemory() / (1024 * 1024));
        info.put("memory_total_mb", runtime.totalMemory() / (1024 * 1024));
        info.put("memory_max_mb", runtime.maxMemory() / (1024 * 1024));
        return info;
    }
    
    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        String host = "localhost";
        int port = 25333;
        int poolSize = 4;
        String gatewayName = "primary";
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                case "--port":
                    if (i + 1 < args.length) port = Integer.parseInt(args[++i]);
                    break;
                case "-h":
                case "--host":
                    if (i + 1 < args.length) host = args[++i];
                    break;
                case "--pool-size":
                    if (i + 1 < args.length) poolSize = Integer.parseInt(args[++i]);
                    break;
                case "--name":
                    if (i + 1 < args.length) gatewayName = args[++i];
                    break;
                case "--help":
                    printHelp();
                    return;
                case "--version":
                    System.out.println("DishtaYantra Gateway v" + VERSION);
                    return;
            }
        }
        
        final DishtaYantraGateway gateway = new DishtaYantraGateway(host, port, poolSize);
        gateway.setGatewayName(gatewayName);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nReceived shutdown signal...");
            gateway.shutdown();
        }));
        
        gateway.start();
        
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            gateway.shutdown();
        }
    }
    
    private static void printHelp() {
        System.out.println(
            "DishtaYantra Java Gateway Server v" + VERSION + "\n\n" +
            "Usage: java -cp \"dishtayantra.jar:py4j.jar\" com.dishtayantra.gateway.DishtaYantraGateway [options]\n\n" +
            "Options:\n" +
            "  -p, --port PORT      Gateway port (default: 25333)\n" +
            "  -h, --host HOST      Bind address (default: localhost)\n" +
            "  --pool-size N        Number of gateway instances (default: 4)\n" +
            "  --name NAME          Gateway name (default: primary)\n" +
            "  --version            Show version\n" +
            "  --help               Show this help message\n"
        );
    }
}
