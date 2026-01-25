#!/usr/bin/env python
"""
Main entry point for DishtaYantra Compute Server

IMPORTANT: This script must be run with `python run_server.py` 
The multiprocessing worker pool requires the __main__ guard.
"""

import os
import sys
import json
import time
import multiprocessing
import warnings

# Suppress LMDB GIL warnings (Python 3.13+ free-threading mode)
warnings.filterwarnings("ignore", message=".*GIL.*lmdb.*", category=RuntimeWarning)

# CRITICAL: Set start method at module level before any other multiprocessing usage
# This ensures consistent behavior across all platforms (Windows, macOS, Linux)
def _setup_multiprocessing():
    """Setup multiprocessing start method"""
    try:
        # 'spawn' is safest and works on all platforms
        # It starts a fresh Python interpreter for each worker process
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        # Already set - this is fine
        pass

# Only setup if we're the main process
if __name__ == '__main__':
    _setup_multiprocessing()

import logging
from core.properties_configurator import PropertiesConfigurator

# Configure logging - but only set up file handler in main process
def _setup_logging():
    """Setup logging configuration"""
    # Ensure logs directory exists
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/dagserver.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger(__name__)


def _initialize_jvm_manager(props):
    """
    Initialize the JVM Manager for Py4J gateway connections.
    
    v1.6.0: JVM Manager provides centralized management of JVM instances
    and Py4J gateways for Java calculator integration.
    
    Args:
        props: PropertiesConfigurator instance
        
    Returns:
        JVMManager instance or None if disabled/failed
    """
    jvm_config_path = 'config/jvm_config.json'
    
    if not os.path.exists(jvm_config_path):
        logger.info("JVM configuration not found, Java calculators will not be available")
        return None
    
    try:
        with open(jvm_config_path, 'r') as f:
            jvm_config = json.load(f)
        
        jvm_manager_config = jvm_config.get('jvm_manager', {})
        
        if not jvm_manager_config.get('enabled', True):
            logger.info("JVM Manager is disabled in configuration")
            return None
        
        if not jvm_manager_config.get('auto_start_on_load', True):
            logger.info("JVM Manager auto-start is disabled")
            return None
        
        # Import here to avoid circular imports
        from core.jvm import JVMManager, get_jvm_manager
        
        print("\n✓ Initializing JVM Manager...")
        logger.info("Initializing JVM Manager for Py4J gateway connections...")
        
        jvm_manager = get_jvm_manager()
        jvm_manager.load_config(jvm_config_path)
        
        # Initialize without auto-connect (we'll connect after checking JVM availability)
        if jvm_manager.initialize(auto_connect=True):
            status = jvm_manager.get_status()
            gateways = status.get('gateways', {})
            connected_count = sum(1 for g in gateways.values() if g.get('connected', False))
            print(f"  • JVM Manager initialized ({connected_count} gateway(s) connected)")
            logger.info(f"JVM Manager initialized with {connected_count} gateway connections")
            return jvm_manager
        else:
            print("  ⚠ JVM Manager initialized but no gateways connected")
            logger.warning("JVM Manager initialized but no gateways connected")
            return jvm_manager
        
    except ImportError as e:
        logger.warning(f"Py4J not available, skipping JVM initialization: {e}")
        print("  ⚠ Py4J not installed, Java calculators not available")
        return None
    except Exception as e:
        logger.error(f"Failed to initialize JVM Manager: {e}")
        print(f"  ⚠ Warning: JVM Manager failed to initialize: {e}")
        return None


def _initialize_cpp_manager(props):
    """
    Initialize the CPP Manager for pybind11 C++ calculator support.
    
    v1.7.0: CPP Manager provides centralized management of C++ modules
    and calculator instances compiled with pybind11.
    
    Args:
        props: PropertiesConfigurator instance
        
    Returns:
        CPPManager instance or None if disabled/failed
    """
    cpp_config_path = 'config/cpp_config.json'
    
    if not os.path.exists(cpp_config_path):
        logger.info("CPP configuration not found, C++ calculators will not be available")
        return None
    
    try:
        with open(cpp_config_path, 'r') as f:
            cpp_config = json.load(f)
        
        cpp_manager_config = cpp_config.get('cpp_manager', {})
        
        if not cpp_manager_config.get('enabled', True):
            logger.info("CPP Manager is disabled in configuration")
            return None
        
        if not cpp_manager_config.get('auto_load_on_startup', True):
            logger.info("CPP Manager auto-load is disabled")
            return None
        
        # Import here to avoid circular imports
        from core.cpp import CPPManager, get_cpp_manager
        
        print("\n✓ Initializing CPP Manager...")
        logger.info("Initializing CPP Manager for pybind11 C++ calculators...")
        
        cpp_manager = get_cpp_manager()
        
        if cpp_manager.initialize(cpp_config_path):
            status = cpp_manager.get_status()
            modules_loaded = status.get('modules_loaded', 0)
            calculators_defined = status.get('calculators_defined', 0)
            
            print(f"  • CPP Manager initialized ({modules_loaded} module(s), {calculators_defined} calculator(s))")
            logger.info(f"CPP Manager initialized: {modules_loaded} modules, {calculators_defined} calculators")
            return cpp_manager
        else:
            print("  ⚠ CPP Manager initialized but no modules loaded")
            logger.warning("CPP Manager initialized but no modules loaded")
            return cpp_manager
    
    except ImportError as e:
        logger.warning(f"CPP Manager not available: {e}")
        print(f"  ⚠ Note: CPP Manager not available")
        return None
    except Exception as e:
        logger.error(f"Failed to initialize CPP Manager: {e}")
        print(f"  ⚠ Warning: CPP Manager failed to initialize: {e}")
        return None


def _initialize_rust_manager(props):
    """
    Initialize the Rust Manager for PyO3 Rust calculator support.
    
    v1.7.0: Rust Manager provides centralized management of Rust modules
    and calculator instances compiled with PyO3/maturin.
    
    Args:
        props: PropertiesConfigurator instance
        
    Returns:
        RustManager instance or None if disabled/failed
    """
    rust_config_path = 'config/rust_config.json'
    
    if not os.path.exists(rust_config_path):
        logger.info("Rust configuration not found, Rust calculators will not be available")
        return None
    
    try:
        with open(rust_config_path, 'r') as f:
            rust_config = json.load(f)
        
        rust_manager_config = rust_config.get('rust_manager', {})
        
        if not rust_manager_config.get('enabled', True):
            logger.info("Rust Manager is disabled in configuration")
            return None
        
        if not rust_manager_config.get('auto_load_on_startup', True):
            logger.info("Rust Manager auto-load is disabled")
            return None
        
        # Import here to avoid circular imports
        from core.rust import RustManager, get_rust_manager
        
        print("\n✓ Initializing Rust Manager...")
        logger.info("Initializing Rust Manager for PyO3 Rust calculators...")
        
        rust_manager = get_rust_manager()
        
        if rust_manager.initialize(rust_config_path):
            status = rust_manager.get_status()
            modules_loaded = status.get('modules_loaded', 0)
            calculators_defined = status.get('calculators_defined', 0)
            
            print(f"  • Rust Manager initialized ({modules_loaded} module(s), {calculators_defined} calculator(s))")
            logger.info(f"Rust Manager initialized: {modules_loaded} modules, {calculators_defined} calculators")
            return rust_manager
        else:
            print("  ⚠ Rust Manager initialized but no modules loaded")
            logger.warning("Rust Manager initialized but no modules loaded")
            return rust_manager
    
    except ImportError as e:
        logger.warning(f"Rust Manager not available: {e}")
        print(f"  ⚠ Note: Rust Manager not available")
        return None
    except Exception as e:
        logger.error(f"Failed to initialize Rust Manager: {e}")
        print(f"  ⚠ Warning: Rust Manager failed to initialize: {e}")
        return None


def _initialize_worker_pool(props):
    """
    Initialize the worker pool early, before any other components.
    
    v1.5.2: Worker pool is now initialized here in run_server.py to ensure
    it's ready before DAGComputeServer or WebApp try to use it.
    
    Args:
        props: PropertiesConfigurator instance
        
    Returns:
        WorkerPoolManager instance or None if disabled/failed
    """
    worker_config_path = 'config/worker_config.json'
    
    if not os.path.exists(worker_config_path):
        logger.info("Worker pool configuration not found, running in single-process mode")
        return None
    
    try:
        with open(worker_config_path, 'r') as f:
            worker_config = json.load(f)
        
        pool_config = worker_config.get('worker_pool', {})
        
        if not pool_config.get('enabled', False):
            logger.info("Worker pool is disabled in configuration")
            return None
        
        # Import here to avoid circular imports
        from core.workers import WorkerPoolManager
        
        print("\n✓ Initializing Worker Pool...")
        logger.info("Initializing Worker Pool Manager...")
        
        worker_pool = WorkerPoolManager(
            config_path=worker_config_path,
            config=worker_config
        )
        
        # Start the worker pool (waits for workers to report ready)
        worker_pool.start()
        
        num_workers = worker_pool.num_workers
        print(f"  • Started {num_workers} worker processes")
        logger.info(f"Worker pool started with {num_workers} workers")
        
        # Worker stabilization wait
        stabilization_seconds = pool_config.get('worker_stabilization_seconds', 10)
        if stabilization_seconds > 0:
            print(f"  • Waiting {stabilization_seconds}s for workers to stabilize...")
            logger.info(f"Waiting {stabilization_seconds}s for workers to fully stabilize...")
            time.sleep(stabilization_seconds)
            print("  • Workers ready")
            logger.info("Worker stabilization complete - ready to dispatch DAGs")
        
        return worker_pool
        
    except Exception as e:
        logger.error(f"Failed to initialize worker pool: {e}")
        print(f"  ⚠ Warning: Worker pool failed to initialize: {e}")
        print("  • Continuing in single-process mode")
        return None


def main():
    """Main entry point"""
    # Setup logging first
    _setup_logging()
    
    logger.info("Starting DishtaYantra Compute Server")

    # Create necessary directories
    directories = ['logs', 'config', 'config/dags']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

    # Import webapp here (after multiprocessing setup)
    from web.dishtyantra_webapp import DishtaYantraWebApp
    
    # Initialize web application singleton
    webapp = None
    worker_pool = None

    try:
        print("\n✓ Loading application properties...")
        props = PropertiesConfigurator(['config/application.properties'])

        # Load external module paths and add to sys.path for dynamic imports
        external_module_paths = props.get_values_by_pattern(r'^external\.module\.path\.')

        if external_module_paths:
            print(f"\n✓ Loading external module paths...")
            for module_path in external_module_paths:
                # Expand environment variables and resolve path
                resolved_path = os.path.expandvars(module_path)
                resolved_path = os.path.expanduser(resolved_path)

                # Convert to absolute path if relative
                if not os.path.isabs(resolved_path):
                    resolved_path = os.path.abspath(resolved_path)

                # Add to sys.path if it exists and not already present
                if os.path.exists(resolved_path):
                    if resolved_path not in sys.path:
                        sys.path.insert(0, resolved_path)
                        logger.info(f"Added to Python path: {resolved_path}")
                        print(f"  • {resolved_path}")
                    else:
                        logger.debug(f"Path already in sys.path: {resolved_path}")
                else:
                    logger.warning(f"External module path does not exist: {resolved_path}")
                    print(f"  ⚠ Warning: Path does not exist: {resolved_path}")

        # v1.6.0: Initialize JVM Manager for Java calculator support
        jvm_manager = _initialize_jvm_manager(props)

        # v1.7.0: Initialize CPP Manager for C++ calculator support
        cpp_manager = _initialize_cpp_manager(props)

        # v1.7.0: Initialize Rust Manager for Rust calculator support
        rust_manager = _initialize_rust_manager(props)

        # v1.5.2: Initialize worker pool EARLY (before webapp and dag_server)
        # This ensures workers are ready before any DAGs try to start
        worker_pool = _initialize_worker_pool(props)

        # Get web application instance, passing the worker pool
        print("\n✓ Initializing DishtaYantra Web Application...")
        webapp = DishtaYantraWebApp.get_instance(worker_pool=worker_pool)

        # Get server configuration
        host = props.get('server.host', '0.0.0.0')
        port = props.get_int('server.port', 5002)
        debug = props.get('server.debug', 'False').lower() == 'true'

        cert_file = props.get('server.cert.file', None)
        key_file = props.get('server.key.file', None)

        # Prepare SSL context if certificates are provided
        ssl_context = None
        if cert_file and key_file:
            ssl_context = (cert_file, key_file)
            logger.info(f"SSL enabled with cert: {cert_file}, key: {key_file}")

        # Start the web application
        print(f"\n✓ Starting server on {host}:{port}")
        print(f"  Debug mode: {debug}")
        print(f"  SSL: {'Enabled' if ssl_context else 'Disabled'}")
        print(f"  Worker Pool: {'Enabled (' + str(worker_pool.num_workers) + ' workers)' if worker_pool else 'Disabled'}")
        print(f"  JVM Manager: {'Enabled' if jvm_manager else 'Disabled'}")
        print("\n" + "=" * 60)

        webapp.start(host=host, port=port, debug=debug, ssl_context=ssl_context)

    except KeyboardInterrupt:
        logger.info("\nReceived keyboard interrupt - shutting down gracefully...")
        if webapp:
            webapp.shutdown()
        elif worker_pool:
            # Shutdown worker pool if webapp wasn't initialized
            logger.info("Shutting down worker pool...")
            worker_pool.stop()
        else:
            logger.warning("Web application not initialized")
    except Exception as e:
        logger.error(f"Error running server: {str(e)}", exc_info=True)
        if webapp:
            webapp.shutdown()
        elif worker_pool:
            worker_pool.stop()
        else:
            logger.warning("Web application not initialized, cannot perform shutdown")
        sys.exit(1)


if __name__ == '__main__':
    main()