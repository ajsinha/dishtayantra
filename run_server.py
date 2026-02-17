#!/usr/bin/env python
"""
DishtaYantra Compute Server - Main Entry Point
Version: 1.7.5

IMPORTANT: This script must be run with `python run_server.py` 
The multiprocessing worker pool requires the __main__ guard.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import os
import sys
import json
import time
import signal
import atexit
import multiprocessing
import warnings
from datetime import datetime

# Suppress LMDB GIL warnings (Python 3.13+ free-threading mode)
warnings.filterwarnings("ignore", message=".*GIL.*lmdb.*", category=RuntimeWarning)

# Version information
VERSION = "1.7.5"
BUILD_DATE = "2025-02-12"

# CRITICAL: Set start method at module level before any other multiprocessing usage
def _setup_multiprocessing():
    """Setup multiprocessing start method"""
    try:
        # 'spawn' is safest and works on all platforms
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        pass

# Only setup if we're the main process
if __name__ == '__main__':
    _setup_multiprocessing()

import logging
from core.properties_configurator import PropertiesConfigurator

# Global references for cleanup
_webapp = None
_worker_pool = None
_jvm_manager = None
_cpp_manager = None
_rust_manager = None
_shutdown_in_progress = False


def _setup_logging():
    """Setup logging configuration"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/dagserver.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger(__name__)


def _print_startup_banner():
    """Print startup banner with ASCII art"""
    banner = f"""
================================================================================
                         DISHTAYANTRA COMPUTE SERVER
================================================================================

    ██████╗ ██╗███████╗██╗  ██╗████████╗ █████╗ ██╗   ██╗ █████╗ ███╗   ██╗████████╗██████╗  █████╗ 
    ██╔══██╗██║██╔════╝██║  ██║╚══██╔══╝██╔══██╗╚██╗ ██╔╝██╔══██╗████╗  ██║╚══██╔══╝██╔══██╗██╔══██╗
    ██║  ██║██║███████╗███████║   ██║   ███████║ ╚████╔╝ ███████║██╔██╗ ██║   ██║   ██████╔╝███████║
    ██║  ██║██║╚════██║██╔══██║   ██║   ██╔══██║  ╚██╔╝  ██╔══██║██║╚██╗██║   ██║   ██╔══██╗██╔══██║
    ██████╔╝██║███████║██║  ██║   ██║   ██║  ██║   ██║   ██║  ██║██║ ╚████║   ██║   ██║  ██║██║  ██║
    ╚═════╝ ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝

    Version: {VERSION}                                    Build: {BUILD_DATE}
    Python:  {sys.version.split()[0]}                                    Platform: {sys.platform}
    
    "The performance of C++, the safety of Rust, the ecosystem of Python"
    
    Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
================================================================================
"""
    print(banner)
    
    # Log startup to file
    logger.info("=" * 80)
    logger.info("DISHTAYANTRA COMPUTE SERVER - STARTUP SEQUENCE INITIATED")
    logger.info("=" * 80)
    logger.info(f"Version: {VERSION}")
    logger.info(f"Build Date: {BUILD_DATE}")
    logger.info(f"Python Version: {sys.version}")
    logger.info(f"Platform: {sys.platform}")
    logger.info(f"Process ID: {os.getpid()}")
    logger.info(f"Working Directory: {os.getcwd()}")
    logger.info(f"Startup Time: {datetime.now().isoformat()}")
    logger.info("-" * 80)


def _print_shutdown_banner():
    """Print shutdown banner"""
    banner = """
================================================================================
                    DISHTAYANTRA COMPUTE SERVER - SHUTDOWN
================================================================================
"""
    print(banner)
    
    logger.info("=" * 80)
    logger.info("DISHTAYANTRA COMPUTE SERVER - SHUTDOWN SEQUENCE INITIATED")
    logger.info("=" * 80)
    logger.info(f"Shutdown Time: {datetime.now().isoformat()}")
    logger.info("-" * 80)


def _log_component_status(component_name, status, details=None):
    """Log component initialization status"""
    status_icon = "✓" if status == "OK" else "⚠" if status == "WARN" else "✗"
    status_text = f"[{status}]"
    
    print(f"  {status_icon} {component_name}: {status_text}")
    
    log_msg = f"Component [{component_name}]: {status}"
    if details:
        log_msg += f" - {details}"
        print(f"      {details}")
    
    if status == "OK":
        logger.info(log_msg)
    elif status == "WARN":
        logger.warning(log_msg)
    else:
        logger.error(log_msg)


def _initialize_jvm_manager(props):
    """
    Initialize the JVM Manager for Py4J gateway connections.
    Only starts if explicitly enabled in configuration.
    """
    global _jvm_manager
    
    print("\n┌─ JVM Manager Initialization ─────────────────────────────────────────┐")
    logger.info("Checking JVM Manager configuration...")
    
    jvm_config_path = 'config/jvm_config.json'
    
    if not os.path.exists(jvm_config_path):
        _log_component_status("JVM Config", "SKIP", "Configuration file not found")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    
    try:
        with open(jvm_config_path, 'r') as f:
            jvm_config = json.load(f)
        
        jvm_manager_config = jvm_config.get('jvm_manager', {})
        
        # Check if JVM Manager is enabled
        if not jvm_manager_config.get('enabled', False):
            _log_component_status("JVM Manager", "SKIP", "Disabled in configuration (enabled=false)")
            logger.info("JVM Manager is disabled in jvm_config.json - skipping initialization")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        # Check if auto-start is enabled
        if not jvm_manager_config.get('auto_start_on_load', False):
            _log_component_status("JVM Manager", "SKIP", "Auto-start disabled (auto_start_on_load=false)")
            logger.info("JVM Manager auto-start is disabled - skipping initialization")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        # Import and initialize
        from core.jvm import JVMManager, get_jvm_manager
        
        logger.info("JVM Manager is ENABLED - initializing Py4J gateway connections...")
        _log_component_status("JVM Config", "OK", f"Loaded from {jvm_config_path}")
        
        _jvm_manager = get_jvm_manager()
        _jvm_manager.load_config(jvm_config_path)
        
        if _jvm_manager.initialize(auto_connect=True):
            status = _jvm_manager.get_status()
            gateways = status.get('gateways', {})
            connected_count = sum(1 for g in gateways.values() if g.get('connected', False))
            total_gateways = len(gateways)
            
            _log_component_status("JVM Manager", "OK", f"{connected_count}/{total_gateways} gateway(s) connected")
            logger.info(f"JVM Manager initialized successfully with {connected_count} active connections")
            
            # Log gateway details
            for name, gw_status in gateways.items():
                conn_status = "Connected" if gw_status.get('connected') else "Disconnected"
                logger.info(f"  Gateway '{name}': {conn_status} (port {gw_status.get('port', 'N/A')})")
        else:
            _log_component_status("JVM Manager", "WARN", "Initialized but no gateways connected")
            logger.warning("JVM Manager initialized but no gateways are connected")
        
        print("└──────────────────────────────────────────────────────────────────────┘")
        return _jvm_manager
        
    except ImportError as e:
        _log_component_status("JVM Manager", "SKIP", f"Py4J not installed: {e}")
        logger.warning(f"Py4J library not available - Java calculators disabled: {e}")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    except Exception as e:
        _log_component_status("JVM Manager", "FAIL", str(e))
        logger.error(f"Failed to initialize JVM Manager: {e}", exc_info=True)
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None


def _initialize_cpp_manager(props):
    """Initialize the CPP Manager for pybind11 C++ calculator support."""
    global _cpp_manager
    
    print("\n┌─ CPP Manager Initialization ─────────────────────────────────────────┐")
    logger.info("Checking CPP Manager configuration...")
    
    cpp_config_path = 'config/cpp_config.json'
    
    if not os.path.exists(cpp_config_path):
        _log_component_status("CPP Config", "SKIP", "Configuration file not found")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    
    try:
        with open(cpp_config_path, 'r') as f:
            cpp_config = json.load(f)
        
        cpp_manager_config = cpp_config.get('cpp_manager', {})
        
        if not cpp_manager_config.get('enabled', False):
            _log_component_status("CPP Manager", "SKIP", "Disabled in configuration (enabled=false)")
            logger.info("CPP Manager is disabled in cpp_config.json - skipping initialization")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        if not cpp_manager_config.get('auto_load_on_startup', False):
            _log_component_status("CPP Manager", "SKIP", "Auto-load disabled (auto_load_on_startup=false)")
            logger.info("CPP Manager auto-load is disabled - skipping initialization")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        from core.cpp import CPPManager, get_cpp_manager
        
        logger.info("CPP Manager is ENABLED - loading pybind11 modules...")
        _log_component_status("CPP Config", "OK", f"Loaded from {cpp_config_path}")
        
        _cpp_manager = get_cpp_manager()
        
        if _cpp_manager.initialize(cpp_config_path):
            status = _cpp_manager.get_status()
            modules_loaded = status.get('modules_loaded', 0)
            calculators_defined = status.get('calculators_defined', 0)
            
            _log_component_status("CPP Manager", "OK", f"{modules_loaded} module(s), {calculators_defined} calculator(s)")
            logger.info(f"CPP Manager initialized: {modules_loaded} modules, {calculators_defined} calculators available")
        else:
            _log_component_status("CPP Manager", "WARN", "Initialized but no modules loaded")
            logger.warning("CPP Manager initialized but no C++ modules were loaded")
        
        print("└──────────────────────────────────────────────────────────────────────┘")
        return _cpp_manager
    
    except ImportError as e:
        _log_component_status("CPP Manager", "SKIP", f"pybind11 modules not available: {e}")
        logger.warning(f"CPP Manager not available: {e}")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    except Exception as e:
        _log_component_status("CPP Manager", "FAIL", str(e))
        logger.error(f"Failed to initialize CPP Manager: {e}", exc_info=True)
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None


def _initialize_rust_manager(props):
    """Initialize the Rust Manager for PyO3 Rust calculator support."""
    global _rust_manager
    
    print("\n┌─ Rust Manager Initialization ────────────────────────────────────────┐")
    logger.info("Checking Rust Manager configuration...")
    
    rust_config_path = 'config/rust_config.json'
    
    if not os.path.exists(rust_config_path):
        _log_component_status("Rust Config", "SKIP", "Configuration file not found")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    
    try:
        with open(rust_config_path, 'r') as f:
            rust_config = json.load(f)
        
        rust_manager_config = rust_config.get('rust_manager', {})
        
        if not rust_manager_config.get('enabled', False):
            _log_component_status("Rust Manager", "SKIP", "Disabled in configuration (enabled=false)")
            logger.info("Rust Manager is disabled in rust_config.json - skipping initialization")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        if not rust_manager_config.get('auto_load_on_startup', False):
            _log_component_status("Rust Manager", "SKIP", "Auto-load disabled (auto_load_on_startup=false)")
            logger.info("Rust Manager auto-load is disabled - skipping initialization")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        from core.rust import RustManager, get_rust_manager
        
        logger.info("Rust Manager is ENABLED - loading PyO3 modules...")
        _log_component_status("Rust Config", "OK", f"Loaded from {rust_config_path}")
        
        _rust_manager = get_rust_manager()
        
        if _rust_manager.initialize(rust_config_path):
            status = _rust_manager.get_status()
            modules_loaded = status.get('modules_loaded', 0)
            calculators_defined = status.get('calculators_defined', 0)
            
            _log_component_status("Rust Manager", "OK", f"{modules_loaded} module(s), {calculators_defined} calculator(s)")
            logger.info(f"Rust Manager initialized: {modules_loaded} modules, {calculators_defined} calculators available")
        else:
            _log_component_status("Rust Manager", "WARN", "Initialized but no modules loaded")
            logger.warning("Rust Manager initialized but no Rust modules were loaded")
        
        print("└──────────────────────────────────────────────────────────────────────┘")
        return _rust_manager
    
    except ImportError as e:
        _log_component_status("Rust Manager", "SKIP", f"PyO3 modules not available: {e}")
        logger.warning(f"Rust Manager not available: {e}")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    except Exception as e:
        _log_component_status("Rust Manager", "FAIL", str(e))
        logger.error(f"Failed to initialize Rust Manager: {e}", exc_info=True)
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None


def _initialize_worker_pool(props):
    """Initialize the worker pool for multiprocessing support."""
    global _worker_pool
    
    print("\n┌─ Worker Pool Initialization ─────────────────────────────────────────┐")
    logger.info("Checking Worker Pool configuration...")
    
    worker_config_path = 'config/worker_config.json'
    
    if not os.path.exists(worker_config_path):
        _log_component_status("Worker Config", "SKIP", "Configuration file not found")
        _log_component_status("Worker Pool", "SKIP", "Running in single-process mode")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None
    
    try:
        with open(worker_config_path, 'r') as f:
            worker_config = json.load(f)
        
        pool_config = worker_config.get('worker_pool', {})
        
        if not pool_config.get('enabled', False):
            _log_component_status("Worker Pool", "SKIP", "Disabled in configuration (enabled=false)")
            logger.info("Worker Pool is disabled - running in single-process mode")
            print("└──────────────────────────────────────────────────────────────────────┘")
            return None
        
        from core.workers import WorkerPoolManager
        
        logger.info("Worker Pool is ENABLED - starting worker processes...")
        _log_component_status("Worker Config", "OK", f"Loaded from {worker_config_path}")
        
        _worker_pool = WorkerPoolManager(
            config_path=worker_config_path,
            config=worker_config
        )
        
        _worker_pool.start()
        
        num_workers = _worker_pool.num_workers
        _log_component_status("Worker Pool", "OK", f"Started {num_workers} worker process(es)")
        logger.info(f"Worker Pool started with {num_workers} workers")
        
        # Worker stabilization
        stabilization_seconds = pool_config.get('worker_stabilization_seconds', 10)
        if stabilization_seconds > 0:
            print(f"      Waiting {stabilization_seconds}s for workers to stabilize...")
            logger.info(f"Waiting {stabilization_seconds}s for worker stabilization...")
            time.sleep(stabilization_seconds)
            _log_component_status("Worker Stabilization", "OK", "All workers ready")
            logger.info("Worker stabilization complete - all workers ready")
        
        print("└──────────────────────────────────────────────────────────────────────┘")
        return _worker_pool
        
    except Exception as e:
        _log_component_status("Worker Pool", "FAIL", str(e))
        logger.error(f"Failed to initialize worker pool: {e}", exc_info=True)
        _log_component_status("Fallback", "OK", "Continuing in single-process mode")
        print("└──────────────────────────────────────────────────────────────────────┘")
        return None


def _graceful_shutdown(signum=None, frame=None):
    """Perform graceful shutdown of all components"""
    global _shutdown_in_progress, _webapp, _worker_pool, _jvm_manager, _cpp_manager, _rust_manager
    
    if _shutdown_in_progress:
        logger.info("Shutdown already in progress...")
        return
    
    _shutdown_in_progress = True
    
    _print_shutdown_banner()
    
    # Shutdown web application
    if _webapp:
        print("  • Shutting down Web Application...")
        logger.info("Shutting down Web Application...")
        try:
            _webapp.shutdown()
            logger.info("Web Application shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down Web Application: {e}")
    
    # Shutdown worker pool
    if _worker_pool:
        print("  • Shutting down Worker Pool...")
        logger.info("Shutting down Worker Pool...")
        try:
            _worker_pool.stop()
            logger.info("Worker Pool shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down Worker Pool: {e}")
    
    # Shutdown JVM Manager
    if _jvm_manager:
        print("  • Shutting down JVM Manager...")
        logger.info("Shutting down JVM Manager and Py4J gateways...")
        try:
            _jvm_manager.shutdown()
            logger.info("JVM Manager shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down JVM Manager: {e}")
    
    # Shutdown CPP Manager
    if _cpp_manager:
        print("  • Shutting down CPP Manager...")
        logger.info("Shutting down CPP Manager...")
        try:
            _cpp_manager.shutdown()
            logger.info("CPP Manager shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down CPP Manager: {e}")
    
    # Shutdown Rust Manager
    if _rust_manager:
        print("  • Shutting down Rust Manager...")
        logger.info("Shutting down Rust Manager...")
        try:
            _rust_manager.shutdown()
            logger.info("Rust Manager shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down Rust Manager: {e}")
    
    logger.info("=" * 80)
    logger.info("DISHTAYANTRA COMPUTE SERVER - SHUTDOWN COMPLETE")
    logger.info(f"Shutdown Time: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    print("\n================================================================================")
    print("                    SHUTDOWN COMPLETE - GOODBYE!")
    print("================================================================================\n")


def main():
    """Main entry point"""
    global _webapp, _worker_pool
    
    # Setup logging first
    _setup_logging()
    
    # Print startup banner
    _print_startup_banner()
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    atexit.register(_graceful_shutdown)
    
    # Create necessary directories
    print("\n┌─ Directory Setup ────────────────────────────────────────────────────┐")
    directories = ['logs', 'config', 'config/dags', 'data']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            _log_component_status(f"Directory '{directory}'", "OK", "Created")
            logger.info(f"Created directory: {directory}")
        else:
            logger.debug(f"Directory exists: {directory}")
    _log_component_status("Directories", "OK", "All required directories ready")
    print("└──────────────────────────────────────────────────────────────────────┘")
    
    # Import webapp here (after multiprocessing setup)
    from web.dishtyantra_webapp import DishtaYantraWebApp

    try:
        # Load application properties
        print("\n┌─ Application Properties ─────────────────────────────────────────────┐")
        logger.info("Loading application properties...")
        props = PropertiesConfigurator(['config/application.properties'])
        _log_component_status("Application Properties", "OK", "Loaded from config/application.properties")
        print("└──────────────────────────────────────────────────────────────────────┘")

        # Load external module paths
        external_module_paths = props.get_values_by_pattern(r'^external\.module\.path\.')
        if external_module_paths:
            print("\n┌─ External Module Paths ──────────────────────────────────────────────┐")
            logger.info("Loading external module paths...")
            for module_path in external_module_paths:
                resolved_path = os.path.expandvars(module_path)
                resolved_path = os.path.expanduser(resolved_path)
                if not os.path.isabs(resolved_path):
                    resolved_path = os.path.abspath(resolved_path)
                
                if os.path.exists(resolved_path):
                    if resolved_path not in sys.path:
                        sys.path.insert(0, resolved_path)
                        _log_component_status("Module Path", "OK", resolved_path)
                        logger.info(f"Added to Python path: {resolved_path}")
                else:
                    _log_component_status("Module Path", "WARN", f"Not found: {resolved_path}")
                    logger.warning(f"External module path does not exist: {resolved_path}")
            print("└──────────────────────────────────────────────────────────────────────┘")

        # Initialize components
        jvm_manager = _initialize_jvm_manager(props)
        cpp_manager = _initialize_cpp_manager(props)
        rust_manager = _initialize_rust_manager(props)
        _worker_pool = _initialize_worker_pool(props)

        # Initialize web application
        print("\n┌─ Web Application Initialization ────────────────────────────────────┐")
        logger.info("Initializing DishtaYantra Web Application...")
        _webapp = DishtaYantraWebApp.get_instance(worker_pool=_worker_pool)
        _log_component_status("Web Application", "OK", "Flask application initialized")
        logger.info("DishtaYantra Web Application initialized successfully")
        print("└──────────────────────────────────────────────────────────────────────┘")

        # Get server configuration
        host = props.get('server.host', '0.0.0.0')
        port = props.get_int('server.port', 5002)
        debug = props.get('server.debug', 'False').lower() == 'true'
        cert_file = props.get('server.cert.file', None)
        key_file = props.get('server.key.file', None)

        ssl_context = None
        if cert_file and key_file:
            ssl_context = (cert_file, key_file)

        # Print server configuration summary
        print("\n┌─ Server Configuration ───────────────────────────────────────────────┐")
        _log_component_status("Host", "OK", host)
        _log_component_status("Port", "OK", str(port))
        _log_component_status("Debug Mode", "OK", "Enabled" if debug else "Disabled")
        _log_component_status("SSL/TLS", "OK", "Enabled" if ssl_context else "Disabled")
        _log_component_status("Worker Pool", "OK", f"Enabled ({_worker_pool.num_workers} workers)" if _worker_pool else "Disabled (single-process)")
        _log_component_status("JVM Manager", "OK", "Enabled" if jvm_manager else "Disabled")
        _log_component_status("CPP Manager", "OK", "Enabled" if cpp_manager else "Disabled")
        _log_component_status("Rust Manager", "OK", "Enabled" if rust_manager else "Disabled")
        print("└──────────────────────────────────────────────────────────────────────┘")

        # Log final startup summary
        logger.info("-" * 80)
        logger.info("STARTUP SEQUENCE COMPLETE - SERVER CONFIGURATION:")
        logger.info(f"  Host: {host}")
        logger.info(f"  Port: {port}")
        logger.info(f"  Debug: {debug}")
        logger.info(f"  SSL: {'Enabled' if ssl_context else 'Disabled'}")
        logger.info(f"  Worker Pool: {'Enabled (' + str(_worker_pool.num_workers) + ' workers)' if _worker_pool else 'Disabled'}")
        logger.info(f"  JVM Manager: {'Enabled' if jvm_manager else 'Disabled'}")
        logger.info(f"  CPP Manager: {'Enabled' if cpp_manager else 'Disabled'}")
        logger.info(f"  Rust Manager: {'Enabled' if rust_manager else 'Disabled'}")
        logger.info("-" * 80)
        logger.info(f"SERVER READY - Listening on http{'s' if ssl_context else ''}://{host}:{port}")
        logger.info("=" * 80)

        # Start the web application
        print(f"""
================================================================================
                         SERVER READY
================================================================================
    
    Access the web interface at: http{'s' if ssl_context else ''}://{host}:{port}
    
    Default credentials:
      Username: admin
      Password: admin123
    
    Press Ctrl+C to shutdown gracefully.
    
================================================================================
""")

        _webapp.start(host=host, port=port, debug=debug, ssl_context=ssl_context)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt (SIGINT)")
        _graceful_shutdown()
    except Exception as e:
        logger.error(f"Fatal error during startup: {str(e)}", exc_info=True)
        print(f"\n  ✗ FATAL ERROR: {str(e)}")
        _graceful_shutdown()
        sys.exit(1)


if __name__ == '__main__':
    main()
