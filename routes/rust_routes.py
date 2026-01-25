"""
Rust Management Routes for DishtaYantra Web UI
Version: 1.7.0

Provides web interface and API for managing Rust PyO3 modules and calculators.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
from functools import wraps
from flask import Blueprint, render_template, jsonify, request

logger = logging.getLogger(__name__)


def require_rust_manager(f):
    """Decorator to ensure Rust Manager is available"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            from core.rust import get_rust_manager
            rust_manager = get_rust_manager()
            return f(rust_manager=rust_manager, *args, **kwargs)
        except ImportError:
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({"error": "Rust Manager not available"}), 503
            return render_template('rust/not_available.html')
    return decorated_function


class RustRoutes:
    """
    Routes for Rust module and calculator management.
    
    Web UI Routes:
    - /rust - Management dashboard
    - /rust/calculators - Calculator listing
    - /rust/calculator/<name> - Calculator details
    - /rust/module/<name> - Module details
    
    API Routes:
    - GET /api/rust/status - Manager status
    - GET /api/rust/modules - List modules
    - GET /api/rust/modules/<name> - Module details
    - POST /api/rust/modules/<name>/load - Load module
    - POST /api/rust/modules/<name>/unload - Unload module
    - POST /api/rust/modules/<name>/reload - Reload module
    - POST /api/rust/modules/<name>/build - Build module
    - GET /api/rust/calculators - List calculators
    - GET /api/rust/calculators/<name> - Calculator details
    - POST /api/rust/calculators/<name>/test - Test calculator
    - GET /api/rust/config - Get configuration
    - POST /api/rust/reload-config - Reload configuration
    """
    
    def __init__(self, app):
        self.app = app
        self._register_routes()
    
    def _register_routes(self):
        """Register all routes with the Flask app"""
        
        # ============================================================
        # Web UI Routes
        # ============================================================
        
        @self.app.route('/rust')
        @require_rust_manager
        def rust_management(rust_manager):
            """Rust management dashboard"""
            status = rust_manager.get_status()
            modules = rust_manager.list_modules()
            calculators = rust_manager.list_calculators()
            
            return render_template('rust/management.html',
                                 status=status,
                                 modules=modules,
                                 calculators=calculators,
                                 config=rust_manager._config)
        
        @self.app.route('/rust/calculators')
        @require_rust_manager
        def rust_calculators(rust_manager):
            """List all Rust calculators"""
            calculators = rust_manager.list_calculators()
            return render_template('rust/calculators.html',
                                 calculators=calculators)
        
        @self.app.route('/rust/calculator/<name>')
        @require_rust_manager
        def rust_calculator_details(rust_manager, name):
            """Rust calculator details page"""
            info = rust_manager.get_calculator_info(name)
            if not info:
                return render_template('error.html', 
                                     message=f"Calculator '{name}' not found"), 404
            
            # Generate example DAG config
            example_dag = {
                "name": f"rust_{name.lower()}_node",
                "type": "CalculatorNode",
                "calculator": {
                    "type": "rust",
                    "name": name
                },
                "config": info.get('default_config', {})
            }
            
            return render_template('rust/calculator_details.html',
                                 calculator=info,
                                 example_dag=json.dumps(example_dag, indent=2))
        
        @self.app.route('/rust/module/<name>')
        @require_rust_manager
        def rust_module_details(rust_manager, name):
            """Rust module details page"""
            info = rust_manager.get_module_info(name)
            if not info:
                return render_template('error.html',
                                     message=f"Module '{name}' not found"), 404
            
            # Get calculators for this module
            calculators = [c for c in rust_manager.list_calculators() 
                          if c['module'] == name]
            
            return render_template('rust/module_details.html',
                                 module=info,
                                 calculators=calculators)
        
        # ============================================================
        # API Routes
        # ============================================================
        
        @self.app.route('/api/rust/status')
        @require_rust_manager
        def api_rust_status(rust_manager):
            """Get Rust Manager status"""
            return jsonify(rust_manager.get_status())
        
        @self.app.route('/api/rust/modules')
        @require_rust_manager
        def api_rust_modules(rust_manager):
            """List all configured modules"""
            return jsonify(rust_manager.list_modules())
        
        @self.app.route('/api/rust/modules/<name>')
        @require_rust_manager
        def api_rust_module_info(rust_manager, name):
            """Get module details"""
            info = rust_manager.get_module_info(name)
            if info:
                return jsonify(info)
            return jsonify({"error": f"Module '{name}' not found"}), 404
        
        @self.app.route('/api/rust/modules/<name>/load', methods=['POST'])
        @require_rust_manager
        def api_rust_load_module(rust_manager, name):
            """Load a Rust module"""
            try:
                success = rust_manager.load_module(name)
                if success:
                    return jsonify({
                        "success": True,
                        "message": f"Module '{name}' loaded successfully"
                    })
                else:
                    error = rust_manager.module_errors.get(name, "Unknown error")
                    return jsonify({
                        "success": False,
                        "error": error
                    }), 400
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500
        
        @self.app.route('/api/rust/modules/<name>/unload', methods=['POST'])
        @require_rust_manager
        def api_rust_unload_module(rust_manager, name):
            """Unload a Rust module"""
            try:
                success = rust_manager.unload_module(name)
                return jsonify({
                    "success": success,
                    "message": f"Module '{name}' unloaded" if success else "Module not loaded"
                })
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500
        
        @self.app.route('/api/rust/modules/<name>/reload', methods=['POST'])
        @require_rust_manager
        def api_rust_reload_module(rust_manager, name):
            """Reload a Rust module"""
            try:
                success = rust_manager.reload_module(name)
                if success:
                    return jsonify({
                        "success": True,
                        "message": f"Module '{name}' reloaded successfully"
                    })
                else:
                    error = rust_manager.module_errors.get(name, "Unknown error")
                    return jsonify({
                        "success": False,
                        "error": error
                    }), 400
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500
        
        @self.app.route('/api/rust/modules/<name>/build', methods=['POST'])
        @require_rust_manager
        def api_rust_build_module(rust_manager, name):
            """Build a Rust module with maturin/cargo"""
            try:
                success, output = rust_manager.build_module(name)
                return jsonify({
                    "success": success,
                    "output": output
                })
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500
        
        @self.app.route('/api/rust/calculators')
        @require_rust_manager
        def api_rust_calculators(rust_manager):
            """List all calculator definitions"""
            return jsonify(rust_manager.list_calculators())
        
        @self.app.route('/api/rust/calculators/<name>')
        @require_rust_manager
        def api_rust_calculator_info(rust_manager, name):
            """Get calculator details"""
            info = rust_manager.get_calculator_info(name)
            if info:
                return jsonify(info)
            return jsonify({"error": f"Calculator '{name}' not found"}), 404
        
        @self.app.route('/api/rust/calculators/<name>/test', methods=['POST'])
        @require_rust_manager
        def api_rust_test_calculator(rust_manager, name):
            """Test a calculator with sample data"""
            try:
                # Get test data from request or use default
                test_data = request.get_json() or {"values": [1.0, 2.0, 3.0, 4.0, 5.0]}
                
                # Get config override
                config_override = test_data.pop('_config', {})
                
                # Create calculator
                calc = rust_manager.create_calculator(name, f"test_{name}", config_override)
                
                # Run calculation
                import time
                start = time.time()
                result = calc.calculate(test_data)
                elapsed = (time.time() - start) * 1000  # ms
                
                return jsonify({
                    "success": True,
                    "input": test_data,
                    "output": result,
                    "elapsed_ms": round(elapsed, 3)
                })
                
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500
        
        @self.app.route('/api/rust/config')
        @require_rust_manager
        def api_rust_config(rust_manager):
            """Get current configuration"""
            return jsonify(rust_manager._config)
        
        @self.app.route('/api/rust/reload-config', methods=['POST'])
        @require_rust_manager
        def api_rust_reload_config(rust_manager):
            """Reload configuration from file"""
            try:
                rust_manager._reload_config()
                return jsonify({
                    "success": True,
                    "message": "Configuration reloaded"
                })
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500
