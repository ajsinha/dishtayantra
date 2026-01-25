"""
CPP Management Routes for DishtaYantra
Version: 1.7.0

Provides web UI and API endpoints for managing C++ pybind11 modules
and calculators.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import logging
from flask import render_template, jsonify, request
from functools import wraps

logger = logging.getLogger(__name__)


def require_cpp_manager(f):
    """Decorator to check if CPP Manager is available"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            from core.cpp import get_cpp_manager
            cpp_manager = get_cpp_manager()
            return f(cpp_manager=cpp_manager, *args, **kwargs)
        except ImportError:
            return jsonify({
                'success': False,
                'error': 'CPP Manager not available',
                'message': 'pybind11 integration is not installed or enabled'
            }), 503
        except Exception as e:
            logger.error(f"CPP Manager error: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    return decorated_function


class CPPRoutes:
    """Routes for CPP management"""
    
    def __init__(self, app):
        self.app = app
        self._register_routes()
    
    def _register_routes(self):
        """Register all CPP management routes"""
        # Web UI routes
        self.app.add_url_rule('/cpp', 'cpp_management', self.cpp_management)
        self.app.add_url_rule('/cpp/calculators', 'cpp_calculators', self.cpp_calculators)
        self.app.add_url_rule('/cpp/calculator/<calculator_name>', 'cpp_calculator_details', self.cpp_calculator_details)
        self.app.add_url_rule('/cpp/module/<module_name>', 'cpp_module_details', self.cpp_module_details)
        
        # API routes
        self.app.add_url_rule('/api/cpp/status', 'api_cpp_status', self.api_cpp_status)
        self.app.add_url_rule('/api/cpp/modules', 'api_cpp_modules', self.api_cpp_modules)
        self.app.add_url_rule('/api/cpp/modules/<module_name>', 'api_cpp_module_detail', self.api_cpp_module_detail)
        self.app.add_url_rule('/api/cpp/modules/<module_name>/load', 'api_cpp_module_load', 
                            self.api_cpp_module_load, methods=['POST'])
        self.app.add_url_rule('/api/cpp/modules/<module_name>/unload', 'api_cpp_module_unload', 
                            self.api_cpp_module_unload, methods=['POST'])
        self.app.add_url_rule('/api/cpp/modules/<module_name>/reload', 'api_cpp_module_reload', 
                            self.api_cpp_module_reload, methods=['POST'])
        self.app.add_url_rule('/api/cpp/modules/<module_name>/build', 'api_cpp_module_build', 
                            self.api_cpp_module_build, methods=['POST'])
        self.app.add_url_rule('/api/cpp/reload-config', 'api_cpp_reload_config', 
                            self.api_cpp_reload_config, methods=['POST'])
        self.app.add_url_rule('/api/cpp/calculators', 'api_cpp_calculators', self.api_cpp_calculators)
        self.app.add_url_rule('/api/cpp/calculators/<calculator_name>', 'api_cpp_calculator_detail', 
                            self.api_cpp_calculator_detail)
        self.app.add_url_rule('/api/cpp/calculators/<calculator_name>/test', 'api_cpp_calculator_test', 
                            self.api_cpp_calculator_test, methods=['POST'])
        self.app.add_url_rule('/api/cpp/config', 'api_cpp_config', self.api_cpp_config)
    
    # ========================================================================
    # Web UI Routes
    # ========================================================================
    
    def cpp_management(self):
        """CPP Management dashboard page"""
        cpp_available = False
        cpp_status = {}
        modules = []
        calculators = []
        
        try:
            from core.cpp import get_cpp_manager, is_cpp_available
            cpp_manager = get_cpp_manager()
            
            if not cpp_manager._initialized:
                cpp_manager.initialize()
            
            cpp_available = is_cpp_available()
            cpp_status = cpp_manager.get_status()
            modules = cpp_manager.list_modules()
            calculators = cpp_manager.list_calculators()
            
        except ImportError:
            logger.debug("CPP Manager not available")
        except Exception as e:
            logger.error(f"Error getting CPP status: {e}")
        
        return render_template('cpp/management.html',
                              app_name='DishtaYantra',
                              cpp_available=cpp_available,
                              cpp_status=cpp_status,
                              modules=modules,
                              calculators=calculators)
    
    def cpp_calculators(self):
        """CPP Calculators listing page"""
        calculators = []
        modules = []
        
        try:
            from core.cpp import get_cpp_manager
            cpp_manager = get_cpp_manager()
            
            if cpp_manager._initialized:
                calculators = cpp_manager.list_calculators()
                modules = cpp_manager.list_modules()
                
        except ImportError:
            pass
        except Exception as e:
            logger.error(f"Error listing calculators: {e}")
        
        return render_template('cpp/calculators.html',
                              app_name='DishtaYantra',
                              calculators=calculators,
                              modules=modules)
    
    def cpp_calculator_details(self, calculator_name):
        """Calculator details page"""
        try:
            from core.cpp import get_cpp_manager
            cpp_manager = get_cpp_manager()
            
            definition = cpp_manager.calculator_definitions.get(calculator_name)
            
            if not definition:
                return render_template('error.html',
                                      app_name='DishtaYantra',
                                      error=f"Calculator '{calculator_name}' not found"), 404
            
            # Get module status
            module_status = cpp_manager.module_status.get(definition.module)
            
            return render_template('cpp/calculator_details.html',
                                  app_name='DishtaYantra',
                                  calculator={
                                      'name': definition.name,
                                      'module': definition.module,
                                      'cpp_class': definition.cpp_class,
                                      'description': definition.description,
                                      'default_config': definition.default_config
                                  },
                                  module_loaded=module_status.loaded if module_status else False)
        
        except ImportError:
            return render_template('cpp/not_available.html',
                                  app_name='DishtaYantra')
        except Exception as e:
            logger.error(f"Error getting calculator details: {e}")
            return render_template('error.html',
                                  app_name='DishtaYantra',
                                  error=str(e)), 500
    
    def cpp_module_details(self, module_name):
        """Module details page"""
        try:
            from core.cpp import get_cpp_manager
            cpp_manager = get_cpp_manager()
            
            config = cpp_manager.module_configs.get(module_name)
            status = cpp_manager.module_status.get(module_name)
            
            if not config:
                return render_template('error.html',
                                      app_name='DishtaYantra',
                                      error=f"Module '{module_name}' not found"), 404
            
            # Get calculators from this module
            module_calculators = [
                c for c in cpp_manager.list_calculators()
                if c['module'] == module_name
            ]
            
            return render_template('cpp/module_details.html',
                                  app_name='DishtaYantra',
                                  module={
                                      'name': config.name,
                                      'enabled': config.enabled,
                                      'description': config.description,
                                      'path': config.path,
                                      'source_dir': config.source_dir,
                                      'build_dir': config.build_dir,
                                      'lmdb_enabled': config.lmdb_enabled,
                                      'lmdb_db_path': config.lmdb_db_path
                                  },
                                  status={
                                      'loaded': status.loaded if status else False,
                                      'path': status.path if status else None,
                                      'available_classes': status.available_classes if status else [],
                                      'load_time': status.load_time.isoformat() if status and status.load_time else None,
                                      'last_error': status.last_error if status else None
                                  },
                                  calculators=module_calculators)
        
        except ImportError:
            return render_template('cpp/not_available.html',
                                  app_name='DishtaYantra')
        except Exception as e:
            logger.error(f"Error getting module details: {e}")
            return render_template('error.html',
                                  app_name='DishtaYantra',
                                  error=str(e)), 500
    
    # ========================================================================
    # API Routes
    # ========================================================================
    
    @require_cpp_manager
    def api_cpp_status(self, cpp_manager):
        """Get CPP Manager status"""
        return jsonify({
            'success': True,
            'data': cpp_manager.get_status()
        })
    
    @require_cpp_manager
    def api_cpp_modules(self, cpp_manager):
        """List all C++ modules"""
        return jsonify({
            'success': True,
            'data': {
                'modules': cpp_manager.list_modules()
            }
        })
    
    @require_cpp_manager
    def api_cpp_module_detail(self, cpp_manager, module_name):
        """Get details for a specific module"""
        config = cpp_manager.module_configs.get(module_name)
        status = cpp_manager.module_status.get(module_name)
        
        if not config:
            return jsonify({
                'success': False,
                'error': f'Unknown module: {module_name}'
            }), 404
        
        return jsonify({
            'success': True,
            'data': {
                'name': config.name,
                'enabled': config.enabled,
                'description': config.description,
                'path': config.path,
                'source_dir': config.source_dir,
                'build_dir': config.build_dir,
                'lmdb_enabled': config.lmdb_enabled,
                'loaded': status.loaded if status else False,
                'available_classes': status.available_classes if status else [],
                'load_time': status.load_time.isoformat() if status and status.load_time else None,
                'last_error': status.last_error if status else None
            }
        })
    
    @require_cpp_manager
    def api_cpp_module_load(self, cpp_manager, module_name):
        """Load a C++ module"""
        if module_name not in cpp_manager.module_configs:
            return jsonify({
                'success': False,
                'error': f'Unknown module: {module_name}'
            }), 404
        
        success = cpp_manager.load_module(module_name)
        
        return jsonify({
            'success': success,
            'message': f'Module {module_name} {"loaded" if success else "failed to load"}'
        })
    
    @require_cpp_manager
    def api_cpp_module_unload(self, cpp_manager, module_name):
        """Unload a C++ module"""
        success = cpp_manager.unload_module(module_name)
        
        return jsonify({
            'success': success,
            'message': f'Module {module_name} unloaded'
        })
    
    @require_cpp_manager
    def api_cpp_module_reload(self, cpp_manager, module_name):
        """Reload a C++ module"""
        success = cpp_manager.reload_module(module_name)
        
        return jsonify({
            'success': success,
            'message': f'Module {module_name} {"reloaded" if success else "failed to reload"}'
        })
    
    @require_cpp_manager
    def api_cpp_module_build(self, cpp_manager, module_name):
        """Build a C++ module using CMake"""
        success = cpp_manager.build_module(module_name)
        
        return jsonify({
            'success': success,
            'message': f'Module {module_name} {"built successfully" if success else "build failed"}'
        })
    
    @require_cpp_manager
    def api_cpp_reload_config(self, cpp_manager):
        """Reload CPP configuration from file"""
        try:
            cpp_manager._reload_config()
            return jsonify({
                'success': True,
                'message': 'Configuration reloaded'
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })
    
    @require_cpp_manager
    def api_cpp_calculators(self, cpp_manager):
        """List all C++ calculators"""
        return jsonify({
            'success': True,
            'data': {
                'definitions': cpp_manager.list_calculators()
            }
        })
    
    @require_cpp_manager
    def api_cpp_calculator_detail(self, cpp_manager, calculator_name):
        """Get details for a specific calculator"""
        definition = cpp_manager.calculator_definitions.get(calculator_name)
        
        if not definition:
            return jsonify({
                'success': False,
                'error': f'Unknown calculator: {calculator_name}'
            }), 404
        
        module_loaded = cpp_manager.is_module_loaded(definition.module)
        
        return jsonify({
            'success': True,
            'data': {
                'name': definition.name,
                'module': definition.module,
                'cpp_class': definition.cpp_class,
                'description': definition.description,
                'default_config': definition.default_config,
                'available': module_loaded
            }
        })
    
    @require_cpp_manager
    def api_cpp_calculator_test(self, cpp_manager, calculator_name):
        """Test a C++ calculator with sample data"""
        # Get test data from request
        test_data = request.json or {'test': True}
        
        try:
            # Create calculator instance
            calc = cpp_manager.create_calculator(calculator_name, f"test_{calculator_name}", use_cache=False)
            
            # Execute calculation
            import time
            start = time.time_ns()
            result = calc.calculate(test_data)
            elapsed_ns = time.time_ns() - start
            
            return jsonify({
                'success': True,
                'data': {
                    'input': test_data,
                    'output': result,
                    'elapsed_ns': elapsed_ns,
                    'elapsed_ms': elapsed_ns / 1_000_000
                }
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })
    
    @require_cpp_manager
    def api_cpp_config(self, cpp_manager):
        """Get CPP configuration"""
        return jsonify({
            'success': True,
            'data': cpp_manager.get_config()
        })
