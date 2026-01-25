"""
JVM Management Routes for DishtaYantra
Version: 1.6.0

Provides web UI and API endpoints for managing JVM instances
and Java calculators via Py4J integration.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import logging
from flask import render_template, jsonify, request
from functools import wraps

logger = logging.getLogger(__name__)


def require_jvm_manager(f):
    """Decorator to check if JVM Manager is available"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            return f(jvm_manager=jvm_manager, *args, **kwargs)
        except ImportError:
            return jsonify({
                'success': False,
                'error': 'JVM Manager not available',
                'message': 'Py4J integration is not installed or enabled'
            }), 503
        except Exception as e:
            logger.error(f"JVM Manager error: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    return decorated_function


class JVMRoutes:
    """Routes for JVM management"""
    
    def __init__(self, app):
        self.app = app
        self._register_routes()
    
    def _register_routes(self):
        """Register all JVM management routes"""
        # Web UI routes
        self.app.add_url_rule('/jvm', 'jvm_management', self.jvm_management)
        self.app.add_url_rule('/jvm/calculators', 'jvm_calculators', self.jvm_calculators)
        self.app.add_url_rule('/jvm/calculator/<calculator_name>', 'jvm_calculator_details', self.jvm_calculator_details)
        self.app.add_url_rule('/jvm/gateway/<gateway_name>', 'jvm_gateway_details', self.jvm_gateway_details)
        
        # API routes
        self.app.add_url_rule('/api/jvm/status', 'api_jvm_status', self.api_jvm_status)
        self.app.add_url_rule('/api/jvm/gateways', 'api_jvm_gateways', self.api_jvm_gateways)
        self.app.add_url_rule('/api/jvm/gateways/<gateway_name>', 'api_jvm_gateway_detail', self.api_jvm_gateway_detail)
        self.app.add_url_rule('/api/jvm/gateways/<gateway_name>/reconnect', 'api_jvm_gateway_reconnect', 
                            self.api_jvm_gateway_reconnect, methods=['POST'])
        self.app.add_url_rule('/api/jvm/gateways/<gateway_name>/stop', 'api_jvm_gateway_stop', 
                            self.api_jvm_gateway_stop, methods=['POST'])
        self.app.add_url_rule('/api/jvm/gateways/<gateway_name>/restart', 'api_jvm_gateway_restart', 
                            self.api_jvm_gateway_restart, methods=['POST'])
        self.app.add_url_rule('/api/jvm/reload-config', 'api_jvm_reload_config', 
                            self.api_jvm_reload_config, methods=['POST'])
        self.app.add_url_rule('/api/jvm/calculators', 'api_jvm_calculators', self.api_jvm_calculators)
        self.app.add_url_rule('/api/jvm/calculators/<calculator_name>', 'api_jvm_calculator_detail', 
                            self.api_jvm_calculator_detail)
        self.app.add_url_rule('/api/jvm/health', 'api_jvm_health', self.api_jvm_health)
        self.app.add_url_rule('/api/jvm/config', 'api_jvm_config', self.api_jvm_config)
    
    def jvm_management(self):
        """JVM Management dashboard page"""
        jvm_available = False
        jvm_status = {}
        
        try:
            from core.jvm import get_jvm_manager, is_jvm_available
            jvm_manager = get_jvm_manager()
            jvm_available = jvm_manager.is_initialized()
            if jvm_available:
                jvm_status = jvm_manager.get_status()
        except ImportError:
            pass
        except Exception as e:
            logger.error(f"Error getting JVM status: {e}")
        
        return render_template('jvm/management.html',
                             jvm_available=jvm_available,
                             jvm_status=jvm_status)
    
    def jvm_calculators(self):
        """Java calculators page"""
        calculators = []
        
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            if jvm_manager.is_initialized():
                calc_defs = jvm_manager.get_calculator_definitions()
                calculators = [
                    {
                        'name': name,
                        'java_class': defn.java_class,
                        'description': defn.description,
                        'gateway': defn.gateway,
                        'default_config': defn.default_config
                    }
                    for name, defn in calc_defs.items()
                ]
        except ImportError:
            pass
        except Exception as e:
            logger.error(f"Error getting calculators: {e}")
        
        return render_template('jvm/calculators.html', calculators=calculators)
    
    def jvm_calculator_details(self, calculator_name):
        """Calculator details page"""
        calculator = None
        
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            
            calc_def = jvm_manager.calculator_definitions.get(calculator_name)
            
            if not calc_def:
                return render_template('error.html', 
                                     error=f'Calculator not found: {calculator_name}'), 404
            
            calculator = {
                'name': calc_def.name,
                'java_class': calc_def.java_class,
                'description': calc_def.description,
                'gateway': calc_def.gateway,
                'default_config': calc_def.default_config
            }
            
        except ImportError:
            return render_template('error.html', 
                                 error='JVM Manager not available'), 503
        except Exception as e:
            logger.error(f"Error getting calculator details: {e}")
            return render_template('error.html', 
                                 error=str(e)), 500
        
        return render_template('jvm/calculator_details.html',
                             calculator=calculator)
    
    def jvm_gateway_details(self, gateway_name):
        """Gateway details page"""
        config = None
        status = None
        
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            
            gw_config = jvm_manager.gateway_configs.get(gateway_name)
            gw_status = jvm_manager.gateway_status.get(gateway_name)
            
            if not gw_config:
                return render_template('error.html', 
                                     error=f'Gateway not found: {gateway_name}'), 404
            
            config = {
                'enabled': gw_config.enabled,
                'host': gw_config.host,
                'base_port': gw_config.base_port,
                'pool_size': gw_config.pool_size,
                'jvm_auto_start': gw_config.jvm_auto_start,
                'heap_size_mb': gw_config.heap_size_mb,
                'max_heap_size_mb': gw_config.max_heap_size_mb,
                'jvm_options': gw_config.jvm_options,
                'classpath': gw_config.classpath,
                'timeout_seconds': gw_config.timeout_seconds,
                'retry_attempts': gw_config.retry_attempts
            }
            
            status = {
                'connected': gw_status.connected if gw_status else False,
                'jvm_started': gw_status.jvm_started if gw_status else False,
                'jvm_pid': gw_status.jvm_process_pid if gw_status else None,
                'active_connections': gw_status.active_connections if gw_status else 0,
                'total_requests': gw_status.total_requests if gw_status else 0,
                'errors': gw_status.errors if gw_status else 0,
                'last_health_check': gw_status.last_health_check.strftime('%Y-%m-%d %H:%M:%S') if gw_status and gw_status.last_health_check else None,
                'start_time': gw_status.start_time.strftime('%Y-%m-%d %H:%M:%S') if gw_status and gw_status.start_time else None
            }
            
        except ImportError:
            return render_template('error.html', 
                                 error='JVM Manager not available'), 503
        except Exception as e:
            logger.error(f"Error getting gateway details: {e}")
            return render_template('error.html', 
                                 error=str(e)), 500
        
        return render_template('jvm/gateway_details.html',
                             gateway_name=gateway_name,
                             config=config,
                             status=status)
    
    @require_jvm_manager
    def api_jvm_status(self, jvm_manager):
        """Get JVM Manager status"""
        return jsonify({
            'success': True,
            'data': jvm_manager.get_status()
        })
    
    @require_jvm_manager
    def api_jvm_gateways(self, jvm_manager):
        """Get all gateway configurations and status"""
        gateways = []
        for name, config in jvm_manager.gateway_configs.items():
            status = jvm_manager.gateway_status.get(name)
            gateways.append({
                'name': name,
                'enabled': config.enabled,
                'host': config.host,
                'base_port': config.base_port,
                'pool_size': config.pool_size,
                'connected': status.connected if status else False,
                'jvm_started': status.jvm_started if status else False,
                'active_connections': status.active_connections if status else 0
            })
        
        return jsonify({
            'success': True,
            'data': gateways
        })
    
    @require_jvm_manager
    def api_jvm_gateway_detail(self, gateway_name, jvm_manager):
        """Get detailed status for a specific gateway"""
        config = jvm_manager.gateway_configs.get(gateway_name)
        status = jvm_manager.gateway_status.get(gateway_name)
        
        if not config:
            return jsonify({
                'success': False,
                'error': f'Gateway not found: {gateway_name}'
            }), 404
        
        return jsonify({
            'success': True,
            'data': {
                'name': gateway_name,
                'config': {
                    'enabled': config.enabled,
                    'host': config.host,
                    'base_port': config.base_port,
                    'pool_size': config.pool_size,
                    'jvm_auto_start': config.jvm_auto_start,
                    'heap_size_mb': config.heap_size_mb,
                    'max_heap_size_mb': config.max_heap_size_mb,
                    'timeout_seconds': config.timeout_seconds,
                    'retry_attempts': config.retry_attempts
                },
                'status': {
                    'connected': status.connected if status else False,
                    'jvm_started': status.jvm_started if status else False,
                    'jvm_pid': status.jvm_process_pid if status else None,
                    'active_connections': status.active_connections if status else 0,
                    'total_requests': status.total_requests if status else 0,
                    'errors': status.errors if status else 0,
                    'last_health_check': status.last_health_check.isoformat() if status and status.last_health_check else None,
                    'start_time': status.start_time.isoformat() if status and status.start_time else None
                }
            }
        })
    
    @require_jvm_manager
    def api_jvm_gateway_reconnect(self, gateway_name, jvm_manager):
        """Reconnect a gateway"""
        try:
            success = jvm_manager._connect_gateway(gateway_name)
            return jsonify({
                'success': success,
                'message': f'Gateway {gateway_name} {"reconnected" if success else "failed to reconnect"}'
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @require_jvm_manager
    def api_jvm_gateway_stop(self, gateway_name, jvm_manager):
        """Stop a JVM"""
        try:
            success = jvm_manager.kill_jvm(gateway_name)
            return jsonify({
                'success': success,
                'message': f'JVM {gateway_name} {"stopped" if success else "failed to stop"}'
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @require_jvm_manager
    def api_jvm_gateway_restart(self, gateway_name, jvm_manager):
        """Restart a JVM"""
        try:
            success = jvm_manager.restart_jvm(gateway_name)
            return jsonify({
                'success': success,
                'message': f'JVM {gateway_name} {"restarted" if success else "failed to restart"}'
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @require_jvm_manager
    def api_jvm_reload_config(self, jvm_manager):
        """Reload JVM configuration"""
        try:
            results = jvm_manager.reload_config()
            return jsonify({
                'success': True,
                'message': 'Configuration reloaded',
                'results': results
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @require_jvm_manager
    def api_jvm_calculators(self, jvm_manager):
        """Get all registered Java calculators"""
        calc_defs = jvm_manager.get_calculator_definitions()
        calculators = [
            {
                'name': name,
                'java_class': defn.java_class,
                'description': defn.description,
                'gateway': defn.gateway,
                'default_config': defn.default_config
            }
            for name, defn in calc_defs.items()
        ]
        
        # Also get calculators registered in the gateway
        try:
            gateway = jvm_manager.get_gateway("primary")
            if gateway:
                registered = list(gateway.entry_point.getRegisteredCalculators())
                return jsonify({
                    'success': True,
                    'data': {
                        'definitions': calculators,
                        'registered_in_gateway': registered
                    }
                })
        except:
            pass
        
        return jsonify({
            'success': True,
            'data': {
                'definitions': calculators,
                'registered_in_gateway': []
            }
        })
    
    @require_jvm_manager
    def api_jvm_calculator_detail(self, calculator_name, jvm_manager):
        """Get details for a specific calculator"""
        calc_defs = jvm_manager.get_calculator_definitions()
        defn = calc_defs.get(calculator_name)
        
        if not defn:
            return jsonify({
                'success': False,
                'error': f'Calculator not found: {calculator_name}'
            }), 404
        
        return jsonify({
            'success': True,
            'data': {
                'name': calculator_name,
                'java_class': defn.java_class,
                'description': defn.description,
                'gateway': defn.gateway,
                'default_config': defn.default_config
            }
        })
    
    @require_jvm_manager
    def api_jvm_health(self, jvm_manager):
        """Health check endpoint"""
        healthy = jvm_manager.is_gateway_available("primary")
        
        return jsonify({
            'success': True,
            'healthy': healthy,
            'initialized': jvm_manager.is_initialized(),
            'gateways': {
                name: status.connected
                for name, status in jvm_manager.gateway_status.items()
            }
        })
    
    @require_jvm_manager
    def api_jvm_config(self, jvm_manager):
        """Get JVM configuration (without sensitive data)"""
        config = jvm_manager.config.copy()
        
        # Remove any sensitive information
        if 'jar_files' in config:
            config['jar_files'] = {
                k: v for k, v in config['jar_files'].items()
                if not k.startswith('_')
            }
        
        return jsonify({
            'success': True,
            'data': config
        })
