"""
JVM Management Routes (FastAPI, v2.0.0)
=======================================

Web UI and API endpoints for managing JVM instances and Java calculators
via Py4J integration.  When the JVM manager is unavailable the API
endpoints return a detailed 503 instead of failing.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.  The legacy ``require_jvm_manager`` decorator is replaced by the
``_jvm_manager()`` helper, which returns either the manager or an error
response - same behaviour, FastAPI-friendly shape.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import render

logger = logging.getLogger(__name__)


class JVMRoutes:
    """Routes for JVM management."""

    def __init__(self, app: FastAPI):
        self.app = app
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        # Web UI routes
        add('/jvm', self.jvm_management, methods=['GET'],
            name='jvm_management', include_in_schema=False)
        add('/jvm/calculators', self.jvm_calculators, methods=['GET'],
            name='jvm_calculators', include_in_schema=False)
        add('/jvm/calculator/{calculator_name}',
            self.jvm_calculator_details, methods=['GET'],
            name='jvm_calculator_details', include_in_schema=False)
        add('/jvm/gateway/{gateway_name}', self.jvm_gateway_details,
            methods=['GET'], name='jvm_gateway_details',
            include_in_schema=False)
        # API routes
        add('/api/jvm/status', self.api_jvm_status, methods=['GET'],
            name='api_jvm_status')
        add('/api/jvm/gateways', self.api_jvm_gateways, methods=['GET'],
            name='api_jvm_gateways')
        add('/api/jvm/gateways/{gateway_name}', self.api_jvm_gateway_detail,
            methods=['GET'], name='api_jvm_gateway_detail')
        add('/api/jvm/gateways/{gateway_name}/reconnect',
            self.api_jvm_gateway_reconnect, methods=['POST'],
            name='api_jvm_gateway_reconnect')
        add('/api/jvm/gateways/{gateway_name}/stop',
            self.api_jvm_gateway_stop, methods=['POST'],
            name='api_jvm_gateway_stop')
        add('/api/jvm/gateways/{gateway_name}/restart',
            self.api_jvm_gateway_restart, methods=['POST'],
            name='api_jvm_gateway_restart')
        add('/api/jvm/reload-config', self.api_jvm_reload_config,
            methods=['POST'], name='api_jvm_reload_config')
        add('/api/jvm/calculators', self.api_jvm_calculators,
            methods=['GET'], name='api_jvm_calculators')
        add('/api/jvm/calculators/{calculator_name}',
            self.api_jvm_calculator_detail, methods=['GET'],
            name='api_jvm_calculator_detail')
        add('/api/jvm/health', self.api_jvm_health, methods=['GET'],
            name='api_jvm_health')
        add('/api/jvm/config', self.api_jvm_config, methods=['GET'],
            name='api_jvm_config')

    # ------------------------------------------------------------------ #
    # Manager helper (replaces the legacy decorator)
    # ------------------------------------------------------------------ #

    @staticmethod
    def _jvm_manager():
        """
        Resolve the JVM manager.

        Returns:
            (manager, None) when available, or
            (None, JSONResponse) carrying the detailed error to return.
        """
        try:
            from core.jvm import get_jvm_manager
            return get_jvm_manager(), None
        except ImportError:
            return None, JSONResponse(
                {'success': False,
                 'error': 'JVM Manager not available',
                 'message': 'Py4J integration is not installed or enabled'},
                status_code=503)
        except Exception as e:  # noqa: BLE001
            logger.error(f"JVM Manager error: {e}")
            logger.error(traceback.format_exc())
            return None, JSONResponse({'success': False, 'error': str(e)},
                                      status_code=500)

    # ------------------------------------------------------------------ #
    # Web UI routes
    # ------------------------------------------------------------------ #

    def jvm_management(self, request: Request):
        """JVM Management dashboard page."""
        jvm_available = False
        jvm_status = {}
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            jvm_available = jvm_manager.is_initialized()
            if jvm_available:
                jvm_status = jvm_manager.get_status()
        except ImportError:
            # Py4J integration not installed - page renders the
            # "not available" state, which is the intended behaviour.
            pass
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting JVM status: {e}")
            logger.error(traceback.format_exc())
        return render(request, 'jvm/management.html',
                      jvm_available=jvm_available, jvm_status=jvm_status)

    def jvm_calculators(self, request: Request):
        """Java calculators page."""
        calculators = []
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            if jvm_manager.is_initialized():
                calc_defs = jvm_manager.get_calculator_definitions()
                calculators = [
                    {'name': name,
                     'java_class': defn.java_class,
                     'description': defn.description,
                     'gateway': defn.gateway,
                     'default_config': defn.default_config}
                    for name, defn in calc_defs.items()
                ]
        except ImportError:
            pass
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting calculators: {e}")
            logger.error(traceback.format_exc())
        return render(request, 'jvm/calculators.html',
                      calculators=calculators)

    def jvm_calculator_details(self, request: Request,
                               calculator_name: str):
        """Calculator details page."""
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            calc_def = jvm_manager.calculator_definitions.get(
                calculator_name)
            if not calc_def:
                return render(request, 'error.html', status_code=404,
                              error=f'Calculator not found: '
                                    f'{calculator_name}')
            calculator = {'name': calc_def.name,
                          'java_class': calc_def.java_class,
                          'description': calc_def.description,
                          'gateway': calc_def.gateway,
                          'default_config': calc_def.default_config}
        except ImportError:
            return render(request, 'error.html', status_code=503,
                          error='JVM Manager not available')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting calculator details: {e}")
            logger.error(traceback.format_exc())
            return render(request, 'error.html', status_code=500,
                          error=str(e))
        return render(request, 'jvm/calculator_details.html',
                      calculator=calculator)

    def jvm_gateway_details(self, request: Request, gateway_name: str):
        """Gateway details page."""
        try:
            from core.jvm import get_jvm_manager
            jvm_manager = get_jvm_manager()
            gw_config = jvm_manager.gateway_configs.get(gateway_name)
            gw_status = jvm_manager.gateway_status.get(gateway_name)
            if not gw_config:
                return render(request, 'error.html', status_code=404,
                              error=f'Gateway not found: {gateway_name}')
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
                'active_connections': gw_status.active_connections
                if gw_status else 0,
                'total_requests': gw_status.total_requests
                if gw_status else 0,
                'errors': gw_status.errors if gw_status else 0,
                'last_health_check':
                    gw_status.last_health_check.strftime('%Y-%m-%d %H:%M:%S')
                    if gw_status and gw_status.last_health_check else None,
                'start_time':
                    gw_status.start_time.strftime('%Y-%m-%d %H:%M:%S')
                    if gw_status and gw_status.start_time else None
            }
        except ImportError:
            return render(request, 'error.html', status_code=503,
                          error='JVM Manager not available')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting gateway details: {e}")
            logger.error(traceback.format_exc())
            return render(request, 'error.html', status_code=500,
                          error=str(e))
        return render(request, 'jvm/gateway_details.html',
                      gateway_name=gateway_name, config=config,
                      status=status)

    # ------------------------------------------------------------------ #
    # API routes
    # ------------------------------------------------------------------ #

    def api_jvm_status(self, request: Request):
        """Get JVM Manager status."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        return JSONResponse({'success': True,
                             'data': jvm_manager.get_status()})

    def api_jvm_gateways(self, request: Request):
        """Get all gateway configurations and status."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
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
                'active_connections': status.active_connections
                if status else 0
            })
        return JSONResponse({'success': True, 'data': gateways})

    def api_jvm_gateway_detail(self, request: Request, gateway_name: str):
        """Get detailed status for a specific gateway."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        config = jvm_manager.gateway_configs.get(gateway_name)
        status = jvm_manager.gateway_status.get(gateway_name)
        if not config:
            return JSONResponse({'success': False,
                                 'error': f'Gateway not found: '
                                          f'{gateway_name}'},
                                status_code=404)
        return JSONResponse({'success': True, 'data': {
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
                'active_connections': status.active_connections
                if status else 0,
                'total_requests': status.total_requests if status else 0,
                'errors': status.errors if status else 0,
                'last_health_check': status.last_health_check.isoformat()
                if status and status.last_health_check else None,
                'start_time': status.start_time.isoformat()
                if status and status.start_time else None
            }
        }})

    def api_jvm_gateway_reconnect(self, request: Request,
                                  gateway_name: str):
        """Reconnect a gateway."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        try:
            success = jvm_manager._connect_gateway(gateway_name)
            return JSONResponse({
                'success': success,
                'message': f'Gateway {gateway_name} '
                           f'{"reconnected" if success else "failed to reconnect"}'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reconnecting gateway {gateway_name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_jvm_gateway_stop(self, request: Request, gateway_name: str):
        """Stop a JVM."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        try:
            success = jvm_manager.kill_jvm(gateway_name)
            return JSONResponse({
                'success': success,
                'message': f'JVM {gateway_name} '
                           f'{"stopped" if success else "failed to stop"}'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error stopping JVM {gateway_name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_jvm_gateway_restart(self, request: Request, gateway_name: str):
        """Restart a JVM."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        try:
            success = jvm_manager.restart_jvm(gateway_name)
            return JSONResponse({
                'success': success,
                'message': f'JVM {gateway_name} '
                           f'{"restarted" if success else "failed to restart"}'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error restarting JVM {gateway_name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_jvm_reload_config(self, request: Request):
        """Reload JVM configuration."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        try:
            results = jvm_manager.reload_config()
            return JSONResponse({'success': True,
                                 'message': 'Configuration reloaded',
                                 'results': results})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reloading JVM config: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_jvm_calculators(self, request: Request):
        """Get all registered Java calculators."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        calc_defs = jvm_manager.get_calculator_definitions()
        calculators = [
            {'name': name,
             'java_class': defn.java_class,
             'description': defn.description,
             'gateway': defn.gateway,
             'default_config': defn.default_config}
            for name, defn in calc_defs.items()
        ]

        # Also get calculators registered in the gateway
        try:
            gateway = jvm_manager.get_gateway("primary")
            if gateway:
                registered = list(
                    gateway.entry_point.getRegisteredCalculators())
                return JSONResponse({'success': True, 'data': {
                    'definitions': calculators,
                    'registered_in_gateway': registered}})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Could not query gateway-registered "
                         f"calculators: {e}")
            logger.error(traceback.format_exc())

        return JSONResponse({'success': True, 'data': {
            'definitions': calculators, 'registered_in_gateway': []}})

    def api_jvm_calculator_detail(self, request: Request,
                                  calculator_name: str):
        """Get details for a specific calculator."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        calc_defs = jvm_manager.get_calculator_definitions()
        defn = calc_defs.get(calculator_name)
        if not defn:
            return JSONResponse({'success': False,
                                 'error': f'Calculator not found: '
                                          f'{calculator_name}'},
                                status_code=404)
        return JSONResponse({'success': True, 'data': {
            'name': calculator_name,
            'java_class': defn.java_class,
            'description': defn.description,
            'gateway': defn.gateway,
            'default_config': defn.default_config}})

    def api_jvm_health(self, request: Request):
        """JVM health check endpoint."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        healthy = jvm_manager.is_gateway_available("primary")
        return JSONResponse({
            'success': True,
            'healthy': healthy,
            'initialized': jvm_manager.is_initialized(),
            'gateways': {name: status.connected
                         for name, status in
                         jvm_manager.gateway_status.items()}})

    def api_jvm_config(self, request: Request):
        """Get JVM configuration (without sensitive data)."""
        jvm_manager, error = self._jvm_manager()
        if error:
            return error
        config = jvm_manager.config.copy()
        if 'jar_files' in config:
            config['jar_files'] = {k: v for k, v in
                                   config['jar_files'].items()
                                   if not k.startswith('_')}
        return JSONResponse({'success': True, 'data': config})
