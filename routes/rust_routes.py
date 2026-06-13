"""
Rust Management Routes (FastAPI, v2.0.0)
========================================

Web UI and API endpoints for managing Rust PyO3 modules and calculators.
When the Rust manager is unavailable, page routes render the
``rust/not_available.html`` template and API routes return a detailed 503 -
the same behaviour as the legacy ``require_rust_manager`` decorator,
implemented as the ``_rust_manager()`` helper.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import time
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import render

logger = logging.getLogger(__name__)


class RustRoutes:
    """Routes for Rust module and calculator management."""

    def __init__(self, app: FastAPI):
        self.app = app
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        # Web UI routes
        add('/rust', self.rust_management, methods=['GET'],
            name='rust_management', include_in_schema=False)
        add('/rust/calculators', self.rust_calculators, methods=['GET'],
            name='rust_calculators', include_in_schema=False)
        add('/rust/calculator/{name}', self.rust_calculator_details,
            methods=['GET'], name='rust_calculator_details',
            include_in_schema=False)
        add('/rust/module/{name}', self.rust_module_details,
            methods=['GET'], name='rust_module_details',
            include_in_schema=False)
        # API routes
        add('/api/rust/status', self.api_rust_status, methods=['GET'],
            name='api_rust_status')
        add('/api/rust/modules', self.api_rust_modules, methods=['GET'],
            name='api_rust_modules')
        add('/api/rust/modules/{name}', self.api_rust_module_info,
            methods=['GET'], name='api_rust_module_info')
        add('/api/rust/modules/{name}/load', self.api_rust_load_module,
            methods=['POST'], name='api_rust_load_module')
        add('/api/rust/modules/{name}/unload', self.api_rust_unload_module,
            methods=['POST'], name='api_rust_unload_module')
        add('/api/rust/modules/{name}/reload', self.api_rust_reload_module,
            methods=['POST'], name='api_rust_reload_module')
        add('/api/rust/modules/{name}/build', self.api_rust_build_module,
            methods=['POST'], name='api_rust_build_module')
        add('/api/rust/calculators', self.api_rust_calculators,
            methods=['GET'], name='api_rust_calculators')
        add('/api/rust/calculators/{name}', self.api_rust_calculator_info,
            methods=['GET'], name='api_rust_calculator_info')
        add('/api/rust/calculators/{name}/test',
            self.api_rust_test_calculator, methods=['POST'],
            name='api_rust_test_calculator')
        add('/api/rust/config', self.api_rust_config, methods=['GET'],
            name='api_rust_config')
        add('/api/rust/reload-config', self.api_rust_reload_config,
            methods=['POST'], name='api_rust_reload_config')

    # ------------------------------------------------------------------ #
    # Manager helper (replaces the legacy decorator)
    # ------------------------------------------------------------------ #

    @staticmethod
    def _rust_manager(request: Request):
        """
        Resolve the Rust manager.

        Returns:
            (manager, None) when available, or (None, Response) where the
            response is JSON 503 for /api/ paths and the
            ``rust/not_available.html`` page otherwise.
        """
        try:
            from core.rust import get_rust_manager
            return get_rust_manager(), None
        except ImportError:
            if request.url.path.startswith('/api/'):
                return None, JSONResponse(
                    {'error': 'Rust Manager not available'},
                    status_code=503)
            return None, render(request, 'rust/not_available.html')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Rust Manager error: {e}")
            logger.error(traceback.format_exc())
            if request.url.path.startswith('/api/'):
                return None, JSONResponse({'error': str(e)},
                                          status_code=500)
            return None, render(request, 'error.html', status_code=500,
                                message=str(e))

    # ------------------------------------------------------------------ #
    # Web UI routes
    # ------------------------------------------------------------------ #

    def rust_management(self, request: Request):
        """Rust management dashboard."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        status = rust_manager.get_status()
        modules = rust_manager.list_modules()
        calculators = rust_manager.list_calculators()
        return render(request, 'rust/management.html',
                      status=status, modules=modules,
                      calculators=calculators,
                      config=rust_manager._config)

    def rust_calculators(self, request: Request):
        """List all Rust calculators."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        calculators = rust_manager.list_calculators()
        return render(request, 'rust/calculators.html',
                      calculators=calculators)

    def rust_calculator_details(self, request: Request, name: str):
        """Rust calculator details page."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        info = rust_manager.get_calculator_info(name)
        if not info:
            return render(request, 'error.html', status_code=404,
                          message=f"Calculator '{name}' not found")
        # Generate example DAG config
        example_dag = {
            "name": f"rust_{name.lower()}_node",
            "type": "CalculatorNode",
            "calculator": {"type": "rust", "name": name},
            "config": info.get('default_config', {})
        }
        return render(request, 'rust/calculator_details.html',
                      calculator=info,
                      example_dag=json.dumps(example_dag, indent=2))

    def rust_module_details(self, request: Request, name: str):
        """Rust module details page."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        info = rust_manager.get_module_info(name)
        if not info:
            return render(request, 'error.html', status_code=404,
                          message=f"Module '{name}' not found")
        calculators = [c for c in rust_manager.list_calculators()
                       if c['module'] == name]
        return render(request, 'rust/module_details.html',
                      module=info, calculators=calculators)

    # ------------------------------------------------------------------ #
    # API routes
    # ------------------------------------------------------------------ #

    def api_rust_status(self, request: Request):
        """Get Rust Manager status."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        return JSONResponse(rust_manager.get_status())

    def api_rust_modules(self, request: Request):
        """List all configured modules."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        return JSONResponse(rust_manager.list_modules())

    def api_rust_module_info(self, request: Request, name: str):
        """Get module details."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        info = rust_manager.get_module_info(name)
        if info:
            return JSONResponse(info)
        return JSONResponse({'error': f"Module '{name}' not found"},
                            status_code=404)

    def api_rust_load_module(self, request: Request, name: str):
        """Load a Rust module."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        try:
            success = rust_manager.load_module(name)
            if success:
                return JSONResponse({'success': True,
                                     'message': f"Module '{name}' loaded "
                                                f"successfully"})
            module_error = rust_manager.module_errors.get(name,
                                                          'Unknown error')
            return JSONResponse({'success': False, 'error': module_error},
                                status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error loading Rust module {name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_rust_unload_module(self, request: Request, name: str):
        """Unload a Rust module."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        try:
            success = rust_manager.unload_module(name)
            return JSONResponse({
                'success': success,
                'message': f"Module '{name}' unloaded" if success
                else 'Module not loaded'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error unloading Rust module {name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_rust_reload_module(self, request: Request, name: str):
        """Reload a Rust module."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        try:
            success = rust_manager.reload_module(name)
            if success:
                return JSONResponse({'success': True,
                                     'message': f"Module '{name}' reloaded "
                                                f"successfully"})
            module_error = rust_manager.module_errors.get(name,
                                                          'Unknown error')
            return JSONResponse({'success': False, 'error': module_error},
                                status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reloading Rust module {name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_rust_build_module(self, request: Request, name: str):
        """Build a Rust module with maturin/cargo."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        try:
            success, output = rust_manager.build_module(name)
            return JSONResponse({'success': success, 'output': output})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error building Rust module {name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_rust_calculators(self, request: Request):
        """List all calculator definitions."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        return JSONResponse(rust_manager.list_calculators())

    def api_rust_calculator_info(self, request: Request, name: str):
        """Get calculator details."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        info = rust_manager.get_calculator_info(name)
        if info:
            return JSONResponse(info)
        return JSONResponse({'error': f"Calculator '{name}' not found"},
                            status_code=404)

    async def api_rust_test_calculator(self, request: Request, name: str):
        """Test a calculator with sample data."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        try:
            try:
                test_data = await request.json()
            except Exception:  # noqa: BLE001 - empty body is legal here
                test_data = None
            test_data = test_data or {"values": [1.0, 2.0, 3.0, 4.0, 5.0]}

            # Get config override
            config_override = test_data.pop('_config', {})

            calc = rust_manager.create_calculator(name, f"test_{name}",
                                                  config_override)
            start = time.time()
            result = calc.calculate(test_data)
            elapsed = (time.time() - start) * 1000  # ms

            return JSONResponse({'success': True,
                                 'input': test_data,
                                 'output': result,
                                 'elapsed_ms': round(elapsed, 3)})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error testing Rust calculator {name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def api_rust_config(self, request: Request):
        """Get current configuration."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        return JSONResponse(rust_manager._config)

    def api_rust_reload_config(self, request: Request):
        """Reload configuration from file."""
        rust_manager, error = self._rust_manager(request)
        if error:
            return error
        try:
            rust_manager._reload_config()
            return JSONResponse({'success': True,
                                 'message': 'Configuration reloaded'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reloading Rust config: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)
