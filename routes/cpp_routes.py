"""
CPP Management Routes (FastAPI, v2.0.0)
=======================================

Web UI and API endpoints for managing C++ pybind11 modules and calculators.
When the CPP manager is unavailable the API endpoints return a detailed 503
instead of failing.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.  The legacy ``require_cpp_manager`` decorator is replaced by the
``_cpp_manager()`` helper, which returns either the manager or an error
response - same behaviour, FastAPI-friendly shape.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import time
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import render

logger = logging.getLogger(__name__)


class CPPRoutes:
    """Routes for CPP management."""

    def __init__(self, app: FastAPI):
        self.app = app
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        # Web UI routes
        add('/cpp', self.cpp_management, methods=['GET'],
            name='cpp_management', include_in_schema=False)
        add('/cpp/calculators', self.cpp_calculators, methods=['GET'],
            name='cpp_calculators', include_in_schema=False)
        add('/cpp/calculator/{calculator_name}',
            self.cpp_calculator_details, methods=['GET'],
            name='cpp_calculator_details', include_in_schema=False)
        add('/cpp/module/{module_name}', self.cpp_module_details,
            methods=['GET'], name='cpp_module_details',
            include_in_schema=False)
        # API routes
        add('/api/cpp/status', self.api_cpp_status, methods=['GET'],
            name='api_cpp_status')
        add('/api/cpp/modules', self.api_cpp_modules, methods=['GET'],
            name='api_cpp_modules')
        add('/api/cpp/modules/{module_name}', self.api_cpp_module_detail,
            methods=['GET'], name='api_cpp_module_detail')
        add('/api/cpp/modules/{module_name}/load', self.api_cpp_module_load,
            methods=['POST'], name='api_cpp_module_load')
        add('/api/cpp/modules/{module_name}/unload',
            self.api_cpp_module_unload, methods=['POST'],
            name='api_cpp_module_unload')
        add('/api/cpp/modules/{module_name}/reload',
            self.api_cpp_module_reload, methods=['POST'],
            name='api_cpp_module_reload')
        add('/api/cpp/modules/{module_name}/build',
            self.api_cpp_module_build, methods=['POST'],
            name='api_cpp_module_build')
        add('/api/cpp/reload-config', self.api_cpp_reload_config,
            methods=['POST'], name='api_cpp_reload_config')
        add('/api/cpp/calculators', self.api_cpp_calculators,
            methods=['GET'], name='api_cpp_calculators')
        add('/api/cpp/calculators/{calculator_name}',
            self.api_cpp_calculator_detail, methods=['GET'],
            name='api_cpp_calculator_detail')
        add('/api/cpp/calculators/{calculator_name}/test',
            self.api_cpp_calculator_test, methods=['POST'],
            name='api_cpp_calculator_test')
        add('/api/cpp/config', self.api_cpp_config, methods=['GET'],
            name='api_cpp_config')

    # ------------------------------------------------------------------ #
    # Manager helper (replaces the legacy decorator)
    # ------------------------------------------------------------------ #

    @staticmethod
    def _cpp_manager():
        """
        Resolve the CPP manager.

        Returns:
            (manager, None) when available, or
            (None, JSONResponse) carrying the detailed error to return.
        """
        try:
            from core.cpp import get_cpp_manager
            return get_cpp_manager(), None
        except ImportError:
            return None, JSONResponse(
                {'success': False,
                 'error': 'CPP Manager not available',
                 'message': 'pybind11 integration is not installed or '
                            'enabled'},
                status_code=503)
        except Exception as e:  # noqa: BLE001
            logger.error(f"CPP Manager error: {e}")
            logger.error(traceback.format_exc())
            return None, JSONResponse({'success': False, 'error': str(e)},
                                      status_code=500)

    # ------------------------------------------------------------------ #
    # Web UI routes
    # ------------------------------------------------------------------ #

    def cpp_management(self, request: Request):
        """CPP Management dashboard page."""
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
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting CPP status: {e}")
            logger.error(traceback.format_exc())
        return render(request, 'cpp/management.html',
                      cpp_available=cpp_available,
                      cpp_status=cpp_status,
                      modules=modules,
                      calculators=calculators)

    def cpp_calculators(self, request: Request):
        """CPP Calculators listing page."""
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
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error listing calculators: {e}")
            logger.error(traceback.format_exc())
        return render(request, 'cpp/calculators.html',
                      calculators=calculators, modules=modules)

    def cpp_calculator_details(self, request: Request,
                               calculator_name: str):
        """Calculator details page."""
        try:
            from core.cpp import get_cpp_manager
            cpp_manager = get_cpp_manager()
            definition = cpp_manager.calculator_definitions.get(
                calculator_name)
            if not definition:
                return render(request, 'error.html', status_code=404,
                              error=f"Calculator '{calculator_name}' not "
                                    f"found")
            module_status = cpp_manager.module_status.get(definition.module)
            return render(request, 'cpp/calculator_details.html',
                          calculator={
                              'name': definition.name,
                              'module': definition.module,
                              'cpp_class': definition.cpp_class,
                              'description': definition.description,
                              'default_config': definition.default_config},
                          module_loaded=module_status.loaded
                          if module_status else False)
        except ImportError:
            return render(request, 'cpp/not_available.html')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting calculator details: {e}")
            logger.error(traceback.format_exc())
            return render(request, 'error.html', status_code=500,
                          error=str(e))

    def cpp_module_details(self, request: Request, module_name: str):
        """Module details page."""
        try:
            from core.cpp import get_cpp_manager
            cpp_manager = get_cpp_manager()
            config = cpp_manager.module_configs.get(module_name)
            status = cpp_manager.module_status.get(module_name)
            if not config:
                return render(request, 'error.html', status_code=404,
                              error=f"Module '{module_name}' not found")
            module_calculators = [c for c in cpp_manager.list_calculators()
                                  if c['module'] == module_name]
            return render(request, 'cpp/module_details.html',
                          module={'name': config.name,
                                  'enabled': config.enabled,
                                  'description': config.description,
                                  'path': config.path,
                                  'source_dir': config.source_dir,
                                  'build_dir': config.build_dir,
                                  'lmdb_enabled': config.lmdb_enabled,
                                  'lmdb_db_path': config.lmdb_db_path},
                          status={'loaded': status.loaded
                                  if status else False,
                                  'path': status.path if status else None,
                                  'available_classes':
                                      status.available_classes
                                      if status else [],
                                  'load_time': status.load_time.isoformat()
                                  if status and status.load_time else None,
                                  'last_error': status.last_error
                                  if status else None},
                          calculators=module_calculators)
        except ImportError:
            return render(request, 'cpp/not_available.html')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting module details: {e}")
            logger.error(traceback.format_exc())
            return render(request, 'error.html', status_code=500,
                          error=str(e))

    # ------------------------------------------------------------------ #
    # API routes
    # ------------------------------------------------------------------ #

    def api_cpp_status(self, request: Request):
        """Get CPP Manager status."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        return JSONResponse({'success': True,
                             'data': cpp_manager.get_status()})

    def api_cpp_modules(self, request: Request):
        """List all C++ modules."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        return JSONResponse({'success': True,
                             'data': {'modules':
                                      cpp_manager.list_modules()}})

    def api_cpp_module_detail(self, request: Request, module_name: str):
        """Get details for a specific module."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        config = cpp_manager.module_configs.get(module_name)
        status = cpp_manager.module_status.get(module_name)
        if not config:
            return JSONResponse({'success': False,
                                 'error': f'Unknown module: {module_name}'},
                                status_code=404)
        return JSONResponse({'success': True, 'data': {
            'name': config.name,
            'enabled': config.enabled,
            'description': config.description,
            'path': config.path,
            'source_dir': config.source_dir,
            'build_dir': config.build_dir,
            'lmdb_enabled': config.lmdb_enabled,
            'loaded': status.loaded if status else False,
            'available_classes': status.available_classes
            if status else [],
            'load_time': status.load_time.isoformat()
            if status and status.load_time else None,
            'last_error': status.last_error if status else None}})

    def api_cpp_module_load(self, request: Request, module_name: str):
        """Load a C++ module."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        if module_name not in cpp_manager.module_configs:
            return JSONResponse({'success': False,
                                 'error': f'Unknown module: {module_name}'},
                                status_code=404)
        success = cpp_manager.load_module(module_name)
        return JSONResponse({
            'success': success,
            'message': f'Module {module_name} '
                       f'{"loaded" if success else "failed to load"}'})

    def api_cpp_module_unload(self, request: Request, module_name: str):
        """Unload a C++ module."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        success = cpp_manager.unload_module(module_name)
        return JSONResponse({'success': success,
                             'message': f'Module {module_name} unloaded'})

    def api_cpp_module_reload(self, request: Request, module_name: str):
        """Reload a C++ module."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        success = cpp_manager.reload_module(module_name)
        return JSONResponse({
            'success': success,
            'message': f'Module {module_name} '
                       f'{"reloaded" if success else "failed to reload"}'})

    def api_cpp_module_build(self, request: Request, module_name: str):
        """Build a C++ module using CMake."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        success = cpp_manager.build_module(module_name)
        return JSONResponse({
            'success': success,
            'message': f'Module {module_name} '
                       f'{"built successfully" if success else "build failed"}'})

    def api_cpp_reload_config(self, request: Request):
        """Reload CPP configuration from file."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        try:
            cpp_manager._reload_config()
            return JSONResponse({'success': True,
                                 'message': 'Configuration reloaded'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reloading CPP config: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)})

    def api_cpp_calculators(self, request: Request):
        """List all C++ calculators."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        return JSONResponse({'success': True,
                             'data': {'definitions':
                                      cpp_manager.list_calculators()}})

    def api_cpp_calculator_detail(self, request: Request,
                                  calculator_name: str):
        """Get details for a specific calculator."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        definition = cpp_manager.calculator_definitions.get(calculator_name)
        if not definition:
            return JSONResponse({'success': False,
                                 'error': f'Unknown calculator: '
                                          f'{calculator_name}'},
                                status_code=404)
        module_loaded = cpp_manager.is_module_loaded(definition.module)
        return JSONResponse({'success': True, 'data': {
            'name': definition.name,
            'module': definition.module,
            'cpp_class': definition.cpp_class,
            'description': definition.description,
            'default_config': definition.default_config,
            'available': module_loaded}})

    async def api_cpp_calculator_test(self, request: Request,
                                      calculator_name: str):
        """Test a C++ calculator with sample data."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        try:
            test_data = await request.json()
        except Exception:  # noqa: BLE001 - empty body is legal here
            test_data = None
        test_data = test_data or {'test': True}
        try:
            calc = cpp_manager.create_calculator(
                calculator_name, f"test_{calculator_name}", use_cache=False)
            start = time.time_ns()
            result = calc.calculate(test_data)
            elapsed_ns = time.time_ns() - start
            return JSONResponse({'success': True, 'data': {
                'input': test_data,
                'output': result,
                'elapsed_ns': elapsed_ns,
                'elapsed_ms': elapsed_ns / 1_000_000}})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error testing calculator {calculator_name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)})

    def api_cpp_config(self, request: Request):
        """Get CPP configuration."""
        cpp_manager, error = self._cpp_manager()
        if error:
            return error
        return JSONResponse({'success': True,
                             'data': cpp_manager.get_config()})
