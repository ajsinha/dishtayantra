"""
Admin Routes - System Monitoring (FastAPI, v2.0.0)
==================================================

Admin-only system monitoring dashboard plus the real-time metrics API.  Log
viewing/streaming lives separately in :mod:`routes.admin_log_routes`
(architecture mandate: routes logically grouped, files small).

v2.0.0 fixes two legacy bugs:
    - the hardcoded ``app_version='1.1.1'`` is replaced by
      :data:`core.version.VERSION`;
    - environment info reports the FastAPI version instead of Flask.

Route names match the legacy Flask endpoint names so templates work
unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import os
import platform
import sys
import threading
import traceback
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.version import VERSION
from web.fastapi_compat import AuthGuards, render

logger = logging.getLogger(__name__)

# Try to import psutil for system metrics
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not installed. System monitoring will have "
                   "limited functionality.")


def format_bytes(bytes_val):
    """Format bytes to a human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} PB"


def format_uptime(seconds):
    """Format uptime seconds to a human-readable string."""
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    return ' '.join(parts) if parts else '< 1m'


class AdminRoutes:
    """Admin system-monitoring routes handler."""

    def __init__(self, app: FastAPI, dag_server=None,
                 guards: AuthGuards = None):
        self.app = app
        self.dag_server = dag_server
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/admin/monitoring', self.system_monitoring, methods=['GET'],
            name='system_monitoring', include_in_schema=False)
        add('/admin/monitoring/api', self.system_metrics_api,
            methods=['GET'], name='system_metrics_api')

    # ------------------------------------------------------------------ #
    # Metric helpers
    # ------------------------------------------------------------------ #

    def _get_system_metrics(self):
        """Gather host/process metrics via psutil (graceful fallback)."""
        metrics = {}

        if PSUTIL_AVAILABLE:
            # CPU
            metrics['cpu_percent'] = psutil.cpu_percent(interval=0.1)
            metrics['cpu_count'] = psutil.cpu_count()

            try:
                load_avg = os.getloadavg()
                metrics['load_avg_1'] = f"{load_avg[0]:.2f}"
                metrics['load_avg_5'] = f"{load_avg[1]:.2f}"
                metrics['load_avg_15'] = f"{load_avg[2]:.2f}"
            except (OSError, AttributeError):
                # getloadavg is unavailable on some platforms (e.g. Windows)
                metrics['load_avg_1'] = 'N/A'
                metrics['load_avg_5'] = 'N/A'
                metrics['load_avg_15'] = 'N/A'

            # Memory
            mem = psutil.virtual_memory()
            metrics['memory_percent'] = mem.percent
            metrics['memory_total'] = format_bytes(mem.total)
            metrics['memory_available'] = format_bytes(mem.available)
            metrics['memory_available_gb'] = mem.available / (1024 ** 3)

            # Disk
            disk = psutil.disk_usage('/')
            metrics['disk_percent'] = disk.percent
            metrics['disk_total'] = format_bytes(disk.total)
            metrics['disk_free'] = format_bytes(disk.free)
            metrics['disk_free_gb'] = disk.free / (1024 ** 3)

            # Network
            net_io = psutil.net_io_counters()
            metrics['network_bytes_sent'] = format_bytes(net_io.bytes_sent)
            metrics['network_bytes_recv'] = format_bytes(net_io.bytes_recv)
            metrics['network_errors'] = net_io.errin + net_io.errout

            try:
                metrics['network_connections'] = len(psutil.net_connections())
            except (psutil.AccessDenied, PermissionError):
                metrics['network_connections'] = 'N/A'

            # System uptime
            boot_time = psutil.boot_time()
            uptime_seconds = datetime.now().timestamp() - boot_time
            metrics['uptime'] = format_uptime(uptime_seconds)

            # Process info
            process = psutil.Process()
            metrics['process_id'] = process.pid
            metrics['process_memory'] = format_bytes(
                process.memory_info().rss)
            metrics['thread_count'] = process.num_threads()

            try:
                metrics['open_files'] = len(process.open_files())
            except (psutil.AccessDenied, PermissionError):
                metrics['open_files'] = 'N/A'

            # Top processes by memory
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent',
                                             'memory_percent', 'status']):
                try:
                    info = proc.info
                    info['memory_percent'] = round(
                        info['memory_percent'] or 0, 1)
                    info['cpu_percent'] = round(info['cpu_percent'] or 0, 1)
                    processes.append(info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    # Process exited or is inaccessible while iterating -
                    # an expected race, not a swallowed failure.
                    pass
            processes.sort(key=lambda x: x['memory_percent'], reverse=True)
            metrics['top_processes'] = processes[:10]

            # Disk partitions
            partitions = []
            for part in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(part.mountpoint)
                    partitions.append({
                        'mountpoint': part.mountpoint,
                        'fstype': part.fstype,
                        'total': format_bytes(usage.total),
                        'used': format_bytes(usage.used),
                        'free': format_bytes(usage.free),
                        'percent': usage.percent
                    })
                except (PermissionError, OSError):
                    # Virtual/system mounts are often unreadable - expected.
                    pass
            metrics['disk_partitions'] = partitions[:5]
        else:
            # Fallback values when psutil is not available
            metrics['cpu_percent'] = 0
            metrics['cpu_count'] = os.cpu_count() or 1
            metrics['load_avg_1'] = 'N/A'
            metrics['memory_percent'] = 0
            metrics['memory_total'] = 'N/A'
            metrics['memory_available'] = 'N/A'
            metrics['memory_available_gb'] = 0
            metrics['disk_percent'] = 0
            metrics['disk_total'] = 'N/A'
            metrics['disk_free'] = 'N/A'
            metrics['disk_free_gb'] = 0
            metrics['network_bytes_sent'] = 'N/A'
            metrics['network_bytes_recv'] = 'N/A'
            metrics['network_connections'] = 'N/A'
            metrics['network_errors'] = 0
            metrics['uptime'] = 'N/A'
            metrics['process_id'] = os.getpid()
            metrics['process_memory'] = 'N/A'
            metrics['thread_count'] = threading.active_count()
            metrics['open_files'] = 'N/A'
            metrics['top_processes'] = []
            metrics['disk_partitions'] = []

        # System info (always available)
        metrics['hostname'] = platform.node()
        metrics['platform'] = f"{platform.system()} {platform.release()}"
        metrics['python_version'] = platform.python_version()
        metrics['python_executable'] = sys.executable
        return metrics

    def _get_dag_stats(self):
        """Get DAG server statistics for the dashboard cards."""
        stats = {
            'total_dags': 0,
            'running_dags': 0,
            'stopped_dags': 0,
            'error_dags': 0,
            'active_threads': threading.active_count()
        }
        if self.dag_server:
            try:
                dags = self.dag_server.list_dags()
                stats['total_dags'] = len(dags)
                for dag in dags:
                    # v2.0.0 BUGFIX: list_dags() emits 'state', not 'status';
                    # the legacy code read a key that never existed so every
                    # counter stayed at zero.
                    status = str(dag.get('state',
                                         dag.get('status',
                                                 'unknown'))).lower()
                    if status == 'running':
                        stats['running_dags'] += 1
                    elif status in ['stopped', 'completed', 'created']:
                        stats['stopped_dags'] += 1
                    elif status == 'error':
                        stats['error_dags'] += 1
            except Exception as e:  # noqa: BLE001
                logger.error(f"Error getting DAG stats: {e}")
                logger.error(traceback.format_exc())
        return stats

    def _get_health_checks(self):
        """Perform availability checks on optional integrations."""
        checks = []

        dag_healthy = self.dag_server is not None
        checks.append({'name': 'DAG Server',
                       'status': 'healthy' if dag_healthy else 'unhealthy',
                       'message': 'Running' if dag_healthy
                       else 'Not initialized',
                       'response_time': '< 1ms'})

        checks.append({'name': 'System Metrics (psutil)',
                       'status': 'healthy' if PSUTIL_AVAILABLE
                       else 'unknown',
                       'message': 'Available' if PSUTIL_AVAILABLE
                       else 'Not installed',
                       'response_time': '< 1ms' if PSUTIL_AVAILABLE
                       else 'N/A'})

        try:
            from py4j.java_gateway import JavaGateway  # noqa: F401
            py4j_status, py4j_msg = 'healthy', 'Library available'
        except ImportError:
            py4j_status, py4j_msg = 'unknown', 'Not installed'
        checks.append({'name': 'Java Integration (Py4J)',
                       'status': py4j_status, 'message': py4j_msg,
                       'response_time': 'N/A'})

        try:
            import requests  # noqa: F401
            requests_status, requests_msg = 'healthy', 'Available'
        except ImportError:
            requests_status, requests_msg = 'unknown', 'Not installed'
        checks.append({'name': 'REST Calculator Support',
                       'status': requests_status, 'message': requests_msg,
                       'response_time': 'N/A'})
        return checks

    def _check_calculator_availability(self):
        """Check which native calculator bridges are importable."""
        result = {'py4j_available': False, 'pybind11_available': False,
                  'pyo3_available': False, 'requests_available': False}
        try:
            import py4j  # noqa: F401
            result['py4j_available'] = True
        except ImportError:
            pass
        try:
            import dishtayantra_cpp  # noqa: F401
            result['pybind11_available'] = True
        except ImportError:
            pass
        try:
            import dishtayantra_rust  # noqa: F401
            result['pyo3_available'] = True
        except ImportError:
            pass
        try:
            import requests  # noqa: F401
            result['requests_available'] = True
        except ImportError:
            pass
        return result

    def _get_python_info(self):
        """Python environment information (FastAPI version in v2.0.0)."""
        info = {'python_version': platform.python_version(),
                'python_executable': sys.executable,
                'fastapi_version': 'Unknown'}
        try:
            import fastapi
            info['fastapi_version'] = fastapi.__version__
        except Exception as e:  # noqa: BLE001
            logger.error(f"Could not determine FastAPI version: {e}")
            logger.error(traceback.format_exc())

        # Free-threading check (Python 3.13+)
        info['free_threading_enabled'] = False
        info['gil_status'] = 'Enabled'
        if hasattr(sys, '_is_gil_enabled'):
            try:
                gil_enabled = sys._is_gil_enabled()
                info['free_threading_enabled'] = not gil_enabled
                info['gil_status'] = 'Disabled' if not gil_enabled \
                    else 'Enabled'
            except Exception as e:  # noqa: BLE001
                logger.error(f"Could not query GIL status: {e}")
                logger.error(traceback.format_exc())
        return info

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #

    def system_monitoring(self, request: Request):
        """System monitoring dashboard."""
        self.guards.admin_required(request)
        metrics = self._get_system_metrics()
        dag_stats = self._get_dag_stats()
        health_checks = self._get_health_checks()
        calculator_info = self._check_calculator_availability()
        python_info = self._get_python_info()

        # Remove duplicates - python_info takes precedence
        metrics.pop('python_version', None)
        metrics.pop('python_executable', None)

        return render(request, 'admin/system_monitoring.html',
                      current_time=datetime.now().strftime(
                          '%Y-%m-%d %H:%M:%S'),
                      app_version=VERSION,
                      dag_stats=dag_stats,
                      health_checks=health_checks,
                      **metrics,
                      **calculator_info,
                      **python_info)

    def system_metrics_api(self, request: Request):
        """API endpoint for real-time metrics updates."""
        self.guards.admin_required(request)
        metrics = self._get_system_metrics()
        return JSONResponse({
            'cpu_percent': metrics['cpu_percent'],
            'memory_percent': metrics['memory_percent'],
            'disk_percent': metrics['disk_percent'],
            'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
