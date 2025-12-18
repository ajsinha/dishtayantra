"""
Admin Routes for DishtaYantra
=============================

Routes for system monitoring, logs, and administrative functions.
All routes require admin role.
"""

import os
import sys
import platform
import threading
import logging
from datetime import datetime, timedelta
from functools import wraps
from flask import render_template, jsonify, session, redirect, url_for, flash, request, send_file

logger = logging.getLogger(__name__)

# Try to import psutil for system metrics
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not installed. System monitoring will have limited functionality.")


def admin_required(f):
    """Decorator to require admin role for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login'))
        if 'admin' not in session.get('roles', []):
            flash('Admin access required.', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function


def format_bytes(bytes_val):
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} PB"


def format_uptime(seconds):
    """Format uptime seconds to human-readable string."""
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
    """Admin routes handler."""
    
    def __init__(self, app, dag_server=None):
        self.app = app
        self.dag_server = dag_server
        self._register_routes()
    
    def _register_routes(self):
        """Register all admin routes."""
        self.app.add_url_rule('/admin/monitoring', 'system_monitoring', 
                             admin_required(self.system_monitoring))
        self.app.add_url_rule('/admin/monitoring/api', 'system_metrics_api', 
                             admin_required(self.system_metrics_api))
        self.app.add_url_rule('/admin/logs', 'system_logs', 
                             admin_required(self.system_logs))
        self.app.add_url_rule('/admin/logs/api', 'system_logs_api', 
                             admin_required(self.system_logs_api))
        self.app.add_url_rule('/admin/logs/download', 'download_logs', 
                             admin_required(self.download_logs))
    
    def _get_system_metrics(self):
        """Gather system metrics."""
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
                metrics['load_avg_1'] = 'N/A'
                metrics['load_avg_5'] = 'N/A'
                metrics['load_avg_15'] = 'N/A'
            
            # Memory
            mem = psutil.virtual_memory()
            metrics['memory_percent'] = mem.percent
            metrics['memory_total'] = format_bytes(mem.total)
            metrics['memory_available'] = format_bytes(mem.available)
            metrics['memory_available_gb'] = mem.available / (1024**3)
            
            # Disk
            disk = psutil.disk_usage('/')
            metrics['disk_percent'] = disk.percent
            metrics['disk_total'] = format_bytes(disk.total)
            metrics['disk_free'] = format_bytes(disk.free)
            metrics['disk_free_gb'] = disk.free / (1024**3)
            
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
            metrics['process_memory'] = format_bytes(process.memory_info().rss)
            metrics['thread_count'] = process.num_threads()
            
            try:
                metrics['open_files'] = len(process.open_files())
            except (psutil.AccessDenied, PermissionError):
                metrics['open_files'] = 'N/A'
            
            # Top processes
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']):
                try:
                    info = proc.info
                    info['memory_percent'] = round(info['memory_percent'] or 0, 1)
                    info['cpu_percent'] = round(info['cpu_percent'] or 0, 1)
                    processes.append(info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            # Sort by memory and get top 10
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
                    pass
            metrics['disk_partitions'] = partitions[:5]  # Limit to 5
            
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
        """Get DAG server statistics."""
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
                    status = dag.get('status', 'unknown').lower()
                    if status == 'running':
                        stats['running_dags'] += 1
                    elif status in ['stopped', 'completed']:
                        stats['stopped_dags'] += 1
                    elif status == 'error':
                        stats['error_dags'] += 1
            except Exception as e:
                logger.error(f"Error getting DAG stats: {e}")
        
        return stats
    
    def _get_health_checks(self):
        """Perform health checks on services."""
        checks = []
        
        # DAG Server
        dag_healthy = self.dag_server is not None
        checks.append({
            'name': 'DAG Server',
            'status': 'healthy' if dag_healthy else 'unhealthy',
            'message': 'Running' if dag_healthy else 'Not initialized',
            'response_time': '< 1ms'
        })
        
        # Check if psutil is available
        checks.append({
            'name': 'System Metrics (psutil)',
            'status': 'healthy' if PSUTIL_AVAILABLE else 'unknown',
            'message': 'Available' if PSUTIL_AVAILABLE else 'Not installed',
            'response_time': '< 1ms' if PSUTIL_AVAILABLE else 'N/A'
        })
        
        # Check Py4J
        try:
            from py4j.java_gateway import JavaGateway
            py4j_status = 'healthy'
            py4j_msg = 'Library available'
        except ImportError:
            py4j_status = 'unknown'
            py4j_msg = 'Not installed'
        
        checks.append({
            'name': 'Java Integration (Py4J)',
            'status': py4j_status,
            'message': py4j_msg,
            'response_time': 'N/A'
        })
        
        # Check requests library
        try:
            import requests
            requests_status = 'healthy'
            requests_msg = 'Available'
        except ImportError:
            requests_status = 'unknown'
            requests_msg = 'Not installed'
        
        checks.append({
            'name': 'REST Calculator Support',
            'status': requests_status,
            'message': requests_msg,
            'response_time': 'N/A'
        })
        
        return checks
    
    def _check_calculator_availability(self):
        """Check which calculator types are available."""
        result = {
            'py4j_available': False,
            'pybind11_available': False,
            'pyo3_available': False,
            'requests_available': False
        }
        
        # Check Py4J
        try:
            import py4j
            result['py4j_available'] = True
        except ImportError:
            pass
        
        # Check pybind11 module
        try:
            import dishtayantra_cpp
            result['pybind11_available'] = True
        except ImportError:
            pass
        
        # Check PyO3/Rust module
        try:
            import dishtayantra_rust
            result['pyo3_available'] = True
        except ImportError:
            pass
        
        # Check requests
        try:
            import requests
            result['requests_available'] = True
        except ImportError:
            pass
        
        return result
    
    def _get_python_info(self):
        """Get Python environment information."""
        info = {
            'python_version': platform.python_version(),
            'python_executable': sys.executable,
            'flask_version': 'Unknown'
        }
        
        # Flask version
        try:
            import flask
            info['flask_version'] = flask.__version__
        except:
            pass
        
        # Free-threading check (Python 3.13+)
        info['free_threading_enabled'] = False
        info['gil_status'] = 'Enabled'
        
        try:
            if hasattr(sys, '_is_gil_enabled'):
                gil_enabled = sys._is_gil_enabled()
                info['free_threading_enabled'] = not gil_enabled
                info['gil_status'] = 'Disabled' if not gil_enabled else 'Enabled'
        except:
            pass
        
        return info
    
    def system_monitoring(self):
        """System monitoring dashboard."""
        metrics = self._get_system_metrics()
        dag_stats = self._get_dag_stats()
        health_checks = self._get_health_checks()
        calculator_info = self._check_calculator_availability()
        python_info = self._get_python_info()
        
        # Remove duplicates - python_info takes precedence
        metrics.pop('python_version', None)
        metrics.pop('python_executable', None)
        
        return render_template(
            'admin/system_monitoring.html',
            current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            app_version='1.1.1',
            dag_stats=dag_stats,
            health_checks=health_checks,
            **metrics,
            **calculator_info,
            **python_info
        )
    
    def system_metrics_api(self):
        """API endpoint for real-time metrics updates."""
        metrics = self._get_system_metrics()
        metrics['current_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Only return essential metrics for updates
        return jsonify({
            'cpu_percent': metrics['cpu_percent'],
            'memory_percent': metrics['memory_percent'],
            'disk_percent': metrics['disk_percent'],
            'current_time': metrics['current_time']
        })
    
    def _parse_log_file(self, log_path, max_lines=500):
        """Parse log file and return structured entries."""
        entries = []
        
        if not os.path.exists(log_path):
            return entries
        
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # Get last N lines
            lines = lines[-max_lines:]
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Try to parse structured log format
                # Expected: TIMESTAMP - LEVEL - MESSAGE
                entry = {
                    'timestamp': '',
                    'level': 'INFO',
                    'message': line
                }
                
                # Try common log formats
                if ' - ' in line:
                    parts = line.split(' - ', 2)
                    if len(parts) >= 2:
                        entry['timestamp'] = parts[0][:19]  # Limit timestamp length
                        
                        if len(parts) >= 3:
                            level = parts[1].strip().upper()
                            if level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
                                entry['level'] = level
                                entry['message'] = parts[2]
                            else:
                                entry['message'] = ' - '.join(parts[1:])
                        else:
                            entry['message'] = parts[1]
                
                entries.append(entry)
        
        except Exception as e:
            logger.error(f"Error parsing log file {log_path}: {e}")
        
        return entries
    
    def _get_log_stats(self, entries):
        """Calculate log statistics."""
        stats = {
            'total': len(entries),
            'errors': sum(1 for e in entries if e['level'] == 'ERROR'),
            'warnings': sum(1 for e in entries if e['level'] == 'WARNING'),
            'info': sum(1 for e in entries if e['level'] == 'INFO'),
            'file_size': 'N/A'
        }
        return stats
    
    def system_logs(self):
        """System logs viewer."""
        log_file = request.args.get('file', 'dagserver')
        
        # Map file names to paths
        log_paths = {
            'dagserver': 'logs/dagserver.log',
            'application': 'logs/application.log',
            'error': 'logs/error.log'
        }
        
        log_path = log_paths.get(log_file, 'logs/dagserver.log')
        entries = self._parse_log_file(log_path)
        stats = self._get_log_stats(entries)
        
        # Get file size
        if os.path.exists(log_path):
            stats['file_size'] = format_bytes(os.path.getsize(log_path))
        
        return render_template(
            'admin/system_logs.html',
            log_entries=entries,
            log_stats=stats,
            selected_file=log_file
        )
    
    def system_logs_api(self):
        """API endpoint for log updates."""
        log_file = request.args.get('file', 'dagserver')
        
        log_paths = {
            'dagserver': 'logs/dagserver.log',
            'application': 'logs/application.log',
            'error': 'logs/error.log'
        }
        
        log_path = log_paths.get(log_file, 'logs/dagserver.log')
        entries = self._parse_log_file(log_path)
        
        return jsonify({
            'entries': entries,
            'total': len(entries)
        })
    
    def download_logs(self):
        """Download log file."""
        log_file = request.args.get('file', 'dagserver')
        
        log_paths = {
            'dagserver': 'logs/dagserver.log',
            'application': 'logs/application.log',
            'error': 'logs/error.log'
        }
        
        log_path = log_paths.get(log_file, 'logs/dagserver.log')
        
        if os.path.exists(log_path):
            return send_file(
                log_path,
                as_attachment=True,
                download_name=f'{log_file}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
            )
        else:
            flash('Log file not found.', 'error')
            return redirect(url_for('system_logs'))
