"""
Metrics Routes for Prometheus Integration
==========================================

Provides endpoints for Prometheus metrics scraping and health checks.

Endpoints:
    /metrics          - Prometheus metrics endpoint
    /health           - Health check endpoint
    /health/live      - Liveness probe
    /health/ready     - Readiness probe
    /metrics/json     - Metrics in JSON format

Author: Ashutosh Sinha
Email: ajsinha@gmail.com
Version: 1.5.0
Date: December 2025
"""

import time
import logging
from datetime import datetime
from flask import Response, jsonify

from core.metrics import metrics, update_system_metrics

logger = logging.getLogger(__name__)


class MetricsRoutes:
    """
    Route handler for Prometheus metrics and health endpoints.
    
    These routes do NOT require authentication to allow Prometheus
    and load balancers to scrape metrics and perform health checks.
    """
    
    def __init__(self, app, dag_server=None, redis_cache=None):
        """
        Initialize metrics routes.
        
        Args:
            app: Flask application instance
            dag_server: DAGComputeServer instance (optional)
            redis_cache: InMemoryRedisClone instance (optional)
        """
        self.app = app
        self.dag_server = dag_server
        self.redis_cache = redis_cache
        self._start_time = time.time()
        
        self._register_routes()
        logger.info("Metrics routes initialized")
    
    def _register_routes(self):
        """Register all metrics endpoints."""
        
        @self.app.route('/metrics')
        def prometheus_metrics():
            """
            Prometheus metrics endpoint.
            
            Returns metrics in Prometheus text format for scraping.
            """
            try:
                # Update system metrics before returning
                update_system_metrics()
                
                # Update uptime
                metrics.uptime_seconds.set(time.time() - self._start_time)
                
                # Update DAG metrics if dag_server available
                if self.dag_server:
                    self._update_dag_metrics()
                
                # Update cache metrics if redis_cache available
                if self.redis_cache:
                    self._update_cache_metrics()
                
                # Generate and return metrics
                output = metrics.generate_latest()
                return Response(
                    output,
                    mimetype=metrics.get_content_type()
                )
            except Exception as e:
                logger.error(f"Error generating metrics: {e}")
                return Response(
                    f"# Error generating metrics: {e}\n",
                    mimetype='text/plain',
                    status=500
                )
        
        @self.app.route('/health')
        def health_check():
            """
            Combined health check endpoint.
            
            Returns overall system health status.
            """
            health = self._get_health_status()
            status_code = 200 if health['status'] == 'healthy' else 503
            return jsonify(health), status_code
        
        @self.app.route('/health/live')
        def liveness_probe():
            """
            Kubernetes liveness probe endpoint.
            
            Returns 200 if the application is running.
            """
            return jsonify({
                'status': 'alive',
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        
        @self.app.route('/health/ready')
        def readiness_probe():
            """
            Kubernetes readiness probe endpoint.
            
            Returns 200 if the application is ready to receive traffic.
            """
            ready = True
            checks = {}
            
            # Check DAG server
            if self.dag_server:
                dag_ready = hasattr(self.dag_server, 'dags')
                checks['dag_server'] = 'ready' if dag_ready else 'not_ready'
                ready = ready and dag_ready
            
            # Check cache
            if self.redis_cache:
                cache_ready = True  # InMemory is always ready
                checks['cache'] = 'ready' if cache_ready else 'not_ready'
                ready = ready and cache_ready
            
            status_code = 200 if ready else 503
            return jsonify({
                'status': 'ready' if ready else 'not_ready',
                'checks': checks,
                'timestamp': datetime.utcnow().isoformat()
            }), status_code
        
        @self.app.route('/metrics/json')
        def metrics_json():
            """
            Metrics in JSON format for debugging.
            
            Returns a subset of key metrics in JSON format.
            """
            try:
                update_system_metrics()
                
                json_metrics = {
                    'application': {
                        'name': 'DishtaYantra',
                        'version': '1.5.0',
                        'uptime_seconds': time.time() - self._start_time
                    },
                    'dags': {},
                    'cache': {},
                    'system': {}
                }
                
                # Add DAG metrics
                if self.dag_server:
                    try:
                        json_metrics['dags'] = {
                            'loaded': len(getattr(self.dag_server, 'dags', {})),
                            'running': len([d for d in getattr(self.dag_server, 'dags', {}).values() 
                                          if getattr(d, 'is_running', False)])
                        }
                    except:
                        json_metrics['dags'] = {'error': 'Unable to fetch DAG stats'}
                
                # Add cache metrics
                if self.redis_cache:
                    try:
                        json_metrics['cache'] = {
                            'keys': len(getattr(self.redis_cache, 'data', {}))
                        }
                    except:
                        json_metrics['cache'] = {'error': 'Unable to fetch cache stats'}
                
                # Add system metrics
                try:
                    import psutil
                    json_metrics['system'] = {
                        'cpu_percent': psutil.cpu_percent(),
                        'memory_percent': psutil.virtual_memory().percent
                    }
                except ImportError:
                    json_metrics['system'] = {'note': 'psutil not installed'}
                
                return jsonify(json_metrics)
                
            except Exception as e:
                logger.error(f"Error generating JSON metrics: {e}")
                return jsonify({'error': str(e)}), 500
    
    def _update_dag_metrics(self):
        """Update DAG-related metrics."""
        try:
            if not self.dag_server:
                return
            
            dags = getattr(self.dag_server, 'dags', {})
            active_count = 0
            
            for dag_name, dag in dags.items():
                try:
                    # Count nodes and edges
                    nodes = len(getattr(dag, 'nodes', []))
                    edges = len(getattr(dag, 'edges', []))
                    
                    metrics.dag_nodes_total.labels(dag_name=dag_name).set(nodes)
                    metrics.dag_edges_total.labels(dag_name=dag_name).set(edges)
                    
                    # Check if running
                    if getattr(dag, 'is_running', False):
                        active_count += 1
                except Exception as e:
                    logger.debug(f"Error updating metrics for DAG {dag_name}: {e}")
            
            metrics.active_dags.set(active_count)
            
        except Exception as e:
            logger.debug(f"Error updating DAG metrics: {e}")
    
    def _update_cache_metrics(self):
        """Update cache-related metrics."""
        try:
            if not self.redis_cache:
                return
            
            # Get cache size
            data = getattr(self.redis_cache, 'data', {})
            metrics.cache_size.labels(cache_name='inmemory').set(len(data))
            
        except Exception as e:
            logger.debug(f"Error updating cache metrics: {e}")
    
    def _get_health_status(self):
        """Get overall health status."""
        health = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.5.0',
            'uptime_seconds': time.time() - self._start_time,
            'components': {}
        }
        
        # Check DAG server
        if self.dag_server:
            try:
                dag_status = 'healthy' if hasattr(self.dag_server, 'dags') else 'unhealthy'
                health['components']['dag_server'] = {
                    'status': dag_status,
                    'dags_loaded': len(getattr(self.dag_server, 'dags', {}))
                }
                if dag_status == 'unhealthy':
                    health['status'] = 'unhealthy'
            except:
                health['components']['dag_server'] = {'status': 'error'}
                health['status'] = 'unhealthy'
        
        # Check cache
        if self.redis_cache:
            try:
                health['components']['cache'] = {
                    'status': 'healthy',
                    'type': 'inmemory',
                    'keys': len(getattr(self.redis_cache, 'data', {}))
                }
            except:
                health['components']['cache'] = {'status': 'error'}
                health['status'] = 'unhealthy'
        
        # Update health check metrics
        for component, status in health['components'].items():
            value = 1 if status.get('status') == 'healthy' else 0
            metrics.health_check_status.labels(component=component).set(value)
        
        return health
