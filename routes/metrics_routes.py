"""
Metrics Routes for Prometheus Integration (FastAPI, v2.0.0)
===========================================================

Endpoints for Prometheus scraping and health probes.  These routes do NOT
require authentication so Prometheus and load balancers can scrape and
probe freely.

Endpoints:
    /metrics          - Prometheus metrics (text exposition format)
    /health           - Combined health check (200 healthy / 503 otherwise)
    /health/live      - Kubernetes liveness probe
    /health/ready     - Kubernetes readiness probe
    /metrics/json     - Key metrics in JSON for debugging

v2.0.0: the hardcoded version string '1.5.0' is replaced by
:data:`core.version.VERSION`; metric-update failures are logged with full
stack traces instead of being silently downgraded.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import time
import traceback
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from core.metrics import metrics, update_system_metrics
from core.version import VERSION

logger = logging.getLogger(__name__)


class MetricsRoutes:
    """Route handler for Prometheus metrics and health endpoints."""

    def __init__(self, app: FastAPI, dag_server=None, redis_cache=None):
        """
        Args:
            app: FastAPI application instance.
            dag_server: DAGComputeServer instance (optional).
            redis_cache: InMemoryRedisClone instance (optional).
        """
        self.app = app
        self.dag_server = dag_server
        self.redis_cache = redis_cache
        self._start_time = time.time()
        # Per-DAG cumulative messages-processed totals seen on the last scrape.
        # messages_received is a Prometheus Counter (inc-only), but the source
        # of truth (subscriber._receive_count) is an absolute running total, so
        # we bridge by incrementing the counter by the per-scrape delta.
        self._last_msg_counts = {}
        self._register_routes()
        logger.info("Metrics routes initialized")

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/metrics', self.prometheus_metrics, methods=['GET'],
            name='prometheus_metrics', include_in_schema=False)
        add('/health', self.health_check, methods=['GET'],
            name='health_check')
        add('/health/live', self.liveness_probe, methods=['GET'],
            name='liveness_probe')
        add('/health/ready', self.readiness_probe, methods=['GET'],
            name='readiness_probe')
        add('/metrics/json', self.metrics_json, methods=['GET'],
            name='metrics_json')

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #

    def prometheus_metrics(self, request: Request):
        """Prometheus metrics endpoint (text exposition format)."""
        try:
            update_system_metrics()
            metrics.uptime_seconds.set(time.time() - self._start_time)
            if self.dag_server:
                self._update_dag_metrics()
            if self.redis_cache:
                self._update_cache_metrics()
            output = metrics.generate_latest()
            return Response(content=output,
                            media_type=metrics.get_content_type())
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error generating metrics: {e}")
            logger.error(traceback.format_exc())
            return Response(content=f"# Error generating metrics: {e}\n",
                            media_type='text/plain', status_code=500)

    def health_check(self, request: Request):
        """Combined health check endpoint (200 healthy / 503 unhealthy)."""
        health = self._get_health_status()
        status_code = 200 if health['status'] == 'healthy' else 503
        return JSONResponse(health, status_code=status_code)

    def liveness_probe(self, request: Request):
        """Kubernetes liveness probe: 200 while the process is running."""
        return JSONResponse({
            'status': 'alive',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    def readiness_probe(self, request: Request):
        """Kubernetes readiness probe: 200 when ready to receive traffic."""
        ready = True
        checks = {}

        if self.dag_server:
            dag_ready = hasattr(self.dag_server, 'dags')
            checks['dag_server'] = 'ready' if dag_ready else 'not_ready'
            ready = ready and dag_ready

        if self.redis_cache:
            cache_ready = True  # InMemory is always ready
            checks['cache'] = 'ready' if cache_ready else 'not_ready'
            ready = ready and cache_ready

        status_code = 200 if ready else 503
        return JSONResponse({
            'status': 'ready' if ready else 'not_ready',
            'checks': checks,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }, status_code=status_code)

    def metrics_json(self, request: Request):
        """A subset of key metrics in JSON format for debugging."""
        try:
            update_system_metrics()
            json_metrics = {
                'application': {
                    'name': 'DishtaYantra',
                    'version': VERSION,
                    'uptime_seconds': time.time() - self._start_time
                },
                'dags': {},
                'cache': {},
                'system': {}
            }

            if self.dag_server:
                try:
                    dags = getattr(self.dag_server, 'dags', {})
                    per_dag = {}
                    total_processed = 0
                    for name, d in dags.items():
                        processed = self._dag_messages_processed(d)
                        total_processed += processed
                        per_dag[name] = {
                            'running': bool(getattr(d, 'is_running', False)),
                            'messages_processed': processed,
                        }
                    json_metrics['dags'] = {
                        'loaded': len(dags),
                        'running': len([d for d in dags.values()
                                        if getattr(d, 'is_running', False)]),
                        'messages_processed_total': total_processed,
                        'per_dag': per_dag,
                    }
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Unable to fetch DAG stats: {e}")
                    logger.error(traceback.format_exc())
                    json_metrics['dags'] = {'error': 'Unable to fetch DAG '
                                                     'stats'}

            if self.redis_cache:
                try:
                    json_metrics['cache'] = {
                        'keys': len(getattr(self.redis_cache, 'data', {}))
                    }
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Unable to fetch cache stats: {e}")
                    logger.error(traceback.format_exc())
                    json_metrics['cache'] = {'error': 'Unable to fetch '
                                                      'cache stats'}

            try:
                import psutil
                json_metrics['system'] = {
                    'cpu_percent': psutil.cpu_percent(),
                    'memory_percent': psutil.virtual_memory().percent
                }
            except ImportError:
                json_metrics['system'] = {'note': 'psutil not installed'}

            return JSONResponse(json_metrics)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error generating JSON metrics: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    # ------------------------------------------------------------------ #
    # Metric refresh helpers
    # ------------------------------------------------------------------ #

    def _dag_messages_processed(self, dag):
        """Sum of messages received across a DAG's subscribers.

        This is the count of messages that entered the DAG for processing. It
        reads the absolute running total each subscriber maintains
        (``_receive_count``); returns 0 for DAGs with no subscribers.
        """
        total = 0
        subscribers = getattr(dag, 'subscribers', {}) or {}
        for sub in subscribers.values():
            try:
                total += int(getattr(sub, '_receive_count', 0) or 0)
            except (TypeError, ValueError):
                continue
        return total

    def _update_dag_metrics(self):
        """Update DAG-related gauges (per-DAG nodes/edges + active count) and
        per-DAG messages-processed counters."""
        try:
            if not self.dag_server:
                return
            dags = getattr(self.dag_server, 'dags', {})
            active_count = 0
            for dag_name, dag in dags.items():
                try:
                    nodes = len(getattr(dag, 'nodes', []))
                    edges = len(getattr(dag, 'edges', []))
                    metrics.dag_nodes_total.labels(dag_name=dag_name) \
                        .set(nodes)
                    metrics.dag_edges_total.labels(dag_name=dag_name) \
                        .set(edges)

                    # Per-DAG messages processed: bridge the absolute running
                    # total into the inc-only Prometheus Counter via a delta.
                    total = self._dag_messages_processed(dag)
                    prev = self._last_msg_counts.get(dag_name, 0)
                    delta = total - prev
                    if delta < 0:
                        # DAG restarted / counters reset; rebase without going
                        # negative on the monotonic Counter.
                        delta = total
                    if delta > 0:
                        metrics.messages_received.labels(
                            transport='dag', topic='all',
                            dag_name=dag_name).inc(delta)
                    self._last_msg_counts[dag_name] = total

                    if getattr(dag, 'is_running', False):
                        active_count += 1
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Error updating metrics for DAG "
                                 f"{dag_name}: {e}")
                    logger.error(traceback.format_exc())
            metrics.active_dags.set(active_count)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error updating DAG metrics: {e}")
            logger.error(traceback.format_exc())

    def _update_cache_metrics(self):
        """Update cache-related gauges."""
        try:
            if not self.redis_cache:
                return
            data = getattr(self.redis_cache, 'data', {})
            metrics.cache_size.labels(cache_name='inmemory').set(len(data))
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error updating cache metrics: {e}")
            logger.error(traceback.format_exc())

    def _get_health_status(self):
        """Build the combined health document and refresh gauges."""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'version': VERSION,
            'uptime_seconds': time.time() - self._start_time,
            'components': {}
        }

        if self.dag_server:
            try:
                dag_status = 'healthy' if hasattr(self.dag_server, 'dags') \
                    else 'unhealthy'
                health['components']['dag_server'] = {
                    'status': dag_status,
                    'dags_loaded': len(getattr(self.dag_server, 'dags', {}))
                }
                if dag_status == 'unhealthy':
                    health['status'] = 'unhealthy'
            except Exception as e:  # noqa: BLE001
                logger.error(f"Health check error (dag_server): {e}")
                logger.error(traceback.format_exc())
                health['components']['dag_server'] = {'status': 'error'}
                health['status'] = 'unhealthy'

        if self.redis_cache:
            try:
                health['components']['cache'] = {
                    'status': 'healthy',
                    'type': 'inmemory',
                    'keys': len(getattr(self.redis_cache, 'data', {}))
                }
            except Exception as e:  # noqa: BLE001
                logger.error(f"Health check error (cache): {e}")
                logger.error(traceback.format_exc())
                health['components']['cache'] = {'status': 'error'}
                health['status'] = 'unhealthy'

        for component, status in health['components'].items():
            value = 1 if status.get('status') == 'healthy' else 0
            metrics.health_check_status.labels(component=component) \
                .set(value)
        return health
