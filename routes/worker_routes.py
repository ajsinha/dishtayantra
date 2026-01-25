"""
Worker Management Routes
Version: 1.5.2

Provides web routes for:
- Viewing worker pool status
- Managing worker processes
- DAG-to-worker assignments
- Worker health monitoring

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import logging
from flask import render_template, jsonify, request, redirect, url_for, flash

logger = logging.getLogger(__name__)


class WorkerRoutes:
    """
    Routes for worker pool management and monitoring.
    """
    
    def __init__(self, app, worker_pool_manager=None, admin_required=None):
        """
        Initialize worker routes.
        
        Args:
            app: Flask application
            worker_pool_manager: WorkerPoolManager instance (optional, can be set later)
            admin_required: Admin authentication decorator
        """
        self.app = app
        self.worker_pool = worker_pool_manager
        self.admin_required = admin_required or (lambda f: f)
        
        self._register_routes()
    
    def set_worker_pool(self, worker_pool_manager):
        """Set the worker pool manager"""
        self.worker_pool = worker_pool_manager
    
    def _register_routes(self):
        """Register all worker management routes"""
        
        # Main workers page
        self.app.add_url_rule(
            '/admin/workers',
            'admin_workers',
            self.admin_required(self.workers_index)
        )
        
        # API endpoints
        self.app.add_url_rule(
            '/api/workers/status',
            'api_workers_status',
            self.admin_required(self.api_workers_status)
        )
        
        self.app.add_url_rule(
            '/api/workers/<int:worker_id>',
            'api_worker_details',
            self.admin_required(self.api_worker_details)
        )
        
        self.app.add_url_rule(
            '/api/workers/<int:worker_id>/restart',
            'api_worker_restart',
            self.admin_required(self.api_worker_restart),
            methods=['POST']
        )
        
        self.app.add_url_rule(
            '/api/workers/pool/start',
            'api_pool_start',
            self.admin_required(self.api_pool_start),
            methods=['POST']
        )
        
        self.app.add_url_rule(
            '/api/workers/pool/stop',
            'api_pool_stop',
            self.admin_required(self.api_pool_stop),
            methods=['POST']
        )
        
        # DAG management
        self.app.add_url_rule(
            '/api/workers/dag/<dag_name>/migrate',
            'api_dag_migrate',
            self.admin_required(self.api_dag_migrate),
            methods=['POST']
        )
        
        self.app.add_url_rule(
            '/api/workers/assignments',
            'api_dag_assignments',
            self.admin_required(self.api_dag_assignments)
        )
        
        self.app.add_url_rule(
            '/api/workers/load-summary',
            'api_load_summary',
            self.admin_required(self.api_load_summary)
        )
    
    def workers_index(self):
        """Main workers monitoring page"""
        pool_status = {}
        if self.worker_pool:
            try:
                pool_status = self.worker_pool.get_pool_status()
            except Exception as e:
                logger.error(f"Error getting pool status: {e}")
                pool_status = {'error': str(e)}
        
        return render_template(
            'admin/workers.html',
            pool_status=pool_status,
            is_admin=True
        )
    
    def api_workers_status(self):
        """API: Get status of all workers"""
        if not self.worker_pool:
            return jsonify({
                'error': 'Worker pool not initialized',
                'pool_enabled': False
            }), 200
        
        try:
            status = self.worker_pool.get_pool_status()
            return jsonify(status)
        except Exception as e:
            logger.error(f"Error getting workers status: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_worker_details(self, worker_id: int):
        """API: Get detailed status of a specific worker"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not initialized'}), 400
        
        try:
            status = self.worker_pool.get_worker_status(worker_id)
            if status is None:
                return jsonify({'error': f'Worker {worker_id} not found'}), 404
            return jsonify(status)
        except Exception as e:
            logger.error(f"Error getting worker {worker_id} status: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_worker_restart(self, worker_id: int):
        """API: Manually restart a worker"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not initialized'}), 400
        
        try:
            # Trigger restart through health monitor
            health = self.worker_pool.health_monitor.get_health_status(worker_id)
            if health is None:
                return jsonify({'error': f'Worker {worker_id} not found'}), 404
            
            # Force restart
            self.worker_pool._handle_worker_restart(worker_id, health)
            
            return jsonify({
                'success': True,
                'message': f'Worker {worker_id} restart initiated'
            })
        except Exception as e:
            logger.error(f"Error restarting worker {worker_id}: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_pool_start(self):
        """API: Start the worker pool"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not configured'}), 400
        
        try:
            if self.worker_pool.is_running():
                return jsonify({
                    'success': False,
                    'message': 'Worker pool already running'
                })
            
            self.worker_pool.start()
            return jsonify({
                'success': True,
                'message': 'Worker pool started'
            })
        except Exception as e:
            logger.error(f"Error starting worker pool: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_pool_stop(self):
        """API: Stop the worker pool"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not initialized'}), 400
        
        try:
            if not self.worker_pool.is_running():
                return jsonify({
                    'success': False,
                    'message': 'Worker pool not running'
                })
            
            self.worker_pool.stop()
            return jsonify({
                'success': True,
                'message': 'Worker pool stopped'
            })
        except Exception as e:
            logger.error(f"Error stopping worker pool: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_dag_migrate(self, dag_name: str):
        """API: Migrate a DAG to a different worker"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not initialized'}), 400
        
        try:
            data = request.get_json() or {}
            to_worker = data.get('to_worker')
            
            if to_worker is None:
                return jsonify({'error': 'to_worker is required'}), 400
            
            to_worker = int(to_worker)
            
            success = self.worker_pool.migrate_dag(dag_name, to_worker)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f'DAG {dag_name} migration to worker {to_worker} initiated'
                })
            else:
                return jsonify({
                    'success': False,
                    'message': f'Failed to migrate DAG {dag_name}'
                }), 400
                
        except Exception as e:
            logger.error(f"Error migrating DAG {dag_name}: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_dag_assignments(self):
        """API: Get all DAG-to-worker assignments"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not initialized'}), 400
        
        try:
            assignments = self.worker_pool.get_all_dag_assignments()
            return jsonify({
                'assignments': assignments,
                'total_dags': len(assignments)
            })
        except Exception as e:
            logger.error(f"Error getting DAG assignments: {e}")
            return jsonify({'error': str(e)}), 500
    
    def api_load_summary(self):
        """API: Get worker load summary"""
        if not self.worker_pool:
            return jsonify({'error': 'Worker pool not initialized'}), 400
        
        try:
            summary = self.worker_pool.get_load_summary()
            return jsonify(summary)
        except Exception as e:
            logger.error(f"Error getting load summary: {e}")
            return jsonify({'error': str(e)}), 500
