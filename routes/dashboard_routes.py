"""Dashboard routes module"""
import logging
from flask import render_template, session, flash, redirect, url_for

logger = logging.getLogger(__name__)


class DashboardRoutes:
    """Handles dashboard-related routes"""
    
    def __init__(self, app, dag_server, user_registry, login_required, worker_pool=None):
        self.app = app
        self.dag_server = dag_server
        self.user_registry = user_registry
        self.login_required = login_required
        self.worker_pool = worker_pool  # v1.5.2: Worker pool for DAG assignments
        self._register_routes()
    
    def _register_routes(self):
        """Register all dashboard routes"""
        self.app.add_url_rule('/dashboard', 'dashboard', 
                             self.login_required(self.dashboard))
        self.app.add_url_rule('/dag/<dag_name>/details', 'dag_details', 
                             self.login_required(self.dag_details))
        self.app.add_url_rule('/dag/<dag_name>/state', 'dag_state', 
                             self.login_required(self.dag_state))
    
    def dashboard(self):
        """Main dashboard view"""
        try:
            server_status = self.dag_server.get_server_status()
            dags = self.dag_server.list_dags()
            return render_template('dashboard.html',
                                   dags=dags,
                                   is_admin=self.user_registry.has_role(session.get('username'), 'admin'),
                                   server_status=server_status)
        except Exception as e:
            logger.error(f"Error loading dashboard: {str(e)}")
            flash(f'Error loading dashboard: {str(e)}', 'error')
            return render_template('dashboard.html', dags=[], is_admin=False, server_status={})
    
    def dag_details(self, dag_name):
        """DAG details view"""
        try:
            details = self.dag_server.details(dag_name)

            # Get topological order
            dag = self.dag_server.dags[dag_name]
            sorted_nodes = dag.topological_sort()

            # Build dependency info with additional details
            node_details = []
            for node in sorted_nodes:
                dependencies = [edge.from_node.name for edge in node._incoming_edges]

                # Get last calculation time
                last_calculation = None
                if hasattr(node, '_last_compute'):
                    last_calculation = node._last_compute

                # Get errors
                errors = []
                if hasattr(node, '_errors'):
                    errors = list(node._errors)

                node_details.append({
                    'name': node.name,
                    'type': node.__class__.__name__,
                    'dependencies': dependencies,
                    'config': node.config,
                    'last_calculation': last_calculation,
                    'errors': errors
                })

            # v1.5.2: Get execution location information
            # Check if DAG is running
            is_running = dag._compute_thread and dag._compute_thread.is_alive() if hasattr(dag, '_compute_thread') else False
            
            worker_info = None
            worker_pool_enabled = self.worker_pool is not None and self.worker_pool.is_running()
            
            if worker_pool_enabled:
                # Worker pool is enabled - check if DAG is assigned to a worker
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    # DAG is assigned to a worker
                    worker_status = self.worker_pool.get_worker_status(worker_id)
                    worker_info = {
                        'worker_id': worker_id,
                        'status': worker_status.get('state', 'unknown') if worker_status else 'unknown',
                        'pid': worker_status.get('pid') if worker_status else None,
                        'dag_count': worker_status.get('dag_count', 0) if worker_status else 0,
                        'cpu_percent': worker_status.get('cpu_percent', 0) if worker_status else 0,
                        'memory_mb': worker_status.get('memory_mb', 0) if worker_status else 0,
                        'execution_mode': 'worker'
                    }
                else:
                    # Worker pool enabled but DAG running in main process
                    import os
                    worker_info = {
                        'worker_id': None,
                        'execution_mode': 'main_process',
                        'status': 'running' if is_running else 'stopped',
                        'pid': os.getpid(),
                        'message': 'Running in main process (DAG server)'
                    }
            else:
                # Worker pool disabled - single process mode
                import os
                worker_info = {
                    'worker_id': None,
                    'execution_mode': 'single_process',
                    'status': 'running' if is_running else 'stopped',
                    'pid': os.getpid(),
                    'message': 'Single-process mode (worker pool disabled)'
                }

            return render_template('dag/details.html',
                                   dag_name=dag_name,
                                   details=details,
                                   node_details=node_details,
                                   worker_info=worker_info,
                                   worker_pool_enabled=worker_pool_enabled,
                                   is_running=is_running,
                                   is_admin=self.user_registry.has_role(session.get('username'), 'admin'))
        except Exception as e:
            logger.error(f"Error loading DAG details: {str(e)}")
            flash(f'Error loading DAG details: {str(e)}', 'error')
            return redirect(url_for('dashboard'))
    
    def dag_state(self, dag_name):
        """DAG state view
        
        v1.5.2: Updated to handle lazy-initialized DAGs.
        When DAG is running in a worker, fetches actual state from the worker process.
        """
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                flash(f'DAG {dag_name} not found', 'error')
                return redirect(url_for('dashboard'))
            
            # Check if running in worker
            worker_id = None
            worker_state = None
            if self.worker_pool and self.worker_pool.is_running():
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
                
                # v1.5.2: If running in worker, fetch actual state from worker
                if worker_id is not None:
                    logger.info(f"Fetching DAG state for {dag_name} from Worker {worker_id}")
                    worker_state = self.worker_pool.get_dag_state(dag_name, timeout=5.0)
                    if worker_state:
                        logger.info(f"Received state from worker: {len(worker_state.get('node_states', []))} nodes")
            
            # v1.5.2: Check if DAG is lazy-initialized (running in worker pool)
            # A DAG is lazy-initialized if _components_built is False OR if nodes dict is empty
            is_lazy_init = (
                (hasattr(dag, '_components_built') and not dag._components_built) or
                (not dag.nodes)  # Empty nodes dict means not built
            )
            
            node_states = []
            subscriber_states = {}
            
            if worker_state and worker_state.get('node_states'):
                # Use state from worker - this is the actual runtime state!
                logger.info(f"Using actual runtime state from Worker {worker_id}")
                node_states = worker_state['node_states']
                subscriber_states = worker_state.get('subscriber_states', {})
                is_running_in_worker = True
            elif is_lazy_init:
                # DAG components not built in main process - get info from config
                logger.info(f"DAG {dag_name} is lazy-initialized, showing state from config")
                
                for node_cfg in dag.config.get('nodes', []):
                    node_name = node_cfg.get('name')
                    node_type = node_cfg.get('type', 'Unknown')
                    
                    node_states.append({
                        'name': node_name,
                        'type': node_type,
                        'input': None,
                        'output': None,
                        'isdirty': False,
                        'errors': [],
                        'calculation_count': None,
                        'last_calculation': None,
                        'status_note': 'Running in Worker' if worker_id is not None else 'Not Started'
                    })
                is_running_in_worker = worker_id is not None
            else:
                # Components built in main process - get actual state
                sorted_nodes = dag.topological_sort()
                logger.info(f"DAG {dag_name} has built components, {len(sorted_nodes)} nodes")

                for node in sorted_nodes:
                    # Get calculation count
                    calculation_count = None
                    if hasattr(node, '_compute_count'):
                        calculation_count = node._compute_count
                    elif hasattr(node, 'compute_count'):
                        calculation_count = node.compute_count

                    # Get last calculation time
                    last_calculation = None
                    if hasattr(node, '_last_compute'):
                        last_calculation = node._last_compute
                    elif hasattr(node, 'last_compute'):
                        last_calculation = node.last_compute

                    node_states.append({
                        'name': node.name,
                        'type': type(node).__name__,
                        'input': node._input,
                        'output': node._output,
                        'isdirty': node._isdirty,
                        'errors': list(node._errors),
                        'calculation_count': calculation_count,
                        'last_calculation': last_calculation,
                        'status_note': None
                    })
                is_running_in_worker = False

            logger.info(f"Rendering state page with {len(node_states)} nodes")
            return render_template('dag/state.html',
                                   dag_name=dag_name,
                                   node_states=node_states,
                                   subscriber_states=subscriber_states,
                                   is_lazy_init=is_lazy_init,
                                   worker_id=worker_id,
                                   has_worker_state=worker_state is not None)
        except Exception as e:
            logger.error(f"Error loading DAG state: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            flash(f'Error loading DAG state: {str(e)}', 'error')
            return redirect(url_for('dashboard'))
