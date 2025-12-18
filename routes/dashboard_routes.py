"""Dashboard routes module"""
import logging
from flask import render_template, session, flash, redirect, url_for

logger = logging.getLogger(__name__)


class DashboardRoutes:
    """Handles dashboard-related routes"""
    
    def __init__(self, app, dag_server, user_registry, login_required):
        self.app = app
        self.dag_server = dag_server
        self.user_registry = user_registry
        self.login_required = login_required
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

            return render_template('dag/details.html',
                                   dag_name=dag_name,
                                   details=details,
                                   node_details=node_details,
                                   is_admin=self.user_registry.has_role(session.get('username'), 'admin'))
        except Exception as e:
            logger.error(f"Error loading DAG details: {str(e)}")
            flash(f'Error loading DAG details: {str(e)}', 'error')
            return redirect(url_for('dashboard'))
    
    def dag_state(self, dag_name):
        """DAG state view"""
        try:
            details = self.dag_server.details(dag_name)

            # Get topological order
            dag = self.dag_server.dags[dag_name]
            sorted_nodes = dag.topological_sort()

            # Build state info with calculation details
            node_states = []
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
                    'input': node._input,
                    'output': node._output,
                    'isdirty': node._isdirty,
                    'errors': list(node._errors),
                    'calculation_count': calculation_count,
                    'last_calculation': last_calculation
                })

            return render_template('dag/state.html',
                                   dag_name=dag_name,
                                   node_states=node_states)
        except Exception as e:
            logger.error(f"Error loading DAG state: {str(e)}")
            flash(f'Error loading DAG state: {str(e)}', 'error')
            return redirect(url_for('dashboard'))
