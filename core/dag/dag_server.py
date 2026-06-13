import json
import logging
import os
import threading
import traceback
from datetime import datetime
from core.dag.compute_graph import ComputeGraph
from core.dag.dag_server_autoclone import DAGAutoCloneMixin
from core.dag.dag_server_support import DAGLoaderMixin, DAGMonitorMixin
from core.dag.time_window_utils import parse_duration, calculate_end_time
from core.ha import create_ha_provider
from core.properties_configurator import PropertiesConfigurator
from core.storage import get_storage_provider

logger = logging.getLogger(__name__)


class DAGComputeServer(DAGLoaderMixin, DAGMonitorMixin, DAGAutoCloneMixin):
    """Server for managing multiple DAG compute graphs.

    v2.0.0 changes:
        - DAG configuration files are read/written through the pluggable
          storage abstraction (core.storage). The logical prefix under which
          DAG JSONs live is configured by the 'storage.dags.prefix' property.
        - Leader election is delegated to the configurable HA Manager
          (core.ha): ha.provider = none | zookeeper | redis | s3 | socket.
          The legacy 'zookeeper_hosts' constructor argument is retained for
          API compatibility but is ignored - HA is fully property-driven.
    v1.5.2: Accepts worker_pool directly from run_server.py.
    """

    def __init__(self, dag_config_folder, zookeeper_hosts='localhost:2181', worker_pool=None):
        """
        Initialize DAGComputeServer.

        Args:
            dag_config_folder: Legacy local folder path. Used only when the
                'storage.dags.prefix' property is absent (backward
                compatibility with pre-2.0 callers).
            zookeeper_hosts: DEPRECATED - ignored. Configure HA via the
                ha.* properties instead (see core/ha/ha_manager.py).
            worker_pool: WorkerPoolManager instance for multiprocess
                execution (v1.5.2).
        """
        self._props = PropertiesConfigurator()
        self._storage = get_storage_provider(self._props)
        # Logical prefix (relative to the storage root) for DAG JSON files.
        configured_prefix = self._props.get('storage.dags.prefix')
        if configured_prefix is not None:
            self.dag_config_prefix = configured_prefix.strip().strip('/')
        else:
            # Backward compatibility with the legacy constructor argument.
            self.dag_config_prefix = dag_config_folder.lstrip('./').strip('/')
            logger.warning(
                "Property 'storage.dags.prefix' is not set - falling back to "
                "legacy folder argument '%s'. Please define the property.",
                self.dag_config_prefix)
        self.dag_config_folder = dag_config_folder  # retained for legacy callers
        self.dags = {}
        self._lock = threading.Lock()
        self._auto_started = False  # v1.5.2: Track if auto-start was already done
        
        # v1.5.2: Worker pool - now passed directly from run_server.py
        self._worker_pool = worker_pool
        if worker_pool:
            logger.info(f"DAGComputeServer initialized with worker pool ({worker_pool.num_workers} workers)")

        # AutoClone tracking
        self.autoclone_info = {}  # dag_name -> {clones: [clone_names], config: {}, status: ''}
        self._autoclone_thread = None
        self._autoclone_stop_event = threading.Event()

        # Time window monitor - checks DAGs outside window and starts them when they enter
        self._time_window_monitor_thread = None
        self._time_window_stop_event = threading.Event()

        # v2.1.0: intraday schedule refresh - re-reads each DAG's schedule
        # from storage so edits take effect automatically within 5 minutes
        # (refresh <=240s + 60s monitor cycle = 300s worst case).
        self._schedule_refresh_thread = None
        refresh = int(self._props.get('schedule.refresh_seconds', 240))
        if refresh > 240:
            logger.warning("schedule.refresh_seconds=%d exceeds the 240s "
                           "cap required for the 5-minute intraday-update "
                           "guarantee - capping.", refresh)
            refresh = 240
        self._schedule_refresh_seconds = max(30, refresh)

        # HA Manager setup (v2.0.0): provider chosen by 'ha.provider'.
        # 'is_primary' is kept as a plain attribute for backward compatibility
        # with code/templates that read it directly; the HA provider keeps it
        # in sync via the elected/demoted callbacks below.
        self.is_primary = False
        self.ha_provider = None
        try:
            self.ha_provider = create_ha_provider(self._props)
            self.ha_provider.on_elected(self._on_elected)
            self.ha_provider.on_demoted(self._on_demoted)
            self.ha_provider.start()
        except Exception as e:
            logger.error(f"Error starting HA provider: {str(e)}")
            logger.error(traceback.format_exc())
            raise

        logger.info("Initializing DAGComputeServer...")
        self._load_dags()

        # Start autoclone management thread
        self._start_autoclone_manager()

        # Start time window monitor thread
        self._start_time_window_monitor()

        logger.info(f"DAGComputeServer initialized with {len(self.dags)} DAG(s)")
        
        # v1.5.2: Auto-start eligible DAGs
        # Worker pool is already initialized and ready (passed from run_server.py)
        self._auto_start_eligible_dags()
        self._auto_started = True

    def add_dag(self, config_data, config_filename=None):
        """Add a new DAG from configuration
        
        v1.5.2: Uses lazy_init when worker pool is enabled to prevent
        creating duplicate subscribers in main process.
        """
        self._check_primary()

        if isinstance(config_data, str):
            config = json.loads(config_data)
        else:
            config = config_data

        dag_name = config.get('name')

        with self._lock:
            if dag_name in self.dags:
                raise ValueError(f"DAG with name {dag_name} already exists")

            # Save config via the storage abstraction if filename provided
            if config_filename:
                object_path = self._dag_object_path(config_filename)
                self._storage.write_text(object_path,
                                         json.dumps(config, indent=2))
                logger.info(f"Saved DAG configuration to storage object "
                            f"'{object_path}' (provider: {self._storage.name})")
                # v2.1.0: remember the source object name so the intraday
                # schedule-refresh loop can re-read this DAG's config.
                config['config_filename'] = config_filename

            # v1.5.2: Use lazy init when worker pool is enabled
            use_lazy_init = self._worker_pool is not None
            dag = ComputeGraph(config, lazy_init=use_lazy_init)
            self.dags[dag_name] = dag
            logger.info(f"Added DAG: {dag_name}")

            return dag_name

    def start(self, dag_name):
        """Start a specific DAG
        
        v1.5.2: If worker pool is configured, dispatches DAG to a worker.
        Otherwise, runs DAG in main process.
        """
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            # Check if already running in main process
            if dag._compute_thread and dag._compute_thread.is_alive():
                logger.warning(f"DAG {dag_name} is already running in main process")
                return
            
            # v1.5.2: Check if worker pool is available and running
            if self._worker_pool and self._worker_pool.is_running():
                # Check if already assigned to a worker
                worker_id = self._worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    logger.warning(f"DAG {dag_name} is already running on Worker {worker_id}")
                    return
                
                # Dispatch to worker pool
                dag_config = dag.config
                if 'name' not in dag_config:
                    dag_config = dict(dag_config)
                    dag_config['name'] = dag_name
                
                worker_id = self._worker_pool.load_dag(dag_config)
                self._log_dag_state_change(dag_name, "STARTED", f"Dispatched to Worker {worker_id}")
                logger.info(f"DAG '{dag_name}' dispatched to Worker {worker_id}")
            else:
                # Run in main process
                dag.start()
                self._log_dag_state_change(dag_name, "STARTED", "User/system initiated start (main process)")
    
    def set_worker_pool(self, worker_pool):
        """
        Set the worker pool for DAG execution (v1.5.2)
        
        When set, DAGs will be dispatched to worker processes instead of
        running in the main process.
        
        Args:
            worker_pool: WorkerPoolManager instance or None to disable
        """
        self._worker_pool = worker_pool
        if worker_pool:
            logger.info("DAGComputeServer: Worker pool integration enabled")
        else:
            logger.info("DAGComputeServer: Worker pool integration disabled")

    def start_all(self):
        """Start all DAGs
        
        v1.5.2: Uses integrated start() which handles worker pool dispatch
        """
        self._check_primary()

        # Don't hold lock during start() since it acquires its own lock
        dag_names = list(self.dags.keys())
        for dag_name in dag_names:
            try:
                self.start(dag_name)
                logger.info(f"Started DAG: {dag_name}")
            except Exception as e:
                logger.error(f"Error starting DAG {dag_name}: {str(e)}")

    def stop(self, dag_name):
        """Stop a specific DAG
        
        v1.5.2: Handles both main process and worker pool execution
        """
        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]
            
            # v1.5.2: Check if running in worker pool
            if self._worker_pool and self._worker_pool.is_running():
                worker_id = self._worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    # Unload from worker
                    self._worker_pool.unload_dag(dag_name)
                    self._log_dag_state_change(dag_name, "STOPPED", f"Unloaded from Worker {worker_id}")
                    return
            
            # Stop in main process
            dag.stop()
            self._log_dag_state_change(dag_name, "STOPPED", "User/system initiated stop")

    def stop_all(self):
        """Stop all DAGs
        
        v1.5.2: Uses integrated stop() which handles worker pool
        """
        # Don't hold lock during stop() since it acquires its own lock
        dag_names = list(self.dags.keys())
        for dag_name in dag_names:
            try:
                self.stop(dag_name)
            except Exception as e:
                logger.error(f"Error stopping DAG {dag_name}: {str(e)}")

    def suspend(self, dag_name):
        """Suspend a specific DAG (UI-driven)"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            if not (dag._compute_thread and dag._compute_thread.is_alive()):
                logger.warning(f"DAG {dag_name} is not running")
                return

            dag.suspend(ui_driven=True)
            self._log_dag_state_change(dag_name, "SUSPENDED", "User initiated suspend (UI-driven)")

    def resume(self, dag_name):
        """Resume a specific DAG (UI-driven)"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            if not (dag._compute_thread and dag._compute_thread.is_alive()):
                logger.warning(f"DAG {dag_name} is not running")
                return

            if dag._suspend_event.is_set():
                logger.warning(f"DAG {dag_name} is not suspended")
                return

            dag.resume(ui_driven=True)
            self._log_dag_state_change(dag_name, "RESUMED", "User initiated resume (UI-driven)")

    def delete(self, dag_name):
        """Delete a specific DAG"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            # Stop if running
            if dag._compute_thread and dag._compute_thread.is_alive():
                dag.stop()

            # Remove from dictionary
            del self.dags[dag_name]
            logger.info(f"Deleted DAG: {dag_name}")

    def details(self, dag_name):
        """Get details of a specific DAG"""
        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            return self.dags[dag_name].details()

    def list_dags(self):
        """List all DAGs with basic info
        
        v1.5.2: Checks both main process and worker pool for running status
        """
        with self._lock:
            dag_list = []

            for dag_name, dag in self.dags.items():
                # Check if running in main process
                is_running_main = dag._compute_thread and dag._compute_thread.is_alive()
                
                # v1.5.2: Check if running in worker pool
                is_running_worker = False
                worker_id = None
                if self._worker_pool and self._worker_pool.is_running():
                    worker_id = self._worker_pool.get_dag_assignment(dag_name)
                    is_running_worker = worker_id is not None
                
                is_running = is_running_main or is_running_worker
                # v2.1.0: schedule-aware activity (time window AND
                # day-of-week AND holiday calendars). The key name
                # 'in_time_window' is kept for template compatibility.
                in_time_window, schedule_reason = dag.is_within_schedule()
                from core.schedule.dag_schedule import classify_schedule_reason
                schedule_reason_kind = classify_schedule_reason(
                    schedule_reason)

                # v2.1.0 BUGFIX (completes bug #1): list_dags never emitted
                # a 'state' key, so the monitoring dashboard counters stayed
                # at zero even after _get_dag_stats was pointed at 'state'.
                # Derive it explicitly here.
                is_suspended = (not dag._suspend_event.is_set()
                                if dag._compute_thread else False)
                if is_running and is_suspended:
                    state = 'suspended'
                elif is_running:
                    state = 'running'
                else:
                    state = 'stopped'

                # Check if this is an autocloned DAG
                parent_dag = self.is_autocloned_dag(dag_name)

                # Check if this DAG has autoclone enabled
                autoclone_enabled = self._is_autoclone_enabled(dag.config)
                autoclone_status = None
                autoclone_clone_count = 0

                if autoclone_enabled and dag_name in self.autoclone_info:
                    info = self.autoclone_info[dag_name]
                    autoclone_status = info.get('status', 'idle')
                    autoclone_clone_count = len(info.get('clones', []))

                # v1.5.2: Get node count from either built nodes or config
                node_count = len(dag.nodes) if dag.nodes else len(dag.config.get('nodes', []))
                
                dag_list.append({
                    'name': dag_name,
                    'state': state,  # v2.1.0: running | suspended | stopped
                    'is_running': is_running,
                    'is_suspended': is_suspended,
                    'start_time': dag.start_time,
                    'end_time': dag.end_time,
                    'duration': dag.duration,  # Add duration field
                    'schedule': dag.schedule.to_dict()
                    if getattr(dag, 'schedule', None) else None,
                    'schedule_reason': schedule_reason,
                    'schedule_reason_kind': schedule_reason_kind,
                    'ui_override': getattr(dag, '_ui_override', False),
                    'node_count': node_count,
                    'in_time_window': in_time_window,
                    'is_autocloned': parent_dag is not None,
                    'parent_dag': parent_dag,
                    'autoclone_enabled': autoclone_enabled,
                    'autoclone_status': autoclone_status,
                    'autoclone_clone_count': autoclone_clone_count,
                    # v1.5.2: Worker pool info
                    'worker_id': worker_id,
                    'execution_mode': 'worker' if is_running_worker else ('main' if is_running_main else None)
                })
            return dag_list

    # Modified clone_dag method for dag_server.py
    # Replace the existing clone_dag method (around line 300-314) with this code

    def clone_dag(self, dag_name, start_time=None, duration=None):
        """
        Clone a DAG with optional time window using duration.

        Args:
            dag_name: Name of DAG to clone
            start_time: New start time in HHMM format, or None
            duration: New duration string (e.g., "1h", "30m", "1h30m"), or None

        Returns:
            str: Name of cloned DAG

        Behavior:
            - If both start_time and duration are None: Perpetual running (no time window)
            - If start_time provided but duration is None: Use default duration (-5 minutes)
            - If both provided: Use specified time window
        """
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            original_dag = self.dags[dag_name]

            # Clone with duration-based time window
            cloned_dag = original_dag.clone(start_time, duration)

            self.dags[cloned_dag.name] = cloned_dag
            logger.info(f"Cloned DAG {dag_name} to {cloned_dag.name}")

            if start_time and duration:
                logger.info(f"  Cloned with: start_time={start_time}, duration={duration}")
            elif start_time:
                logger.info(f"  Cloned with: start_time={start_time} (default duration: -5 minutes)")
            else:
                logger.info(f"  Cloned with: perpetual running (no time window)")

            return cloned_dag.name

    def get_server_status(self):
        """Get server status"""
        return {
            'is_primary': self.is_primary,
            'dag_count': len(self.dags),
            'dags': self.list_dags()
        }

    def shutdown(self):
        """Shutdown the server"""
        logger.info("Shutting down DishtaYantra DAG Server")

        # Stop autoclone manager
        self._autoclone_stop_event.set()
        if self._autoclone_thread and self._autoclone_thread.is_alive():
            self._autoclone_thread.join(timeout=5)
            logger.info("AutoClone manager thread stopped")

        self.stop_all()

        if self.ha_provider:
            try:
                self.ha_provider.stop()
            except Exception as e:
                logger.error(f"Error stopping HA provider: {str(e)}")
                logger.error(traceback.format_exc())

        logger.info("DishtaYantra DAG Server shutdown complete")