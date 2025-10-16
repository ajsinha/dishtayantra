import json
import logging
import os
import threading
import time
from datetime import datetime
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
from core.dag.compute_graph import ComputeGraph

logger = logging.getLogger(__name__)


class DAGComputeServer:
    """Server for managing multiple DAG compute graphs"""

    def __init__(self, dag_config_folder, zookeeper_hosts='localhost:2181'):
        self.dag_config_folder = dag_config_folder
        self.dags = {}
        self._lock = threading.Lock()

        # Zookeeper setup
        self.zk_client = KazooClient(hosts=zookeeper_hosts)
        self.is_primary = False
        self.election = None

        # Time window monitor
        self._monitor_thread = None
        self._monitor_stop_event = threading.Event()
        self._monitor_interval = 60  # Check every 60 seconds

        self._load_dags()
        self._setup_leader_election()

        # Start monitor after everything is initialized
        self._start_time_window_monitor()

        logger.info("DAGComputeServer initialized with time window monitor")

    def _load_dags(self):
        """Load all DAG configurations from folder"""
        # Create folder if it doesn't exist
        if not os.path.exists(self.dag_config_folder):
            logger.warning(f"DAG config folder {self.dag_config_folder} does not exist, creating it...")
            try:
                os.makedirs(self.dag_config_folder)
                logger.info(f"Created DAG config folder: {self.dag_config_folder}")
            except Exception as e:
                logger.error(f"Error creating DAG config folder: {str(e)}")
                return

        # List files in directory
        files = os.listdir(self.dag_config_folder)
        json_files = [f for f in files if f.endswith('.json')]

        if not json_files:
            logger.warning(f"No JSON configuration files found in {self.dag_config_folder}")
            logger.info("You can add DAG configurations via the web UI or place JSON files in the config folder")
            return

        logger.info(f"Found {len(json_files)} DAG configuration file(s)")

        for filename in json_files:
            filepath = os.path.join(self.dag_config_folder, filename)
            try:
                logger.info(f"Loading DAG from: {filepath}")
                dag = ComputeGraph(filepath)
                with self._lock:
                    self.dags[dag.name] = dag
                logger.info(f"Successfully loaded DAG: {dag.name}")
            except Exception as e:
                logger.error(f"Error loading DAG from {filepath}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

    def _setup_leader_election(self):
        """Setup Zookeeper leader election"""
        try:
            self.zk_client.start()
            self.election = Election(self.zk_client, "/dag_server_election")
            self.election.run(self._on_elected)
        except Exception as e:
            logger.error(f"Error setting up leader election: {str(e)}")
            # If Zookeeper is not available, assume primary
            self.is_primary = True
            logger.warning("Running without Zookeeper - assuming primary role")

    def _on_elected(self):
        """Callback when elected as leader"""
        logger.info("Elected as primary DAG server")
        self.is_primary = True

        # Resume all suspended DAGs
        with self._lock:
            for dag in self.dags.values():
                if dag._suspend_event.is_set():
                    try:
                        dag.resume()
                    except:
                        pass

    def _start_time_window_monitor(self):
        """Start the time window monitoring thread"""
        if not self._monitor_thread or not self._monitor_thread.is_alive():
            self._monitor_stop_event.clear()
            self._monitor_thread = threading.Thread(target=self._time_window_monitor_loop, daemon=True)
            self._monitor_thread.start()
            logger.info("Time window monitor started")

    def _time_window_monitor_loop(self):
        """Monitor loop to check DAG time windows"""
        logger.info("Time window monitor loop started")

        while not self._monitor_stop_event.is_set():
            try:
                # Get current time in HHMM format
                now = datetime.now()
                current_time = now.strftime('%H%M')
                current_time_int = int(current_time)

                logger.debug(f"Time window check - Current time: {current_time} ({now.strftime('%I:%M %p')})")

                with self._lock:
                    for dag_name, dag in self.dags.items():
                        # Skip if DAG has no time window
                        if not dag.start_time or not dag.end_time:
                            continue

                        # Convert times to integers for proper comparison
                        start_time_int = int(dag.start_time)
                        end_time_int = int(dag.end_time)

                        is_running = dag._compute_thread and dag._compute_thread.is_alive()
                        in_time_window = start_time_int <= current_time_int <= end_time_int

                        logger.debug(
                            f"DAG {dag_name}: start={start_time_int}, end={end_time_int}, current={current_time_int}, in_window={in_time_window}, running={is_running}")

                        # Start DAG if in window and not running
                        if in_time_window and not is_running and self.is_primary:
                            try:
                                logger.info(
                                    f"Time window opened for DAG {dag_name}, starting... (current: {current_time}, window: {dag.start_time}-{dag.end_time})")
                                dag.start()
                            except Exception as e:
                                logger.error(f"Error starting DAG {dag_name}: {str(e)}")

                        # Stop DAG if outside window and running
                        elif not in_time_window and is_running:
                            try:
                                logger.info(
                                    f"Time window closed for DAG {dag_name}, stopping... (current: {current_time}, window: {dag.start_time}-{dag.end_time})")
                                dag.stop()
                            except Exception as e:
                                logger.error(f"Error stopping DAG {dag_name}: {str(e)}")

            except Exception as e:
                logger.error(f"Error in time window monitor: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

            # Sleep for configured interval
            time.sleep(self._monitor_interval)

    def _check_primary(self):
        """Check if this instance is primary"""
        if not self.is_primary:
            raise PermissionError("This instance is not primary. Operation not allowed.")

    def add_dag(self, config_data, config_filename=None):
        """Add a new DAG from configuration"""
        self._check_primary()

        if isinstance(config_data, str):
            config = json.loads(config_data)
        else:
            config = config_data

        dag_name = config.get('name')

        with self._lock:
            if dag_name in self.dags:
                raise ValueError(f"DAG with name {dag_name} already exists")

            # Save config to file if filename provided
            if config_filename:
                filepath = os.path.join(self.dag_config_folder, config_filename)
                with open(filepath, 'w') as f:
                    json.dump(config, f, indent=2)

            dag = ComputeGraph(config)
            self.dags[dag_name] = dag
            logger.info(f"Added DAG: {dag_name}")

            return dag_name

    def start(self, dag_name):
        """Start a specific DAG"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            if dag._compute_thread and dag._compute_thread.is_alive():
                logger.warning(f"DAG {dag_name} is already running")
                return

            dag.start()
            logger.info(f"Started DAG: {dag_name}")

    def start_all(self):
        """Start all DAGs"""
        self._check_primary()

        with self._lock:
            for dag_name, dag in self.dags.items():
                try:
                    if not (dag._compute_thread and dag._compute_thread.is_alive()):
                        dag.start()
                        logger.info(f"Started DAG: {dag_name}")
                except Exception as e:
                    logger.error(f"Error starting DAG {dag_name}: {str(e)}")

    def stop(self, dag_name):
        """Stop a specific DAG"""
        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]
            dag.stop()
            logger.info(f"Stopped DAG: {dag_name}")

    def stop_all(self):
        """Stop all DAGs"""
        with self._lock:
            for dag_name, dag in self.dags.items():
                try:
                    dag.stop()
                    logger.info(f"Stopped DAG: {dag_name}")
                except Exception as e:
                    logger.error(f"Error stopping DAG {dag_name}: {str(e)}")

    def suspend(self, dag_name):
        """Suspend a specific DAG"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            if not (dag._compute_thread and dag._compute_thread.is_alive()):
                logger.warning(f"DAG {dag_name} is not running")
                return

            dag.suspend()
            logger.info(f"Suspended DAG: {dag_name}")

    def resume(self, dag_name):
        """Resume a specific DAG"""
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

            dag.resume()
            logger.info(f"Resumed DAG: {dag_name}")

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
        """List all DAGs with basic info"""
        with self._lock:
            dag_list = []
            now = datetime.now()
            current_time = now.strftime('%H%M')
            current_time_int = int(current_time)

            for dag_name, dag in self.dags.items():
                is_running = dag._compute_thread and dag._compute_thread.is_alive()
                in_time_window = True

                if dag.start_time and dag.end_time:
                    # Convert to integers for proper comparison
                    start_time_int = int(dag.start_time)
                    end_time_int = int(dag.end_time)
                    in_time_window = start_time_int <= current_time_int <= end_time_int

                dag_list.append({
                    'name': dag_name,
                    'is_running': is_running,
                    'is_suspended': not dag._suspend_event.is_set() if dag._compute_thread else False,
                    'start_time': dag.start_time,
                    'end_time': dag.end_time,
                    'node_count': len(dag.nodes),
                    'in_time_window': in_time_window
                })
            return dag_list

    def clone_dag(self, dag_name, start_time=None, end_time=None):
        """Clone a DAG with optional time window"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            original_dag = self.dags[dag_name]
            cloned_dag = original_dag.clone(start_time, end_time)

            self.dags[cloned_dag.name] = cloned_dag
            logger.info(f"Cloned DAG {dag_name} to {cloned_dag.name}")

            return cloned_dag.name

    def get_server_status(self):
        """Get server status"""
        return {
            'is_primary': self.is_primary,
            'dag_count': len(self.dags),
            'dags': self.list_dags(),
            'monitor_running': self._monitor_thread and self._monitor_thread.is_alive()
        }

    def shutdown(self):
        """Shutdown the server"""
        logger.info("Shutting down DAG compute server")

        # Stop time window monitor
        self._monitor_stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

        self.stop_all()

        if self.zk_client:
            self.zk_client.stop()
            self.zk_client.close()

        logger.info("DAG compute server shutdown complete")

    def _load_dags(self):
        """Load all DAG configurations from folder"""
        # Create folder if it doesn't exist
        if not os.path.exists(self.dag_config_folder):
            logger.warning(f"DAG config folder {self.dag_config_folder} does not exist, creating it...")
            try:
                os.makedirs(self.dag_config_folder)
                logger.info(f"Created DAG config folder: {self.dag_config_folder}")
            except Exception as e:
                logger.error(f"Error creating DAG config folder: {str(e)}")
                return

        # List files in directory
        files = os.listdir(self.dag_config_folder)
        json_files = [f for f in files if f.endswith('.json')]

        if not json_files:
            logger.warning(f"No JSON configuration files found in {self.dag_config_folder}")
            logger.info("You can add DAG configurations via the web UI or place JSON files in the config folder")
            return

        logger.info(f"Found {len(json_files)} DAG configuration file(s)")

        for filename in json_files:
            filepath = os.path.join(self.dag_config_folder, filename)
            try:
                logger.info(f"Loading DAG from: {filepath}")
                dag = ComputeGraph(filepath)
                with self._lock:
                    self.dags[dag.name] = dag
                logger.info(f"Successfully loaded DAG: {dag.name}")
            except Exception as e:
                logger.error(f"Error loading DAG from {filepath}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

    def _setup_leader_election(self):
        """Setup Zookeeper leader election"""
        try:
            self.zk_client.start()
            self.election = Election(self.zk_client, "/dag_server_election")
            self.election.run(self._on_elected)
        except Exception as e:
            logger.error(f"Error setting up leader election: {str(e)}")
            # If Zookeeper is not available, assume primary
            self.is_primary = True
            logger.warning("Running without Zookeeper - assuming primary role")

    def _on_elected(self):
        """Callback when elected as leader"""
        logger.info("Elected as primary DAG server")
        self.is_primary = True

        # Resume all suspended DAGs
        with self._lock:
            for dag in self.dags.values():
                if dag._suspend_event.is_set():
                    try:
                        dag.resume()
                    except:
                        pass

    def _check_primary(self):
        """Check if this instance is primary"""
        if not self.is_primary:
            raise PermissionError("This instance is not primary. Operation not allowed.")

    def add_dag(self, config_data, config_filename=None):
        """Add a new DAG from configuration"""
        self._check_primary()

        if isinstance(config_data, str):
            config = json.loads(config_data)
        else:
            config = config_data

        dag_name = config.get('name')

        with self._lock:
            if dag_name in self.dags:
                raise ValueError(f"DAG with name {dag_name} already exists")

            # Save config to file if filename provided
            if config_filename:
                filepath = os.path.join(self.dag_config_folder, config_filename)
                with open(filepath, 'w') as f:
                    json.dump(config, f, indent=2)

            dag = ComputeGraph(config)
            self.dags[dag_name] = dag
            logger.info(f"Added DAG: {dag_name}")

            return dag_name

    def start(self, dag_name):
        """Start a specific DAG"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            if dag._compute_thread and dag._compute_thread.is_alive():
                logger.warning(f"DAG {dag_name} is already running")
                return

            dag.start()
            logger.info(f"Started DAG: {dag_name}")

    def start_all(self):
        """Start all DAGs"""
        self._check_primary()

        with self._lock:
            for dag_name, dag in self.dags.items():
                try:
                    if not (dag._compute_thread and dag._compute_thread.is_alive()):
                        dag.start()
                        logger.info(f"Started DAG: {dag_name}")
                except Exception as e:
                    logger.error(f"Error starting DAG {dag_name}: {str(e)}")

    def stop(self, dag_name):
        """Stop a specific DAG"""
        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]
            dag.stop()
            logger.info(f"Stopped DAG: {dag_name}")

    def stop_all(self):
        """Stop all DAGs"""
        with self._lock:
            for dag_name, dag in self.dags.items():
                try:
                    dag.stop()
                    logger.info(f"Stopped DAG: {dag_name}")
                except Exception as e:
                    logger.error(f"Error stopping DAG {dag_name}: {str(e)}")

    def suspend(self, dag_name):
        """Suspend a specific DAG"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            dag = self.dags[dag_name]

            if not (dag._compute_thread and dag._compute_thread.is_alive()):
                logger.warning(f"DAG {dag_name} is not running")
                return

            dag.suspend()
            logger.info(f"Suspended DAG: {dag_name}")

    def resume(self, dag_name):
        """Resume a specific DAG"""
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

            dag.resume()
            logger.info(f"Resumed DAG: {dag_name}")

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
        """List all DAGs with basic info"""
        with self._lock:
            dag_list = []
            for dag_name, dag in self.dags.items():
                dag_list.append({
                    'name': dag_name,
                    'is_running': dag._compute_thread and dag._compute_thread.is_alive(),
                    'is_suspended': not dag._suspend_event.is_set() if dag._compute_thread else False,
                    'start_time': dag.start_time,
                    'end_time': dag.end_time,
                    'node_count': len(dag.nodes)
                })
            return dag_list

    def clone_dag(self, dag_name, start_time=None, end_time=None):
        """Clone a DAG with optional time window"""
        self._check_primary()

        with self._lock:
            if dag_name not in self.dags:
                raise ValueError(f"DAG {dag_name} not found")

            original_dag = self.dags[dag_name]
            cloned_dag = original_dag.clone(start_time, end_time)

            self.dags[cloned_dag.name] = cloned_dag
            logger.info(f"Cloned DAG {dag_name} to {cloned_dag.name}")

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
        logger.info("Shutting down DAG compute server")
        self.stop_all()

        if self.zk_client:
            self.zk_client.stop()
            self.zk_client.close()