import json
import logging
import os
import threading
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
        self.zk_client = None
        self.is_primary = True  # Default to primary
        self.election = None

        logger.info("Initializing DAGComputeServer...")
        self._load_dags()

        # Setup Zookeeper in background
        try:
            zk_thread = threading.Thread(
                target=self._setup_leader_election_async,
                args=(zookeeper_hosts,),
                daemon=True
            )
            zk_thread.start()
        except Exception as e:
            logger.error(f"Error starting Zookeeper thread: {str(e)}")

        logger.info(f"DAGComputeServer initialized with {len(self.dags)} DAG(s)")

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
                raise

    def _setup_leader_election_async(self, zookeeper_hosts):
        """Setup Zookeeper leader election asynchronously"""
        try:
            logger.info(f"Connecting to Zookeeper at {zookeeper_hosts}...")
            self.zk_client = KazooClient(hosts=zookeeper_hosts)
            self.zk_client.start(timeout=5)

            logger.info("Connected to Zookeeper, starting election...")
            self.election = Election(self.zk_client, "/dag_server_election")
            self.election.run(self._on_elected)

        except Exception as e:
            logger.warning(f"Zookeeper not available: {str(e)}")
            logger.info("Continuing as PRIMARY without Zookeeper")
            self.is_primary = True

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
                is_running = dag._compute_thread and dag._compute_thread.is_alive()
                in_time_window = dag.is_in_time_window()

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
            'dags': self.list_dags()
        }

    def shutdown(self):
        """Shutdown the server"""
        logger.info("Shutting down DAG compute server")
        self.stop_all()

        if self.zk_client:
            self.zk_client.stop()
            self.zk_client.close()

        logger.info("DAG compute server shutdown complete")