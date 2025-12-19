import json
import logging
import os
import threading
from datetime import datetime
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
from core.dag.compute_graph import ComputeGraph
from core.dag.time_window_utils import parse_duration, calculate_end_time

logger = logging.getLogger(__name__)


class DAGComputeServer:
    """Server for managing multiple DAG compute graphs"""

    def __init__(self, dag_config_folder, zookeeper_hosts='localhost:2181'):
        self.dag_config_folder = dag_config_folder
        self.dags = {}
        self._lock = threading.Lock()

        # AutoClone tracking
        self.autoclone_info = {}  # dag_name -> {clones: [clone_names], config: {}, status: ''}
        self._autoclone_thread = None
        self._autoclone_stop_event = threading.Event()

        # Time window monitor - checks DAGs outside window and starts them when they enter
        self._time_window_monitor_thread = None
        self._time_window_stop_event = threading.Event()

        # Zookeeper setup
        self.zk_client = None
        self.is_primary = True  # Default to primary
        self.election = None

        logger.info("Initializing DAGComputeServer...")
        self._load_dags()

        # Start autoclone management thread
        self._start_autoclone_manager()

        # Start time window monitor thread
        self._start_time_window_monitor()

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
        
        # Auto-start eligible DAGs (perpetual or within time window)
        self._auto_start_eligible_dags()

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

    def _auto_start_eligible_dags(self):
        """
        Auto-start DAGs that should be running on system startup.
        
        A DAG should be auto-started if:
        1. It has no time window (perpetual/always active), OR
        2. Current time is within its configured time window
        
        DAGs are started one by one with a 5 second pause between each.
        """
        import time
        
        logger.info("=" * 70)
        logger.info("AUTO-START: Checking which DAGs should be started on system startup")
        logger.info("=" * 70)
        
        dags_to_start = []
        
        with self._lock:
            for dag_name, dag in self.dags.items():
                # Check if DAG is perpetual (no time window)
                is_perpetual = (dag.start_time is None or dag.end_time is None)
                
                if is_perpetual:
                    dags_to_start.append((dag_name, 'PERPETUAL (no time window)'))
                    logger.info(f"AUTO-START: DAG '{dag_name}' is PERPETUAL - will be started")
                elif dag.is_in_time_window():
                    dags_to_start.append((dag_name, f'WITHIN TIME WINDOW ({dag.start_time}-{dag.end_time})'))
                    logger.info(f"AUTO-START: DAG '{dag_name}' is WITHIN TIME WINDOW ({dag.start_time}-{dag.end_time}) - will be started")
                else:
                    logger.info(f"AUTO-START: DAG '{dag_name}' is OUTSIDE time window ({dag.start_time}-{dag.end_time}) - will NOT be started")
        
        if not dags_to_start:
            logger.info("AUTO-START: No DAGs eligible for auto-start at this time")
            logger.info("=" * 70)
            return
        
        logger.info(f"AUTO-START: {len(dags_to_start)} DAG(s) will be started with 5 second intervals")
        logger.info("-" * 70)
        
        for i, (dag_name, reason) in enumerate(dags_to_start):
            try:
                logger.info("")
                logger.info("*" * 70)
                logger.info(f"*  AUTO-START: STARTING DAG '{dag_name}'")
                logger.info(f"*  Reason: {reason}")
                logger.info(f"*  Progress: {i + 1} of {len(dags_to_start)}")
                logger.info("*" * 70)
                
                self.start(dag_name)
                
                logger.info(f"AUTO-START: Successfully started DAG '{dag_name}'")
                
                # Pause between starts (except after the last one)
                if i < len(dags_to_start) - 1:
                    logger.info(f"AUTO-START: Pausing 5 seconds before starting next DAG...")
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"AUTO-START: Failed to start DAG '{dag_name}': {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"AUTO-START: Completed - {len(dags_to_start)} DAG(s) processed")
        logger.info("=" * 70)

    def _start_time_window_monitor(self):
        """Start the time window monitor thread to check DAGs outside their window"""
        if self._time_window_monitor_thread and self._time_window_monitor_thread.is_alive():
            return

        self._time_window_stop_event.clear()
        self._time_window_monitor_thread = threading.Thread(
            target=self._time_window_monitor_loop,
            daemon=True
        )
        self._time_window_monitor_thread.start()
        logger.info("Time window monitor thread started (checks every 60 seconds)")

    def _time_window_monitor_loop(self):
        """
        Monitor DAGs that are outside their time window and start them when they enter.
        
        This runs every 60 seconds and checks:
        1. DAGs that have a time window configured
        2. Are not currently running
        3. Have now entered their time window
        """
        import time as time_module
        
        logger.info("Time window monitor: Starting monitoring loop")
        
        while not self._time_window_stop_event.is_set():
            try:
                with self._lock:
                    for dag_name, dag in self.dags.items():
                        # Skip if no time window configured (perpetual DAGs)
                        if dag.start_time is None or dag.end_time is None:
                            continue
                        
                        # Skip if already running
                        if dag._compute_thread and dag._compute_thread.is_alive():
                            continue
                        
                        # Check if now within time window
                        if dag.is_in_time_window():
                            logger.info("")
                            logger.info("!" * 70)
                            logger.info("!  TIME WINDOW MONITOR: DAG ENTERING TIME WINDOW")
                            logger.info(f"!  DAG: {dag_name}")
                            logger.info(f"!  Window: {dag.start_time} - {dag.end_time}")
                            logger.info("!  ACTION: Starting DAG automatically")
                            logger.info("!" * 70)
                            
                            try:
                                dag.start()
                                self._log_dag_state_change(dag_name, "STARTED", 
                                    f"Entered time window ({dag.start_time}-{dag.end_time})")
                            except Exception as e:
                                logger.error(f"Time window monitor: Failed to start DAG '{dag_name}': {e}")
                        else:
                            logger.debug(f"Time window monitor: DAG '{dag_name}' still outside window ({dag.start_time}-{dag.end_time})")
                            
            except Exception as e:
                logger.error(f"Time window monitor error: {e}")
            
            # Sleep for 60 seconds before next check
            self._time_window_stop_event.wait(60)
        
        logger.info("Time window monitor: Stopped")

    def _log_dag_state_change(self, dag_name, state, reason=None):
        """Log DAG state changes with prominent formatting"""
        logger.info("")
        logger.info("█" * 70)
        logger.info(f"█  DAG STATE CHANGE: {state}")
        logger.info(f"█  DAG Name: {dag_name}")
        if reason:
            logger.info(f"█  Reason: {reason}")
        logger.info(f"█  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("█" * 70)
        logger.info("")

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
            self._log_dag_state_change(dag_name, "STARTED", "User/system initiated start")

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
            self._log_dag_state_change(dag_name, "STOPPED", "User/system initiated stop")

    def stop_all(self):
        """Stop all DAGs"""
        with self._lock:
            for dag_name, dag in self.dags.items():
                try:
                    dag.stop()
                    self._log_dag_state_change(dag_name, "STOPPED", "Stop all DAGs command")
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
        """List all DAGs with basic info"""
        with self._lock:
            dag_list = []

            for dag_name, dag in self.dags.items():
                is_running = dag._compute_thread and dag._compute_thread.is_alive()
                in_time_window = dag.is_in_time_window()

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

                dag_list.append({
                    'name': dag_name,
                    'is_running': is_running,
                    'is_suspended': not dag._suspend_event.is_set() if dag._compute_thread else False,
                    'start_time': dag.start_time,
                    'end_time': dag.end_time,
                    'duration': dag.duration,  # Add duration field
                    'node_count': len(dag.nodes),
                    'in_time_window': in_time_window,
                    'is_autocloned': parent_dag is not None,
                    'parent_dag': parent_dag,
                    'autoclone_enabled': autoclone_enabled,
                    'autoclone_status': autoclone_status,
                    'autoclone_clone_count': autoclone_clone_count
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

    def _is_autoclone_enabled(self, dag_config):
        """Check if autoclone is enabled for a DAG configuration

        New format (preferred):
            "autoclone": {
                "ramp_up_time": "1255",
                "duration": "1h30m",  # Optional, defaults to "8h"
                "ramp_count": 10
            }

        Legacy format (still supported):
            "autoclone": {
                "ramp_up_time": "1255",
                "ramp_down_time": "1310",
                "ramp_count": 10
            }
        """
        if 'autoclone' not in dag_config:
            return False

        autoclone = dag_config['autoclone']

        # Check required keys
        if 'ramp_up_time' not in autoclone or not autoclone['ramp_up_time']:
            return False

        if 'ramp_count' not in autoclone or not autoclone['ramp_count']:
            return False

        # Must have either duration (new) or ramp_down_time (legacy)
        has_duration = 'duration' in autoclone and autoclone['duration']
        has_ramp_down = 'ramp_down_time' in autoclone and autoclone['ramp_down_time']

        if not has_duration and not has_ramp_down:
            # Neither provided - use default duration of 8h
            # This is valid, we'll use default
            pass

        try:
            # Validate ramp_count is a positive integer
            if int(autoclone['ramp_count']) <= 0:
                return False

            # Validate duration if provided (new format)
            if has_duration:
                duration_minutes = parse_duration(autoclone['duration'])
                if duration_minutes is None or duration_minutes <= 0:
                    logger.warning(f"Invalid autoclone duration: {autoclone['duration']}")
                    return False

            return True
        except (ValueError, TypeError):
            return False

    def _start_autoclone_manager(self):
        """Start the autoclone management thread"""
        self._autoclone_thread = threading.Thread(
            target=self._autoclone_manager_loop,
            daemon=True,
            name="AutoCloneManager"
        )
        self._autoclone_thread.start()
        logger.info("AutoClone manager thread started")

    def _autoclone_manager_loop(self):
        """Background thread that manages autoclone operations"""
        from datetime import datetime
        import time

        logger.info("AutoClone manager loop started")

        while not self._autoclone_stop_event.is_set():
            try:
                # Run every 10 seconds
                self._autoclone_stop_event.wait(10)

                if not self.is_primary:
                    continue

                current_time = datetime.now().strftime('%H%M')

                # Collect work to do without holding the lock for long
                dags_to_process = []
                clones_to_delete = []
                dags_to_cleanup = []

                with self._lock:
                    # Quickly scan all DAGs to determine what work needs to be done
                    for dag_name, dag in list(self.dags.items()):
                        # Skip if this is an autocloned DAG itself
                        if dag_name in [clone for info in self.autoclone_info.values() for clone in
                                        info.get('clones', [])]:
                            continue

                        if not self._is_autoclone_enabled(dag.config):
                            # Clean up any existing autoclone info if config was removed
                            if dag_name in self.autoclone_info:
                                dags_to_cleanup.append(dag_name)
                            continue

                        autoclone_config = dag.config['autoclone']
                        ramp_up_time = autoclone_config['ramp_up_time']

                        # NEW: Support duration instead of ramp_down_time
                        # Calculate ramp_down_time from ramp_up_time + duration
                        if 'duration' in autoclone_config and autoclone_config['duration']:
                            # New format with duration
                            duration = autoclone_config['duration']
                            ramp_down_time = calculate_end_time(ramp_up_time, duration)
                            logger.debug(
                                f"AutoClone {dag_name}: Using duration {duration}, ramp_down_time={ramp_down_time}")
                        elif 'ramp_down_time' in autoclone_config and autoclone_config['ramp_down_time']:
                            # Legacy format with explicit ramp_down_time
                            ramp_down_time = autoclone_config['ramp_down_time']
                            logger.debug(f"AutoClone {dag_name}: Using legacy ramp_down_time={ramp_down_time}")
                        else:
                            # No duration or ramp_down_time provided - use default 8h
                            ramp_down_time = calculate_end_time(ramp_up_time, "8h")
                            logger.debug(
                                f"AutoClone {dag_name}: No duration/ramp_down_time, using default 8h, ramp_down_time={ramp_down_time}")

                        ramp_count = int(autoclone_config['ramp_count'])

                        # Initialize autoclone info if not exists
                        if dag_name not in self.autoclone_info:
                            self.autoclone_info[dag_name] = {
                                'clones': [],
                                'config': autoclone_config,
                                'status': 'idle',
                                'last_clone_time': None,
                                'ramp_up_completed': False,
                                'ramp_down_started': False
                            }

                        info = self.autoclone_info[dag_name]

                        # RAMP UP: Determine if we should create a clone
                        if current_time >= ramp_up_time and current_time < ramp_down_time:
                            if not info['ramp_up_completed'] and len(info['clones']) < ramp_count:
                                # Check if we should create a new clone (1 minute interval)
                                should_create = False

                                if info['last_clone_time'] is None:
                                    should_create = True
                                else:
                                    time_since_last = (datetime.now() - info['last_clone_time']).total_seconds()
                                    if time_since_last >= 60:  # 1 minute
                                        should_create = True

                                if should_create:
                                    dags_to_process.append((dag_name, ramp_count, 'create'))

                        # RAMP DOWN: Determine if we should delete a clone
                        elif current_time >= ramp_down_time or current_time < ramp_up_time:
                            if info['clones'] and not info['ramp_down_started']:
                                info['ramp_down_started'] = True
                                info['status'] = 'ramping_down'
                                logger.info(f"AutoClone: Starting ramp down for {dag_name}")

                            if info['clones']:
                                # Stop and delete one clone per minute
                                should_delete = False

                                if info['last_clone_time'] is None:
                                    should_delete = True
                                else:
                                    time_since_last = (datetime.now() - info['last_clone_time']).total_seconds()
                                    if time_since_last >= 60:  # 1 minute
                                        should_delete = True

                                if should_delete:
                                    # Just mark for deletion, we'll do it outside the lock
                                    clone_name = info['clones'][0]  # Peek at first clone
                                    clones_to_delete.append((dag_name, clone_name))

                # Now do the actual work outside the lock

                # Handle cleanup
                for dag_name in dags_to_cleanup:
                    logger.info(f"AutoClone disabled for {dag_name}, cleaning up")
                    self._cleanup_autoclones(dag_name)

                # Handle creating clones
                for dag_name, ramp_count, action in dags_to_process:
                    try:
                        # Create clone with no time window (always active)
                        clone_name = self._create_autoclone_unlocked(dag_name)

                        # Update info with lock
                        with self._lock:
                            if dag_name in self.autoclone_info:
                                info = self.autoclone_info[dag_name]
                                info['clones'].append(clone_name)
                                info['last_clone_time'] = datetime.now()
                                info['status'] = f'ramping_up ({len(info["clones"])}/{ramp_count})'

                                logger.info(
                                    f"AutoClone: Created {clone_name} for {dag_name} ({len(info['clones'])}/{ramp_count})")

                                if len(info['clones']) >= ramp_count:
                                    info['ramp_up_completed'] = True
                                    info['status'] = 'active'
                                    logger.info(f"AutoClone: Ramp up completed for {dag_name}")

                        # Start the clone (outside lock)
                        self.start(clone_name)

                    except Exception as e:
                        logger.error(f"AutoClone: Error creating clone for {dag_name}: {str(e)}")

                # Handle deleting clones
                for dag_name, clone_name in clones_to_delete:
                    try:
                        logger.info(f"AutoClone: Stopping and deleting {clone_name}")

                        # Stop the clone (outside lock)
                        with self._lock:
                            if clone_name in self.dags:
                                exists = True
                            else:
                                exists = False

                        if exists:
                            self.stop(clone_name)
                            # Wait a bit for it to stop
                            time.sleep(2)
                            # Delete it
                            self.delete(clone_name)

                        # Update info with lock
                        with self._lock:
                            if dag_name in self.autoclone_info:
                                info = self.autoclone_info[dag_name]
                                # Remove from list
                                if clone_name in info['clones']:
                                    info['clones'].remove(clone_name)

                                info['last_clone_time'] = datetime.now()
                                info['status'] = f'ramping_down ({len(info["clones"])} remaining)'

                                if not info['clones']:
                                    info['ramp_up_completed'] = False
                                    info['ramp_down_started'] = False
                                    info['status'] = 'idle'
                                    logger.info(f"AutoClone: Ramp down completed for {dag_name}")

                    except Exception as e:
                        logger.error(f"AutoClone: Error deleting clone: {str(e)}")

            except Exception as e:
                logger.error(f"Error in autoclone manager loop: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

    def _create_autoclone(self, parent_dag_name):
        """Create an autoclone of a DAG with no time window"""
        from datetime import datetime

        if parent_dag_name not in self.dags:
            raise ValueError(f"Parent DAG {parent_dag_name} not found")

        parent_dag = self.dags[parent_dag_name]

        # Clone with empty time window (None, None makes it always active)
        cloned_dag = parent_dag.clone(None, None)

        # Add to dags dictionary
        self.dags[cloned_dag.name] = cloned_dag

        return cloned_dag.name

    def _create_autoclone_unlocked(self, parent_dag_name):
        """Create an autoclone of a DAG without holding the main lock for the entire operation"""
        from datetime import datetime

        # Get parent DAG with lock
        with self._lock:
            if parent_dag_name not in self.dags:
                raise ValueError(f"Parent DAG {parent_dag_name} not found")
            parent_dag = self.dags[parent_dag_name]

        # Clone without lock (this can take time)
        cloned_dag = parent_dag.clone(None, None)

        # Add to dags dictionary with lock
        with self._lock:
            self.dags[cloned_dag.name] = cloned_dag

        return cloned_dag.name

    def _cleanup_autoclones(self, dag_name):
        """Clean up all autoclones for a DAG"""
        # Get list of clones to delete
        with self._lock:
            if dag_name not in self.autoclone_info:
                return

            info = self.autoclone_info[dag_name]
            clones_to_delete = list(info['clones'])

        # Stop and delete clones outside the lock
        for clone_name in clones_to_delete:
            try:
                if clone_name in self.dags:
                    self.stop(clone_name)
                    self.delete(clone_name)
                    logger.info(f"AutoClone: Cleaned up {clone_name}")
            except Exception as e:
                logger.error(f"AutoClone: Error cleaning up {clone_name}: {str(e)}")

        # Remove the autoclone info
        with self._lock:
            if dag_name in self.autoclone_info:
                del self.autoclone_info[dag_name]

    def get_autoclone_status(self, dag_name):
        """Get autoclone status for a DAG"""
        if dag_name not in self.autoclone_info:
            return None

        return self.autoclone_info[dag_name]

    def is_autocloned_dag(self, dag_name):
        """Check if a DAG is an autocloned instance"""
        for parent_name, info in self.autoclone_info.items():
            if dag_name in info.get('clones', []):
                return parent_name
        return None

    def shutdown(self):
        """Shutdown the server"""
        logger.info("Shutting down DishtaYantra DAG Server")

        # Stop autoclone manager
        self._autoclone_stop_event.set()
        if self._autoclone_thread and self._autoclone_thread.is_alive():
            self._autoclone_thread.join(timeout=5)
            logger.info("AutoClone manager thread stopped")

        self.stop_all()

        if self.zk_client:
            self.zk_client.stop()
            self.zk_client.close()

        logger.info("DishtaYantra DAG Server shutdown complete")