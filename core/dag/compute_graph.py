import json
import logging
import threading
import time
from datetime import datetime
from collections import deque, defaultdict
from pathlib import Path
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
from core.calculator.core_calculator import *
from core.transformer.core_transformer import *
from core.dag.graph_elements import Node, Edge
from core.dag.node_implementations import *
from core.dag.subgraph import (
    Subgraph, SubgraphNode, SubgraphSupervisor, SubgraphState,
    ExecutionMode, load_subgraph_from_config, SubgraphConfigError
)
from core.core_utils import instantiate_module
from core.dag.time_window_utils import calculate_end_time, get_time_window_info, is_within_time_window
from core.schedule.dag_schedule import DagSchedule, is_schedule_active
from core.properties_configurator import PropertiesConfigurator
logger = logging.getLogger(__name__)


from core.dag.compute_graph_builders import ComponentBuilderMixin
from core.dag.compute_graph_support import (
    GraphAlgorithmsMixin,
    GraphIntrospectionMixin,
    TimeWindowMixin,
)


class ComputeGraph(ComponentBuilderMixin, GraphAlgorithmsMixin,
                   TimeWindowMixin, GraphIntrospectionMixin):
    """Compute graph for DAG execution"""

    def __init__(self, config, lazy_init=False):
        """
        Initialize ComputeGraph.
        
        Args:
            config: DAG configuration dict or path to JSON file
            lazy_init: If True, only load config without creating subscribers/publishers.
                      This is used in main process when worker pool is enabled.
                      Components are created later when DAG is actually started.
        """
        self.prop_conf = PropertiesConfigurator()
        # Load config if it's a file path
        if isinstance(config, str):
            '''
            with open(config, 'r') as f:
                config = json.load(f)
            '''
            file_path = config
            self._config_base_path = str(Path(file_path).parent)
            config = self.prop_conf.load_and_resolve_json_file_content(file_path)

        self.config = config
        self.name = config.get('name')

        # Handle time window with duration
        raw_start_time = config.get('start_time')
        duration = config.get('duration')

        # DEPRECATED: Support legacy end_time for backward compatibility
        # If end_time is present but duration is not, calculate duration from end_time
        legacy_end_time = config.get('end_time')

        if raw_start_time:
            # New duration-based approach
            if duration is not None:
                # Use duration to calculate end_time
                self.start_time = raw_start_time.replace(':', '')
                self.duration = duration
                self.end_time = calculate_end_time(self.start_time, duration)
                logger.info(f"DAG {self.name}: Using duration-based time window")
                logger.info(f"  start_time={self.start_time}, duration={duration}, end_time={self.end_time}")
            elif legacy_end_time is not None:
                # Legacy mode: end_time provided
                self.start_time = raw_start_time.replace(':', '')
                self.end_time = legacy_end_time.replace(':', '')
                self.duration = None  # No duration in legacy mode
                logger.warning(f"DAG {self.name}: Using DEPRECATED end_time. Please migrate to duration.")
                logger.info(f"  start_time={self.start_time}, end_time={self.end_time}")
            else:
                # Duration not provided, use default (-5 minutes)
                self.start_time = raw_start_time.replace(':', '')
                self.duration = None  # None means use default
                self.end_time = calculate_end_time(self.start_time, None)
                logger.info(f"DAG {self.name}: No duration provided, using default (-5 minutes)")
                logger.info(f"  start_time={self.start_time}, end_time={self.end_time}")
        else:
            # No start_time: perpetual running (ignore duration)
            self.start_time = None
            self.end_time = None
            self.duration = None
            logger.info(f"DAG {self.name}: No start_time provided, running perpetually")

        # v2.1.0: optional day-of-week + holiday-calendar schedule. Parsed
        # and validated eagerly (fail fast: a trading DAG must not load
        # with a broken holiday configuration). None = legacy behaviour.
        self.schedule = DagSchedule.from_config(config.get('schedule'))
        if self.schedule is not None:
            logger.info(f"DAG {self.name}: schedule configured "
                        f"{self.schedule.to_dict()}")

        self.subscribers = {}
        self.publishers = {}
        self.calculators = {}
        self.transformers = {}
        self.nodes = {}
        self.edges = []
        
        # Subgraph support
        self.subgraph_nodes = {}  # name -> SubgraphNode
        self.subgraph_supervisor = None
        # BUGFIX v2.0.0: only initialize when not already set from a file
        # path above - the unconditional assignment previously clobbered the
        # base path and broke relative subgraph file resolution.
        if not hasattr(self, '_config_base_path'):
            self._config_base_path = None  # For resolving subgraph file paths

        self._compute_thread = None
        self._stop_event = threading.Event()
        self._suspend_event = threading.Event()
        self._suspend_event.set()  # Start in running state

        # v3.0.0: event-driven wakeup for the compute loop. Subscribers (and
        # metronomes) set this when new work arrives so the loop reacts
        # immediately instead of waiting for a fixed poll interval. A small
        # fallback timeout preserves liveness for sources that do not signal.
        self._work_available = threading.Event()
        self._idle_poll_interval = 0.01  # fallback wait; was the fixed sleep

        self._time_check_thread = None
        
        # UI override flag: when True, time window checker won't auto-suspend
        # This is set when DAG is manually started via UI while outside time window
        # It takes precedence over time window scheduling until manually stopped
        self._ui_override = False
        
        # UI suspended flag: when True, time window checker won't auto-resume
        # This is set when DAG is manually suspended via UI
        self._ui_suspended = False

        # v1.5.2: Lazy initialization support
        # When lazy_init=True, we only load config metadata (for main process with worker pool)
        # Components are created when DAG is actually started in main process or loaded in worker
        self._lazy_init = lazy_init
        self._components_built = False
        
        if not lazy_init:
            self._build_components()
            self.build_dag()
            self._components_built = True
        else:
            logger.info(f"DAG {self.name}: Lazy initialization (components deferred)")

        logger.info(f"DAG {self.name}: initialized")

    def subscriber_by_name(self, sub_name):
        """Return the named subscriber; raises KeyError if it isn't declared."""
        return self.subscribers[sub_name]

    def get_publisher_by_name(self, publisher_name):
        """Return the named publisher (v2.0.0 - correctly spelled)."""
        return self.publishers[publisher_name]

    def publisher_by_name(self, pub_name):
        """Return the named publisher.

        v2.0.0 BUGFIX: this previously returned from ``self.subscribers``
        (the wrong dictionary), so callers always received a subscriber or
        a KeyError. No in-repo callers existed, confirming the bug was
        latent; it now correctly reads from ``self.publishers``.
        """
        return self.publishers[pub_name]

    def get_publiisher_by_name(self, publisher_name):
        """DEPRECATED misspelled alias (double 'i') kept for backward
        compatibility with external DAG calculator code. Use
        :meth:`get_publisher_by_name` instead."""
        logger.warning(
            "get_publiisher_by_name() is a deprecated misspelling - "
            "update callers to get_publisher_by_name()")
        return self.get_publisher_by_name(publisher_name)

    def publish(self, publisher_name, data_to_send):
        """Route data to the named publisher (used by PublisherSinkNode). Raises
        KeyError if the publisher isn't declared."""
        self.publishers[publisher_name].publish(data_to_send)

    def build_dag(self):
        """Build the DAG from config"""
        # Build nodes
        for node_config in self.config.get('nodes', []):
            name = node_config['name']
            node_type = node_config.get('type', 'CalculationNode')
            config = node_config.get('config', {})

            # Handle SubgraphNode specially
            if node_type == 'SubgraphNode':
                node = self._build_subgraph_node(node_config)
            elif node_type in globals():
                node = globals()[node_type](name, config)
            else:
                # Custom node
                parts = node_type.rsplit('.', 1)
                module_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else node_type
                node = instantiate_module(module_path, class_name, {'name': name, 'config': config})

            # Set subscriber if specified. A declared reference that does not resolve is
            # a hard error - silently dropping it would yield a node that never receives
            # input, which is worse than failing fast at load time.
            if 'subscriber' in node_config:
                sub_name = node_config['subscriber']
                if sub_name not in self.subscribers:
                    raise ValueError(
                        f"DAG '{self.name}': node '{name}' references undefined "
                        f"subscriber '{sub_name}'")
                node.set_subscriber(self.subscribers[sub_name])

            # Add publishers if specified
            if 'publishers' in node_config:
                for pub_name in node_config['publishers']:
                    if pub_name not in self.publishers:
                        raise ValueError(
                            f"DAG '{self.name}': node '{name}' references undefined "
                            f"publisher '{pub_name}'")
                    node.add_publisher(self.publishers[pub_name])

            # Set calculator if specified
            if 'calculator' in node_config:
                calc_name = node_config['calculator']
                if calc_name not in self.calculators:
                    raise ValueError(
                        f"DAG '{self.name}': node '{name}' references undefined "
                        f"calculator '{calc_name}'")
                node.set_calculator(self.calculators[calc_name])

            # Add input transformers if specified. 'passthru' and 'null' are implicit
            # built-ins (pre-seeded in self.transformers), so they resolve without an
            # explicit definition; any other unresolved name is a hard error.
            if 'input_transformers' in node_config:
                for trans_name in node_config['input_transformers']:
                    if trans_name not in self.transformers:
                        raise ValueError(
                            f"DAG '{self.name}': node '{name}' references undefined "
                            f"input transformer '{trans_name}'")
                    node.add_input_transformer(self.transformers[trans_name])

            # Add output transformers if specified
            if 'output_transformers' in node_config:
                for trans_name in node_config['output_transformers']:
                    if trans_name not in self.transformers:
                        raise ValueError(
                            f"DAG '{self.name}': node '{name}' references undefined "
                            f"output transformer '{trans_name}'")
                    node.add_output_transformer(self.transformers[trans_name])

            node.set_graph(self)
            self.nodes[name] = node
            logger.info(f"Created node: {name}")

        # Build edges
        for edge_config in self.config.get('edges', []):
            from_node_name = edge_config['from_node']
            to_node_name = edge_config['to_node']
            transformer_name = edge_config.get('data_transformer')
            pname = edge_config.get('pname')

            from_node = self.nodes[from_node_name]
            to_node = self.nodes[to_node_name]
            transformer = self.transformers.get(transformer_name) if transformer_name else None

            edge = Edge(from_node, to_node, transformer, pname)
            self.edges.append(edge)
            logger.info(f"Created edge: {edge.name}")

        # Check for cycles
        if self._has_cycle():
            cycle_info = self._find_cycle()
            error_msg = f"Cycle detected in graph: {' -> '.join(cycle_info)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Initialize subgraph supervisor if we have subgraphs
        if self.subgraph_nodes:
            supervisor_config = self.config.get('subgraph_supervisor', {})
            self.subgraph_supervisor = SubgraphSupervisor(self, supervisor_config)
            for sg_node in self.subgraph_nodes.values():
                self.subgraph_supervisor.register_subgraph(sg_node)
            logger.info(f"Subgraph supervisor initialized with {len(self.subgraph_nodes)} subgraphs")

        # v5.14.0: invalidate the memoized topological order; the structure is
        # static after this point until the next build.
        self._topo_cache = None

        logger.info(f"DAG built successfully with {len(self.nodes)} nodes and {len(self.edges)} edges")

    def get_node(self, name: str):
        """Get a node by name."""
        return self.nodes.get(name)

    def start(self, force=False):
        """Start the compute graph
        
        Args:
            force: If True, start even if outside time window and don't auto-suspend
                   (This is always True for UI-driven starts)
        """
        # v1.5.2: Build components if lazy-initialized
        # This happens when DAG runs in main process but was loaded with lazy_init=True
        if not self._components_built:
            logger.info(f"DAG {self.name}: Building deferred components...")
            self._build_components()
            self.build_dag()
            self._components_built = True
        
        # Check if we're starting outside the active schedule (time window
        # + day-of-week + holidays in v2.1.0)
        if (self.start_time and self.end_time) or self.schedule is not None:
            active, reason = self.is_within_schedule()
            if not active:
                # Starting outside schedule - set UI override to prevent auto-suspend
                self._ui_override = True
                logger.info(f"DAG {self.name}: Started outside schedule "
                            f"({reason}) - UI override enabled, "
                            f"auto-suspend disabled")
            else:
                # Starting within schedule - clear UI override (normal behavior)
                self._ui_override = False
                logger.info(f"DAG {self.name}: Started within schedule - normal scheduling")
        
        # CRITICAL: Ensure DAG is in running state (not suspended)
        self._suspend_event.set()
        
        # Start all subscribers
        for subscriber in self.subscribers.values():
            subscriber.start()
            # CRITICAL: Ensure subscriber is resumed (not suspended from previous run)
            subscriber.resume()

        # Start metronome nodes
        for node in self.nodes.values():
            if isinstance(node, MetronomeNode):
                # v3.0.0: let ticks wake the compute loop immediately.
                node._notify_callback = self.notify_work
                node.start_metronome()

        # v3.0.0: wire subscribers to wake the compute loop on data arrival.
        for subscriber in self.subscribers.values():
            try:
                subscriber.set_notify_callback(self.notify_work)
            except Exception:
                pass  # older/foreign subscribers without the hook still work

        # Start compute thread
        if not self._compute_thread or not self._compute_thread.is_alive():
            self._stop_event.clear()
            self._compute_thread = threading.Thread(target=self.do_compute, daemon=True)
            self._compute_thread.start()

        # Start the schedule monitor thread when a time window OR a
        # day-of-week/holiday schedule is configured (v2.1.0: the loop
        # evaluates the full schedule, not just the time window).
        if (self.start_time and self.end_time) or self.schedule is not None:
            if not self._time_check_thread or not self._time_check_thread.is_alive():
                self._time_check_thread = threading.Thread(target=self._time_window_check, daemon=True)
                self._time_check_thread.start()
                logger.info(f"DAG {self.name}: Schedule monitor started "
                            f"(window={self.start_time}-{self.end_time}, "
                            f"schedule={self.schedule.to_dict() if self.schedule else None})")

        logger.info(f"DAG {self.name}: started")

    def suspend(self, ui_driven=False):
        """Suspend the compute graph
        
        Args:
            ui_driven: If True, this is a UI-driven suspend that should prevent auto-resume
        """
        self._suspend_event.clear()
        
        if ui_driven:
            self._ui_suspended = True
            logger.info(f"DAG {self.name}: suspended by UI - auto-resume disabled")
        
        for subscriber in self.subscribers.values():
            subscriber.suspend()

        logger.info(f"DAG {self.name}: suspended")

    def resume(self, ui_driven=False):
        """Resume the compute graph
        
        Args:
            ui_driven: If True, this is a UI-driven resume that clears the UI suspended flag
        """
        self._suspend_event.set()
        
        if ui_driven:
            self._ui_suspended = False
            logger.info(f"DAG {self.name}: resumed by UI - normal scheduling")

        for subscriber in self.subscribers.values():
            subscriber.resume()

        logger.info(f"DAG {self.name}: resumed")

    # ------------------------------------------------------------------ #
    # v5.15.0: drain-mode freeze of subscribers (maintenance windows)
    # ------------------------------------------------------------------ #
    def freeze_subscribers(self, names=None):
        """Freeze all (names=None) or the named subscribers so they stop pulling
        from their brokers. Compute and publishing keep running, so the DAG
        drains. Returns the list of affected subscriber names."""
        affected = []
        for sub_name, sub in self.subscribers.items():
            if names is None or sub_name in names:
                if hasattr(sub, 'freeze'):
                    sub.freeze()
                    affected.append(sub_name)
        logger.info(f"DAG {self.name}: froze {len(affected)} subscriber(s): {affected}")
        return affected

    def unfreeze_subscribers(self, names=None):
        """Unfreeze all (names=None) or the named subscribers."""
        affected = []
        for sub_name, sub in self.subscribers.items():
            if names is None or sub_name in names:
                if hasattr(sub, 'unfreeze'):
                    sub.unfreeze()
                    affected.append(sub_name)
        logger.info(f"DAG {self.name}: unfroze {len(affected)} subscriber(s): {affected}")
        return affected

    def drain_status(self):
        """v5.15.0: snapshot of how 'drained' the DAG is, for maintenance windows.

        Reports frozen subscribers, total messages still queued at subscribers and
        publishers, and whether the DAG is fully drained (no frozen-subscriber
        intake AND nothing left queued). Note: 'publications finished' also
        requires any async-egress WAL to be empty - publishers that expose a
        pending count contribute it.
        """
        frozen = []
        sub_queue_total = 0
        any_frozen = False
        for sub_name, sub in self.subscribers.items():
            depth = sub.get_queue_size() if hasattr(sub, 'get_queue_size') else 0
            sub_queue_total += depth
            if hasattr(sub, 'is_frozen') and sub.is_frozen():
                frozen.append(sub_name)
                any_frozen = True
        pub_queue_total = 0
        wal_pending = 0
        for pub in self.publishers.values():
            if hasattr(pub, 'get_queue_size'):
                try:
                    pub_queue_total += pub.get_queue_size() or 0
                except Exception:  # noqa: BLE001
                    pass
            pend = getattr(pub, 'async_pending', None)
            if callable(pend):
                try:
                    wal_pending += pend() or 0
                except Exception:  # noqa: BLE001
                    pass
        drained = (sub_queue_total == 0 and pub_queue_total == 0 and wal_pending == 0)
        return {
            'frozen_subscribers': frozen,
            'any_frozen': any_frozen,
            'subscriber_queue_total': sub_queue_total,
            'publisher_queue_total': pub_queue_total,
            'wal_pending': wal_pending,
            'drained': drained,
        }

    def stop(self):
        """Stop the compute graph"""
        self._stop_event.set()
        self._suspend_event.set()  # Unblock if suspended
        self._ui_override = False  # Clear UI override flag
        self._ui_suspended = False  # Clear UI suspended flag

        # Stop subscribers
        for subscriber in self.subscribers.values():
            subscriber.stop()

        # Stop publishers
        for publisher in self.publishers.values():
            publisher.stop()

        # Stop metronome nodes
        for node in self.nodes.values():
            if isinstance(node, MetronomeNode):
                node.stop_metronome()

        # Wait for compute thread
        if self._compute_thread:
            self._compute_thread.join(timeout=5)

        logger.info(f"DAG {self.name}: stopped")

    def notify_work(self):
        """v3.0.0: wake the compute loop immediately (event-driven).

        Called by subscribers when data arrives and by metronome ticks. Safe to
        call from any thread; if the loop is mid-sweep the event simply stays
        set and is consumed on the next wait.
        """
        self._work_available.set()

    def do_compute(self):
        """Main compute loop (v3.0.0: event-driven, minimal polling).

        Instead of a fixed sleep between sweeps, the loop reacts to work:
        - If a sweep did real work, it loops again immediately to drain the
          pipeline at full speed (per-hop latency approaches the cost of the
          work itself rather than a poll interval).
        - If a sweep was idle, it blocks on ``_work_available`` with a small
          fallback timeout, so an incoming message wakes it in well under a
          millisecond while still staying responsive to non-signalling sources.
        """
        sorted_nodes = self.topological_sort()

        while not self._stop_event.is_set():
            self._suspend_event.wait()  # Block if suspended

            if self._stop_event.is_set():
                break

            # Consume any pending wakeup before the sweep so signals that arrive
            # during the sweep are not lost (they re-set the event -> next wait
            # returns immediately).
            self._work_available.clear()

            # Pre-compute phase
            for node in sorted_nodes:
                node.pre_compute()

            # Compute phase
            acted_node_set = []
            for node in sorted_nodes:
                if node.isdirty():
                    computed = node.compute()
                    if computed:
                        acted_node_set.append(node)
            for node in acted_node_set:
                node.increment_compute_count()
                node.post_compute()

            if acted_node_set:
                # Work was done: drain immediately, do not wait. This is what
                # collapses multi-hop latency from (depth x poll_interval) down
                # to the cost of the work itself.
                continue

            # Idle: wait for a wakeup signal, with a small fallback timeout so
            # non-signalling sources (and safety) still get serviced.
            self._work_available.wait(timeout=self._idle_poll_interval)

    def clone(self, start_time=None, duration=None, lazy_init=False):
        """
        Clone the DAG with optional time window override.

        Args:
            start_time: New start time in HHMM format, or None to remove time window
            duration: New duration string (e.g., "1h", "30m"), or None for default (-5m)
            lazy_init: If True, create with lazy initialization (v1.5.2)

        Rules:
            - If both start_time and duration are None: Remove time window (perpetual running)
            - If start_time is provided but duration is None: Use default duration (-5m)
            - If start_time is None: Remove time window (ignore duration)
            - If both provided: Set new time window
            - Autoclone config is ALWAYS removed from clones (prevents recursive cloning)
        """
        from datetime import datetime

        # Create timestamp for unique name
        cloned_config = self.config.copy()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        new_name = f"{self.name}_{timestamp}"
        cloned_config['name'] = new_name

        # CRITICAL: Remove autoclone config from cloned DAG
        # Cloned and autocloned DAGs should never be able to autoclone
        if 'autoclone' in cloned_config:
            cloned_config.pop('autoclone')
            logger.info(f"Removed autoclone config from cloned DAG {new_name} (clones cannot autoclone)")

        # Handle time window
        if start_time is None:
            # Remove time window completely (perpetual running)
            cloned_config.pop('start_time', None)
            cloned_config.pop('end_time', None)  # Remove legacy
            cloned_config.pop('duration', None)
            logger.info(f"Cloning {self.name} to {new_name} with NO time window (perpetual)")
        else:
            # Set new time window with duration
            cloned_config['start_time'] = start_time
            cloned_config['duration'] = duration
            # Remove legacy end_time if present
            cloned_config.pop('end_time', None)

            if duration:
                logger.info(f"Cloning {self.name} to {new_name} with start_time={start_time}, duration={duration}")
            else:
                logger.info(f"Cloning {self.name} to {new_name} with start_time={start_time}, duration=default (-5m)")

        # v1.5.2: Pass lazy_init to cloned DAG
        cloned_dag = ComputeGraph(cloned_config, lazy_init=lazy_init)

        logger.info(f"Cloned DAG {self.name} to {new_name}")
        logger.info(f"  Original: start_time={self.start_time}, duration={self.duration}, end_time={self.end_time}")
        logger.info(
            f"  Cloned: start_time={cloned_dag.start_time}, duration={cloned_dag.duration}, end_time={cloned_dag.end_time}")

        return cloned_dag

