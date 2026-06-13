"""
ComputeGraph Support Mixins (v2.0.0 module split)
=================================================

Collaborator mixins for :class:`core.dag.compute_graph.ComputeGraph`,
extracted verbatim so each module stays within the 500-line architecture
limit. All state lives on ComputeGraph.

    GraphAlgorithmsMixin   - cycle detection and topological ordering.
    TimeWindowMixin        - the background time-window checker thread and
                             the wrap-around-aware is_in_time_window()
                             (v2.0.0 midnight bugfix).
    GraphIntrospectionMixin- JSON/details rendering and the subgraph
                             light-up/light-down control surface.

Patent Pending - DishtaYantra Framework
Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

import json
import logging
import time
import traceback
from collections import deque
from datetime import datetime

from core.dag.subgraph import SubgraphState
from core.dag.time_window_utils import get_time_window_info, is_within_time_window
from core.schedule.dag_schedule import is_schedule_active

logger = logging.getLogger(__name__)

# Sentinel so apply_schedule_update() can distinguish "duration not passed"
# (keep current value) from an explicit None (perpetual / default window).
_UNSET = object()


class GraphAlgorithmsMixin:
    """Cycle detection and topological ordering."""

    def _has_cycle(self):
        """Check if graph has a cycle using DFS"""
        visited = set()
        rec_stack = set()

        def dfs(node):
            visited.add(node.name)
            rec_stack.add(node.name)

            for edge in node._outgoing_edges:
                child = edge.to_node
                if child.name not in visited:
                    if dfs(child):
                        return True
                elif child.name in rec_stack:
                    return True

            rec_stack.remove(node.name)
            return False

        for node in self.nodes.values():
            if node.name not in visited:
                if dfs(node):
                    return True

        return False

    def _find_cycle(self):
        """Find a cycle in the graph for error reporting"""
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node):
            visited.add(node.name)
            rec_stack.add(node.name)
            path.append(node.name)

            for edge in node._outgoing_edges:
                child = edge.to_node
                if child.name not in visited:
                    if dfs(child):
                        return True
                elif child.name in rec_stack:
                    # Found cycle - find start of cycle in path
                    cycle_start = path.index(child.name)
                    path.append(child.name)
                    return path[cycle_start:]

            rec_stack.remove(node.name)
            path.pop()
            return False

        for node in self.nodes.values():
            if node.name not in visited:
                result = dfs(node)
                if result:
                    return result

        return []

    def topological_sort(self):
        """Return nodes in topological order"""
        in_degree = {name: 0 for name in self.nodes}

        for node in self.nodes.values():
            for edge in node._outgoing_edges:
                in_degree[edge.to_node.name] += 1

        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        sorted_nodes = []

        while queue:
            node_name = queue.popleft()
            sorted_nodes.append(self.nodes[node_name])

            for edge in self.nodes[node_name]._outgoing_edges:
                child_name = edge.to_node.name
                in_degree[child_name] -= 1
                if in_degree[child_name] == 0:
                    queue.append(child_name)

        return sorted_nodes



class TimeWindowMixin:
    """Schedule monitor thread + activity checks (time window, day-of-week,
    holiday calendars - the last two are the v2.1.0 additions)."""

    def is_within_schedule(self, now=None):
        """
        Full activity check (v2.1.0): wrap-around-aware time window AND
        day-of-week AND holiday calendars.

        Returns:
            (active, reason)
        """
        return is_schedule_active(self.start_time, self.end_time,
                                  getattr(self, 'schedule', None), now)

    def apply_schedule_update(self, start_time, end_time, schedule,
                              duration=_UNSET):
        """
        Apply an intraday schedule change (v2.1.0). Called by the DAG
        server's schedule-refresh loop when the DAG's configuration object
        in storage changed; together with the holiday-calendar TTL this
        guarantees edits take effect within 5 minutes. The monitor loop
        picks the new rules up on its next 60-second pass.

        v2.2 BUGFIX: also refresh ``self.duration``. The scheduling decision
        uses ``end_time`` (which was always updated correctly), but the
        dashboard's Time Window column and duration badge are derived from
        ``start_time`` + ``duration``. Leaving ``duration`` stale made an
        edited window display the OLD span (e.g. a new 1100/01h00m window
        rendered as 11:00-17:30 using the previous 6h30m duration), even
        though the DAG actually ran 11:00-12:00. ``duration`` defaults to a
        sentinel so existing callers that omit it keep the old value.
        """
        old = (self.start_time, self.end_time,
               self.schedule.to_dict() if getattr(self, 'schedule', None)
               else None)
        self.start_time = start_time
        self.end_time = end_time
        self.schedule = schedule
        if duration is not _UNSET:
            self.duration = duration
        new = (self.start_time, self.end_time,
               self.schedule.to_dict() if self.schedule else None)
        logger.info("")
        logger.info("█" * 70)
        logger.info(f"█  DAG SCHEDULE UPDATED INTRADAY")
        logger.info(f"█  DAG: {self.name}")
        logger.info(f"█  Old: window={old[0]}-{old[1]} schedule={old[2]}")
        logger.info(f"█  New: window={new[0]}-{new[1]} schedule={new[2]}")
        logger.info(f"█  Effective: within the next monitor cycle (<=60s)")
        logger.info("█" * 70)
        logger.info("")

    def _time_window_check(self):
        """Monitor loop: auto-suspend/resume based on the full schedule
        (time window + day-of-week + holiday calendars in v2.1.0)."""
        logger.info(f"DAG {self.name}: Schedule checker started")

        while not self._stop_event.is_set():
            has_window = bool(self.start_time and self.end_time)
            has_schedule = getattr(self, 'schedule', None) is not None
            if not has_window and not has_schedule:
                # Nothing to enforce (rules may appear intraday via
                # apply_schedule_update, so keep the loop alive).
                time.sleep(60)
                continue

            now = datetime.now()
            current_time = now.strftime('%H%M')

            # v2.0.0: wrap-around-aware window; v2.1.0: plus day-of-week
            # and holiday-calendar rules.
            in_window, schedule_reason = self.is_within_schedule(now)

            logger.debug(
                f"DAG {self.name} schedule check: current={current_time}, start={self.start_time}, end={self.end_time}, in_window={in_window}, reason={schedule_reason}, ui_override={self._ui_override}, ui_suspended={self._ui_suspended}")

            if in_window:
                # Within time window - should be running
                # Clear UI override flag when entering time window (normal behavior resumes)
                if self._ui_override:
                    self._ui_override = False
                    logger.info(f"DAG {self.name}: Entered time window - UI override cleared, normal scheduling resumes")
                
                # Only auto-resume if NOT UI-suspended
                if self._ui_suspended:
                    logger.debug(f"DAG {self.name}: In time window but UI-suspended, not auto-resuming")
                elif not self._suspend_event.is_set():
                    logger.info("")
                    logger.info("█" * 70)
                    logger.info(f"█  DAG SCHEDULE: AUTO-RESUME")
                    logger.info(f"█  DAG: {self.name}")
                    logger.info(f"█  Window: {self.start_time} - {self.end_time}")
                    logger.info(f"█  Schedule: {self.schedule.to_dict() if getattr(self, 'schedule', None) else 'time window only'}")
                    logger.info(f"█  Current Time: {current_time}")
                    logger.info(f"█  Action: Resuming DAG (schedule active)")
                    logger.info("█" * 70)
                    logger.info("")
                    self.resume()
            else:
                # Outside time window
                # Only auto-suspend if NOT UI-overridden
                if self._ui_override:
                    # UI override active: don't auto-suspend, just log at debug level
                    logger.debug(f"DAG {self.name}: Outside time window but UI override active, not suspending")
                else:
                    # Normal mode: auto-suspend when outside window
                    if self._suspend_event.is_set():
                        logger.info("")
                        logger.info("█" * 70)
                        logger.info(f"█  DAG SCHEDULE: AUTO-SUSPEND")
                        logger.info(f"█  DAG: {self.name}")
                        logger.info(f"█  Window: {self.start_time} - {self.end_time}")
                        logger.info(f"█  Current Time: {current_time}")
                        logger.info(f"█  Reason: {schedule_reason}")
                        logger.info(f"█  Action: Suspending DAG (schedule inactive)")
                        logger.info("█" * 70)
                        logger.info("")
                        self.suspend()

            time.sleep(60)  # Check every minute

        logger.info(f"DAG {self.name}: Time window checker stopped")

    def is_in_time_window(self):
        """Check if current time is within the configured window.

        v2.0.0 BUGFIX: uses the minutes-since-midnight helper with an
        explicit wrap-around branch so overnight windows (2200-0600) work.
        """
        if not self.start_time or not self.end_time:
            return True  # Always active if no window configured
        return is_within_time_window(self.start_time, self.end_time)



class GraphIntrospectionMixin:
    """JSON/details rendering and subgraph light controls."""

    def show_json(self):
        """Return configuration as JSON string"""
        return json.dumps(self.config, indent=2)

    def details(self):
        """Return details of the compute graph
        
        v1.5.2: Updated to work with lazy-initialized DAGs. When components aren't
        built, returns info from config instead of empty dictionaries.
        """
        # v1.5.2: Get subscriber/publisher info from config if lazy-initialized
        if not self._components_built:
            # Extract info from config for lazy-initialized DAGs
            subscribers_info = {}
            for sub_cfg in self.config.get('subscribers', []):
                name = sub_cfg.get('name')
                config = sub_cfg.get('config', {})
                subscribers_info[name] = {
                    'name': name,
                    'type': sub_cfg.get('type', 'Unknown'),
                    'source': config.get('source', 'N/A'),
                    'config': config,
                    'queue_depth': 0,
                    'status': 'not_started (lazy init)'
                }
            
            publishers_info = {}
            for pub_cfg in self.config.get('publishers', []):
                name = pub_cfg.get('name')
                config = pub_cfg.get('config', {})
                publishers_info[name] = {
                    'name': name,
                    'type': pub_cfg.get('type', 'Unknown'),
                    'destination': config.get('destination', 'N/A'),
                    'config': config,
                    'status': 'not_started (lazy init)'
                }
            
            calculators_info = {}
            for calc_cfg in self.config.get('calculators', []):
                name = calc_cfg.get('name')
                calculators_info[name] = {
                    'name': name,
                    'type': calc_cfg.get('type', 'Unknown'),
                    'config': calc_cfg.get('config', {}),
                    'status': 'not_started (lazy init)'
                }
            
            transformers_info = {}
            for trans_cfg in self.config.get('transformers', []):
                name = trans_cfg.get('name')
                transformers_info[name] = {
                    'name': name,
                    'type': trans_cfg.get('type', 'Unknown'),
                    'config': trans_cfg.get('config', {}),
                    'status': 'not_started (lazy init)'
                }
            
            nodes_info = {}
            for node_cfg in self.config.get('nodes', []):
                name = node_cfg.get('name')
                nodes_info[name] = {
                    'name': name,
                    'type': node_cfg.get('type', 'Unknown'),
                    'config': node_cfg.get('config', {}),
                    'status': 'not_started (lazy init)'
                }
            
            edges_info = []
            for edge_cfg in self.config.get('edges', []):
                edges_info.append({
                    'from_node': edge_cfg.get('from_node'),
                    'to_node': edge_cfg.get('to_node')
                })
            
            return {
                'name': self.name,
                'start_time': self.start_time,
                'end_time': self.end_time,
                'duration': self.duration,
                'schedule': self.schedule.to_dict()
                if getattr(self, 'schedule', None) else None,
                'is_running': False,  # Can't be running if lazy-initialized
                'is_suspended': False,
                'ui_override': self._ui_override,
                'ui_suspended': self._ui_suspended,
                'lazy_init': True,
                'nodes': nodes_info,
                'edges': edges_info,
                'subscribers': subscribers_info,
                'publishers': publishers_info,
                'calculators': calculators_info,
                'transformers': transformers_info
            }
        
        # Normal path - components are built
        result = {
            'name': self.name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration,
            'schedule': self.schedule.to_dict()
            if getattr(self, 'schedule', None) else None,
            'is_running': self._compute_thread and self._compute_thread.is_alive(),
            'is_suspended': not self._suspend_event.is_set(),
            'ui_override': self._ui_override,
            'ui_suspended': self._ui_suspended,
            'lazy_init': False,
            'nodes': {name: node.details() for name, node in self.nodes.items()},
            'edges': [edge.details() for edge in self.edges],
            'subscribers': {name: sub.details() for name, sub in self.subscribers.items()},
            'publishers': {name: pub.details() for name, pub in self.publishers.items()},
            'calculators': {name: calc.details() for name, calc in self.calculators.items()},
            'transformers': {name: trans.details() for name, trans in self.transformers.items()}
        }
        
        # Add subgraph information
        if self.subgraph_nodes:
            result['subgraphs'] = {
                name: sg_node.to_dict() for name, sg_node in self.subgraph_nodes.items()
            }
            result['subgraph_count'] = len(self.subgraph_nodes)
        
        # Add supervisor status
        if self.subgraph_supervisor:
            result['subgraph_supervisor'] = self.subgraph_supervisor.to_dict()
        
        return result
    
    # Subgraph control methods
    
    def light_up_subgraph(self, subgraph_name: str, reason: str = None):
        """Activate a subgraph."""
        if self.subgraph_supervisor:
            self.subgraph_supervisor.light_up(subgraph_name, reason)
        elif subgraph_name in self.subgraph_nodes:
            self.subgraph_nodes[subgraph_name].light_up(reason)
    
    def light_down_subgraph(self, subgraph_name: str, reason: str = None):
        """Suspend a subgraph."""
        if self.subgraph_supervisor:
            self.subgraph_supervisor.light_down(subgraph_name, reason)
        elif subgraph_name in self.subgraph_nodes:
            self.subgraph_nodes[subgraph_name].light_down(reason)
    
    def light_up_all_subgraphs(self, reason: str = None):
        """Activate all subgraphs."""
        if self.subgraph_supervisor:
            self.subgraph_supervisor.light_up_all(reason)
    
    def light_down_all_subgraphs(self, reason: str = None):
        """Suspend all subgraphs."""
        if self.subgraph_supervisor:
            self.subgraph_supervisor.light_down_all(reason)
    
    def get_subgraph_status(self) -> dict:
        """Get status of all subgraphs."""
        if self.subgraph_supervisor:
            return self.subgraph_supervisor.get_status()
        return {}