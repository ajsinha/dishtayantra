"""
DAG Server Support Mixins (v2.0.0)
==================================

Collaborator mixins for :class:`core.dag.dag_server.DAGComputeServer`,
split out so each module stays within the 500-line architecture limit:

    DAGLoaderMixin   - reading DAG JSON objects through the storage
                       abstraction (direct children of storage.dags.prefix
                       only, matching the legacy os.listdir behaviour).
    DAGMonitorMixin  - auto-start of time-window-eligible DAGs, the
                       background time-window monitor loop, the boxed DAG
                       state-change logging, and the HA election callbacks
                       (_on_elected / _on_demoted / _check_primary).

These are pure method carriers - all state lives on DAGComputeServer.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import threading
import traceback
from datetime import datetime

from core.dag.compute_graph import ComputeGraph
from core.dag.time_window_utils import parse_duration, calculate_end_time

logger = logging.getLogger(__name__)


# v2.2: DAGLoaderMixin moved to dag_server_loader.py (kept under the
# 500-line limit). Re-exported here so existing imports still resolve.
from core.dag.dag_server_loader import DAGLoaderMixin  # noqa: F401,E402


class DAGMonitorMixin:
    """Time-window monitoring, state-change logging, HA callbacks."""

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
                # Check if DAG is perpetual (no time window and no
                # day-of-week/holiday schedule - v2.1.0)
                is_perpetual = ((dag.start_time is None or dag.end_time is None)
                                and getattr(dag, 'schedule', None) is None)

                if is_perpetual:
                    dags_to_start.append((dag_name, 'PERPETUAL (no time window)'))
                    logger.info(f"AUTO-START: DAG '{dag_name}' is PERPETUAL - will be started")
                else:
                    # v2.1.0: full schedule check - time window AND
                    # day-of-week AND holiday calendars.
                    active, reason = dag.is_within_schedule()
                    if active:
                        dags_to_start.append((dag_name, f'SCHEDULE ACTIVE ({dag.start_time}-{dag.end_time})'))
                        logger.info(f"AUTO-START: DAG '{dag_name}' SCHEDULE ACTIVE ({dag.start_time}-{dag.end_time}) - will be started")
                    else:
                        logger.info(f"AUTO-START: DAG '{dag_name}' schedule inactive ({reason}) - will NOT be started")
        
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
        self._start_schedule_refresh()

    def _start_schedule_refresh(self):
        """Start the intraday schedule-refresh thread (v2.1.0)."""
        if getattr(self, '_schedule_refresh_thread', None) and \
                self._schedule_refresh_thread.is_alive():
            return
        self._schedule_refresh_thread = threading.Thread(
            target=self._schedule_refresh_loop, daemon=True)
        self._schedule_refresh_thread.start()
        logger.info("Schedule refresh thread started (re-reads DAG "
                    "schedules from storage every %ss)",
                    self._schedule_refresh_seconds)

    def _schedule_refresh_loop(self):
        """
        Intraday schedule update loop (v2.1.0).

        Every ``schedule.refresh_seconds`` (default 240, hard-capped at
        240) this re-reads each DAG's configuration object from storage
        and, when the scheduling fields changed (start_time / end_time /
        duration / schedule), applies them to the LIVE ComputeGraph via
        apply_schedule_update(). Combined with the 60-second monitor loop
        this guarantees that an intraday edit to a DAG's schedule takes
        effect automatically within 5 minutes (240s + 60s = 300s
        worst case). Holiday-calendar file edits are handled separately by
        the HolidayCalendarManager's own <=180s reload TTL.

        Failures are logged with full stack traces and never crash the
        loop - the previous schedule simply stays in effect.
        """
        import time as time_module
        from core.dag.time_window_utils import calculate_end_time
        from core.schedule.dag_schedule import DagSchedule

        self._time_window_stop_event.wait(10)  # let startup settle
        while not self._time_window_stop_event.is_set():
            try:
                with self._lock:
                    snapshot = {name: dag.config.get('config_filename')
                                for name, dag in self.dags.items()}
                for dag_name in snapshot:
                    if self._time_window_stop_event.is_set():
                        return
                    try:
                        self._refresh_dag_schedule(dag_name)
                    except Exception as e:  # noqa: BLE001
                        logger.error(f"Schedule refresh failed for DAG "
                                     f"'{dag_name}': {e}")
                        logger.error(traceback.format_exc())
            except Exception as e:  # noqa: BLE001
                logger.error(f"Schedule refresh loop error: {e}")
                logger.error(traceback.format_exc())
            self._time_window_stop_event.wait(
                self._schedule_refresh_seconds)

    def _refresh_dag_schedule(self, dag_name):
        """Re-read one DAG's config from storage and apply schedule-field
        changes (start_time/end_time/duration/schedule) to the live DAG."""
        from core.dag.time_window_utils import calculate_end_time
        from core.schedule.dag_schedule import DagSchedule

        with self._lock:
            dag = self.dags.get(dag_name)
        if dag is None:
            return
        filename = dag.config.get('config_filename')
        if not filename:
            # In-memory DAG (designer-created clone etc.) - no storage
            # object to watch.
            return
        object_path = self._dag_object_path(filename)
        if not self._storage.exists(object_path):
            return
        config = self._read_dag_config(object_path)

        # Recompute the window exactly like ComputeGraph.__init__ does.
        raw_start = config.get('start_time')
        duration = config.get('duration')
        legacy_end = config.get('end_time')
        if raw_start:
            new_start = str(raw_start).replace(':', '')
            if duration is not None:
                new_end = calculate_end_time(new_start, duration)
            elif legacy_end:
                new_end = str(legacy_end).replace(':', '')
            else:
                new_end = calculate_end_time(new_start, None)
        else:
            new_start, new_end = None, None

        # Parse + validate the (possibly new) schedule block. A broken
        # edit must NOT replace a working schedule: log and keep the old.
        try:
            new_schedule = DagSchedule.from_config(config.get('schedule'))
        except Exception as e:  # noqa: BLE001
            logger.error(f"Intraday schedule edit for DAG '{dag_name}' is "
                         f"INVALID and was NOT applied: {e}")
            logger.error(traceback.format_exc())
            return

        old_schedule = getattr(dag, 'schedule', None)
        unchanged = (new_start == dag.start_time and
                     new_end == dag.end_time and
                     ((new_schedule is None and old_schedule is None) or
                      (new_schedule is not None and
                       new_schedule == old_schedule)))
        if unchanged:
            return
        dag.apply_schedule_update(new_start, new_end, new_schedule)


    def _time_window_monitor_loop(self):
        """
        Monitor DAGs that are outside their time window and start them when they enter.
        
        This runs every 60 seconds and checks:
        1. DAGs that have a time window configured
        2. Are not currently running
        3. Have now entered their time window
        
        v1.5.2: Uses self.start() to properly dispatch to worker pool
        """
        import time as time_module
        
        logger.info("Time window monitor: Starting monitoring loop")
        
        # v1.5.2: Brief initial wait to let auto-start complete first
        self._time_window_stop_event.wait(5)
        
        if self._time_window_stop_event.is_set():
            logger.info("Time window monitor: Stopped during initialization wait")
            return
        
        logger.info("Time window monitor: Beginning DAG monitoring")
        
        while not self._time_window_stop_event.is_set():
            try:
                # Collect DAGs to start (don't hold lock while starting)
                dags_to_start = []
                
                with self._lock:
                    for dag_name, dag in self.dags.items():
                        # Skip true perpetual DAGs (no window AND no
                        # day-of-week/holiday schedule - v2.1.0)
                        if (dag.start_time is None or dag.end_time is None) \
                                and getattr(dag, 'schedule', None) is None:
                            continue

                        # Skip if already running in main process
                        if dag._compute_thread and dag._compute_thread.is_alive():
                            continue

                        # v1.5.2: Skip if already assigned to a worker
                        if self._worker_pool and self._worker_pool.is_running():
                            if self._worker_pool.get_dag_assignment(dag_name) is not None:
                                continue

                        # v2.1.0: full schedule check (window + days +
                        # holiday calendars)
                        active, _reason = dag.is_within_schedule()
                        if active:
                            dags_to_start.append((dag_name, dag.start_time, dag.end_time))
                
                # Start DAGs outside the lock
                for dag_name, start_time, end_time in dags_to_start:
                    logger.info("")
                    logger.info("!" * 70)
                    logger.info("!  SCHEDULE MONITOR: DAG SCHEDULE BECAME ACTIVE")
                    logger.info(f"!  DAG: {dag_name}")
                    logger.info(f"!  Window: {start_time} - {end_time}")
                    logger.info("!  ACTION: Starting DAG automatically")
                    logger.info("!" * 70)
                    
                    try:
                        # v1.5.2: Use self.start() to properly dispatch to worker pool
                        self.start(dag_name)
                    except Exception as e:
                        logger.error(f"Time window monitor: Failed to start DAG '{dag_name}': {e}")
                            
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

    def _on_elected(self):
        """HA callback: this instance was promoted to PRIMARY.

        Resumes all suspended DAGs so the new primary takes over the
        workload. Failures resuming individual DAGs are logged with full
        stack traces and do not block the remaining DAGs.
        """
        logger.info("HA: elected as PRIMARY DAG server")
        self.is_primary = True

        # Resume all suspended DAGs
        with self._lock:
            for dag in self.dags.values():
                if dag._suspend_event.is_set():
                    try:
                        dag.resume()
                    except Exception as e:
                        logger.error(f"Error resuming DAG '{dag.name}' on "
                                     f"election: {str(e)}")
                        logger.error(traceback.format_exc())

    def _on_demoted(self):
        """HA callback: this instance lost the primary lease.

        Suspends all running DAGs so the workload moves to the new primary.
        Failures suspending individual DAGs are logged with full stack traces
        and do not block the remaining DAGs.
        """
        logger.warning("HA: DEMOTED to secondary DAG server")
        self.is_primary = False

        with self._lock:
            for dag in self.dags.values():
                try:
                    if not dag._suspend_event.is_set():
                        dag.suspend()
                except Exception as e:
                    logger.error(f"Error suspending DAG '{dag.name}' on "
                                 f"demotion: {str(e)}")
                    logger.error(traceback.format_exc())

    def _check_primary(self):
        """Check if this instance is primary"""
        if not self.is_primary:
            raise PermissionError("This instance is not primary. Operation not allowed.")

