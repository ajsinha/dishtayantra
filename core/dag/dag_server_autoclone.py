"""
DAG Server Auto-Clone Mixin (v2.0.0)
====================================

Carries the auto-clone feature for
:class:`core.dag.dag_server.DAGComputeServer`: queue-depth-triggered clone
creation, the background manager loop, idle-clone cleanup, and status
reporting. Split out of dag_server.py so each module stays within the
500-line architecture limit. All state lives on DAGComputeServer.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading
import traceback
from datetime import datetime

from core.dag.time_window_utils import calculate_end_time, parse_duration

logger = logging.getLogger(__name__)


class DAGAutoCloneMixin:
    """Queue-depth-driven automatic DAG cloning."""

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

                # current_time is resolved per-DAG below in the schedule's
                # timezone (autoclone ramp times are wall-clock like the
                # market schedule, so a UTC server must not shift them).

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

                        # Evaluate ramp times in the schedule's timezone
                        # (default US/Eastern) so they behave as wall-clock
                        # regardless of the server's own zone. An explicit
                        # autoclone.timezone overrides; otherwise inherit the
                        # DAG schedule's timezone.
                        from core.schedule.dag_schedule import (
                            DEFAULT_SCHEDULE_TIMEZONE, _resolve_timezone)
                        tz_name = autoclone_config.get('timezone')
                        if not tz_name:
                            sched = getattr(dag, 'schedule', None)
                            tz_name = getattr(sched, 'timezone_name', None) \
                                or DEFAULT_SCHEDULE_TIMEZONE
                        tzinfo = _resolve_timezone(tz_name)
                        if tzinfo is not None:
                            current_time = datetime.now(tzinfo).strftime('%H%M')
                        else:
                            current_time = datetime.now().strftime('%H%M')

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

