"""
Holiday Calendar Manager (v2.1.0)
=================================

Loads named holiday calendars (JSON files) through the storage abstraction
and answers "is this date a holiday?" queries for DAG scheduling. Built for
stock-trading schedules: a DAG may follow one or MORE calendars, and a date
that is a holiday in ANY followed calendar counts as a holiday.

Calendar file format (e.g. config/holidays/usa.json):

    {
        "name": "USA",
        "description": "...",
        "holidays": {
            "2026-01-01": "New Year's Day",
            "2026-11-26": "Thanksgiving Day",
            ...
        }
    }

Intraday updates: calendars are cached with a TTL (property
``holidays.reload_seconds``, default 180). Edited or newly added calendar
files are therefore picked up automatically well within the 5-minute
reaction guarantee (180s cache age + the 60s scheduler loop < 5 min).

Fail-fast philosophy: a DAG that references a calendar which cannot be
found raises ScheduleConfigError at validation time. At runtime, a reload
failure logs a full stack trace and falls back to the last good copy so a
transient storage outage never silently flips trading DAGs on.

Properties:
    holidays.prefix          Logical storage prefix (default config/holidays)
    holidays.reload_seconds  Cache TTL in seconds (default 180, max 300)

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import threading
import time
import traceback
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = 'config/holidays'
DEFAULT_RELOAD_SECONDS = 180
MAX_RELOAD_SECONDS = 300  # hard cap: guarantees <5-min reaction


class ScheduleConfigError(Exception):
    """Invalid schedule / holiday-calendar configuration."""


class _CachedCalendar:
    """One loaded calendar plus its load timestamp."""

    __slots__ = ('name', 'holidays', 'description', 'loaded_at')

    def __init__(self, name: str, holidays: Dict[str, str],
                 description: str, loaded_at: float):
        self.name = name
        self.holidays = holidays
        self.description = description
        self.loaded_at = loaded_at


class HolidayCalendarManager:
    """
    Thread-safe, storage-backed holiday calendar registry with TTL reload.

    Singleton by default (``get_instance``); independently constructible
    for tests.
    """

    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self, props=None, storage=None):
        if props is None:
            from core.properties_configurator import PropertiesConfigurator
            props = PropertiesConfigurator()
        if storage is None:
            from core.storage import get_storage_provider
            storage = get_storage_provider()
        self._storage = storage
        self.prefix = props.get('holidays.prefix', DEFAULT_PREFIX)
        reload_seconds = int(props.get('holidays.reload_seconds',
                                       DEFAULT_RELOAD_SECONDS))
        if reload_seconds > MAX_RELOAD_SECONDS:
            logger.warning(
                "holidays.reload_seconds=%d exceeds the %ds cap required "
                "for the 5-minute intraday-update guarantee - capping.",
                reload_seconds, MAX_RELOAD_SECONDS)
            reload_seconds = MAX_RELOAD_SECONDS
        self.reload_seconds = max(1, reload_seconds)
        self._cache: Dict[str, _CachedCalendar] = {}
        self._lock = threading.RLock()
        logger.info("HolidayCalendarManager initialized "
                    "(prefix=%s reload=%ss)", self.prefix,
                    self.reload_seconds)

    # ------------------------------------------------------------------ #
    # Singleton plumbing
    # ------------------------------------------------------------------ #

    @classmethod
    def get_instance(cls) -> "HolidayCalendarManager":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._instance_lock:
            cls._instance = None

    # ------------------------------------------------------------------ #
    # Loading
    # ------------------------------------------------------------------ #

    def _object_path(self, name: str) -> str:
        return f"{self.prefix}/{name.strip().lower()}.json"

    def _load_calendar(self, name: str) -> _CachedCalendar:
        """Read and parse one calendar file (raises on any problem)."""
        path = self._object_path(name)
        raw = self._storage.read_text(path)
        data = json.loads(raw)
        holidays = data.get('holidays')
        if not isinstance(holidays, dict):
            raise ScheduleConfigError(
                f"Holiday calendar '{name}' ({path}) is malformed: a "
                f"'holidays' object mapping YYYY-MM-DD dates to names is "
                f"required.")
        # Validate date keys eagerly - a typo must fail loudly, not match
        # nothing silently.
        for key in holidays:
            try:
                date.fromisoformat(key)
            except ValueError as exc:
                raise ScheduleConfigError(
                    f"Holiday calendar '{name}' ({path}) contains an "
                    f"invalid date key '{key}': {exc}") from exc
        return _CachedCalendar(
            name=str(data.get('name', name)).upper(),
            holidays=holidays,
            description=data.get('description', ''),
            loaded_at=time.time())

    def get_calendar(self, name: str) -> _CachedCalendar:
        """
        Return the named calendar, reloading it when the cache entry is
        older than ``reload_seconds`` (this is what makes intraday edits
        take effect automatically).

        Raises:
            ScheduleConfigError: when the calendar has never been loadable.
        """
        key = name.strip().lower()
        with self._lock:
            cached = self._cache.get(key)
            fresh_enough = (cached is not None and
                            time.time() - cached.loaded_at
                            < self.reload_seconds)
            if fresh_enough:
                return cached
            try:
                loaded = self._load_calendar(key)
                if cached is not None and \
                        loaded.holidays != cached.holidays:
                    logger.warning(
                        "Holiday calendar '%s' changed intraday: "
                        "%d -> %d entries - new version now in effect.",
                        key, len(cached.holidays), len(loaded.holidays))
                self._cache[key] = loaded
                return loaded
            except Exception as exc:  # noqa: BLE001
                if cached is not None:
                    # Transient reload failure: keep trading on the last
                    # good copy, but be loud about it.
                    logger.error(
                        "Reload of holiday calendar '%s' failed - "
                        "continuing with the copy loaded at %s: %s",
                        key, datetime.fromtimestamp(
                            cached.loaded_at).isoformat(), exc)
                    logger.error(traceback.format_exc())
                    cached.loaded_at = time.time()  # back off retries
                    return cached
                logger.error("Holiday calendar '%s' could not be loaded "
                             "from '%s': %s", key,
                             self._object_path(key), exc)
                logger.error(traceback.format_exc())
                raise ScheduleConfigError(
                    f"Holiday calendar '{name}' could not be loaded from "
                    f"'{self._object_path(key)}': {exc}. Add the calendar "
                    f"file or correct the DAG's 'holiday_calendars' "
                    f"configuration.") from exc

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def validate_calendars(self, names: List[str]) -> None:
        """Eagerly load every named calendar; raise on any failure.
        Called when a DAG's schedule is (re)configured - fail fast."""
        for name in names:
            self.get_calendar(name)

    def is_holiday(self, when, calendar_names: List[str]
                   ) -> Tuple[bool, Optional[str]]:
        """
        True when *when* is a holiday in ANY of the named calendars
        (union semantics - a trading DAG following USA and CANADA stays
        down whenever either market is closed).

        Args:
            when: date or datetime to test.
            calendar_names: calendar names, case-insensitive.

        Returns:
            (is_holiday, "CALENDAR: Holiday Name" or None)
        """
        if not calendar_names:
            return False, None
        if isinstance(when, datetime):
            when = when.date()
        key = when.isoformat()
        for name in calendar_names:
            calendar = self.get_calendar(name)
            holiday_name = calendar.holidays.get(key)
            if holiday_name is not None:
                return True, f"{calendar.name}: {holiday_name}"
        return False, None

    def list_calendars(self) -> List[str]:
        """Names of calendar files available under the prefix."""
        names = self._storage.list_names(prefix=self.prefix,
                                         suffix='.json')
        base = f"{self.prefix}/"
        return sorted(n[len(base):-len('.json')] for n in names
                      if '/' not in n[len(base):])
