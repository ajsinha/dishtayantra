"""
DAG Schedule Evaluation (v2.1.0)
================================

Optional day-of-week and holiday-calendar scheduling for DAGs, layered on
top of the existing time-window mechanism. Built for stock-trading
schedules and fully optional - DAGs without a ``schedule`` block behave
exactly as before.

DAG configuration (all keys optional):

    {
        "name": "us_equities_ingest",
        "start_time": "0930",
        "end_time": "1600",
        "schedule": {
            "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
            "exclude_days_of_week": [],
            "holiday_calendars": ["USA", "CANADA"]
        },
        ...
    }

Semantics (a DAG is *active* only when ALL of these hold):
    1. The time window matches (wrap-around-aware, see time_window_utils) -
       or no window is configured.
    2. ``days_of_week``: when present, today must be in the list
       (allowlist). Accepts mon/tue/... or full names, case-insensitive.
    3. ``exclude_days_of_week``: today must NOT be in the list (denylist,
       applied after the allowlist).
    4. ``holiday_calendars``: today must not be a holiday in ANY listed
       calendar (union semantics - following USA and CANADA keeps the DAG
       down whenever either market is closed).

Intraday updates: the holiday manager reloads calendar files on a <=180s
TTL and the DAG server re-reads each DAG's schedule from storage every
<=240s, so edits to either take effect automatically within 5 minutes.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from core.schedule.holiday_calendar import (
    HolidayCalendarManager,
    ScheduleConfigError,
)

# NOTE: core.dag.time_window_utils is imported lazily inside
# is_schedule_active() - importing it at module level triggers the heavy
# core.dag package __init__, whose compute_graph imports THIS module
# (circular import).

logger = logging.getLogger(__name__)

# v2.2: market schedules are wall-clock in a market's local zone. Default to
# US/Eastern so a 0930-1600 window with USA/CANADA holiday calendars behaves
# correctly regardless of the server's own timezone (often UTC).
DEFAULT_SCHEDULE_TIMEZONE = "America/New_York"


def _fmt_hhmm(t):
    """Format an HHMM or HH:MM string as HH:MM for display; pass through
    anything unexpected unchanged."""
    if not t:
        return str(t)
    s = str(t).replace(':', '')
    if len(s) == 4 and s.isdigit():
        return f"{s[:2]}:{s[2:]}"
    return str(t)


def _resolve_timezone(name):
    """Resolve an IANA timezone name to a tzinfo, or None to fall back to
    server-local time. Fails fast on an unknown name (a trading DAG must not
    silently run in the wrong timezone)."""
    if not name:
        return None
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception as exc:  # noqa: BLE001
        raise ScheduleConfigError(
            f"schedule.timezone '{name}' is not a valid IANA timezone "
            f"(e.g. 'America/New_York', 'UTC'): {exc}")


_DAY_TOKENS = {
    'mon': 0, 'monday': 0,
    'tue': 1, 'tues': 1, 'tuesday': 1,
    'wed': 2, 'wednesday': 2,
    'thu': 3, 'thur': 3, 'thurs': 3, 'thursday': 3,
    'fri': 4, 'friday': 4,
    'sat': 5, 'saturday': 5,
    'sun': 6, 'sunday': 6,
}
_DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def _parse_days(tokens, field_name: str) -> Optional[set]:
    """Parse a list of day tokens to a set of weekday ints (Mon=0)."""
    if tokens is None:
        return None
    if not isinstance(tokens, (list, tuple)):
        raise ScheduleConfigError(
            f"schedule.{field_name} must be a list of day names, got "
            f"{type(tokens).__name__}")
    days = set()
    for token in tokens:
        key = str(token).strip().lower()
        if key not in _DAY_TOKENS:
            raise ScheduleConfigError(
                f"schedule.{field_name} contains an unknown day "
                f"'{token}'. Use mon/tue/wed/thu/fri/sat/sun or full "
                f"names.")
        days.add(_DAY_TOKENS[key])
    return days


class DagSchedule:
    """
    Parsed, validated schedule for one DAG.

    Construct with :meth:`from_config`; evaluate with :meth:`is_active`.
    """

    def __init__(self, days_of_week: Optional[set],
                 exclude_days_of_week: Optional[set],
                 holiday_calendars: List[str],
                 raw: Dict[str, Any],
                 timezone_name: Optional[str] = None):
        self.days_of_week = days_of_week
        self.exclude_days_of_week = exclude_days_of_week
        self.holiday_calendars = holiday_calendars
        self.raw = raw
        # v2.2: schedules are evaluated in this timezone. Market schedules
        # (start/end times + holiday calendars) are inherently wall-clock in
        # a market's local zone, so we default to US/Eastern rather than the
        # server's local time (which is often UTC and would shift the whole
        # window by several hours).
        self.timezone_name = timezone_name or DEFAULT_SCHEDULE_TIMEZONE
        self._tzinfo = _resolve_timezone(self.timezone_name)

    def now_in_zone(self, now: datetime = None) -> datetime:
        """Return *now* as a naive wall-clock datetime in this schedule's
        timezone. Accepts an optional reference datetime (aware or naive)
        for testing; a naive value is assumed to already be in this zone."""
        if now is not None:
            if now.tzinfo is None:
                return now  # caller supplied an explicit wall-clock time
            return now.astimezone(self._tzinfo).replace(tzinfo=None)
        if self._tzinfo is None:
            return datetime.now()
        return datetime.now(self._tzinfo).replace(tzinfo=None)

    # ------------------------------------------------------------------ #

    @classmethod
    def from_config(cls, schedule_cfg: Optional[Dict[str, Any]],
                    manager: HolidayCalendarManager = None,
                    validate_calendars: bool = True
                    ) -> Optional["DagSchedule"]:
        """
        Parse a DAG's ``schedule`` block. Returns None when absent/empty
        (DAG behaves exactly as pre-v2.1). Raises ScheduleConfigError on
        malformed input or, when ``validate_calendars`` is True, on a
        referenced calendar that cannot be loaded (fail fast - a trading
        DAG must not start with a broken holiday configuration).
        """
        if not schedule_cfg:
            return None
        if not isinstance(schedule_cfg, dict):
            raise ScheduleConfigError(
                f"'schedule' must be an object, got "
                f"{type(schedule_cfg).__name__}")

        known = {'days_of_week', 'exclude_days_of_week',
                 'holiday_calendars', 'timezone'}
        unknown = set(schedule_cfg) - known
        if unknown:
            raise ScheduleConfigError(
                f"'schedule' contains unknown key(s) {sorted(unknown)}. "
                f"Supported keys: {sorted(known)}")

        days = _parse_days(schedule_cfg.get('days_of_week'),
                           'days_of_week')
        excluded = _parse_days(schedule_cfg.get('exclude_days_of_week'),
                               'exclude_days_of_week')

        calendars = schedule_cfg.get('holiday_calendars') or []
        if not isinstance(calendars, (list, tuple)):
            raise ScheduleConfigError(
                "schedule.holiday_calendars must be a list of calendar "
                "names (a DAG may follow more than one).")
        calendars = [str(c).strip() for c in calendars if str(c).strip()]

        tz_name = schedule_cfg.get('timezone')
        if tz_name is not None and not str(tz_name).strip():
            tz_name = None
        if tz_name is not None:
            tz_name = str(tz_name).strip()
            _resolve_timezone(tz_name)  # validate now (fail fast)

        if days is None and excluded is None and not calendars \
                and tz_name is None:
            return None

        if calendars and validate_calendars:
            (manager or HolidayCalendarManager.get_instance()) \
                .validate_calendars(calendars)

        return cls(days, excluded, calendars, dict(schedule_cfg),
                   timezone_name=tz_name)

    # ------------------------------------------------------------------ #

    def is_active(self, now: datetime = None,
                  manager: HolidayCalendarManager = None
                  ) -> Tuple[bool, str]:
        """
        Evaluate day-of-week + holiday rules (NOT the time window - that
        is combined by the caller, see ``is_schedule_active``).

        Returns:
            (active, reason) - reason explains a False result, or is
            'scheduled' when active.
        """
        if now is None or now.tzinfo is not None:
            now = self.now_in_zone(now)
        weekday = now.weekday()
        day_name = _DAY_NAMES[weekday]

        if self.days_of_week is not None and \
                weekday not in self.days_of_week:
            return False, (f"{day_name} is not a scheduled day "
                           f"(runs {self.describe_days(self.days_of_week)})")
        if self.exclude_days_of_week is not None and \
                weekday in self.exclude_days_of_week:
            return False, (f"{day_name} is excluded from the schedule")
        if self.holiday_calendars:
            mgr = manager or HolidayCalendarManager.get_instance()
            holiday, label = mgr.is_holiday(now, self.holiday_calendars)
            if holiday:
                return False, f"market holiday ({label})"
        return True, 'scheduled'

    @staticmethod
    def describe_days(days: set) -> str:
        return ','.join(_DAY_NAMES[d] for d in sorted(days))

    def to_dict(self) -> Dict[str, Any]:
        """JSON-friendly representation for details()/UI."""
        return {
            'days_of_week': self.describe_days(self.days_of_week)
            if self.days_of_week is not None else None,
            'exclude_days_of_week':
                self.describe_days(self.exclude_days_of_week)
                if self.exclude_days_of_week is not None else None,
            'holiday_calendars': list(self.holiday_calendars),
            'timezone': self.timezone_name,
        }

    def __eq__(self, other):
        if other is None or not isinstance(other, DagSchedule):
            return False
        return (self.days_of_week == other.days_of_week and
                self.exclude_days_of_week == other.exclude_days_of_week and
                self.holiday_calendars == other.holiday_calendars)


def is_schedule_active(start_time, end_time,
                       schedule: Optional[DagSchedule],
                       now: datetime = None,
                       manager: HolidayCalendarManager = None
                       ) -> Tuple[bool, str]:
    """
    Combined activity check: time window (wrap-around-aware) AND
    day-of-week AND holiday calendars. The single entry point used by the
    ComputeGraph monitor thread and the DAG server.

    Returns:
        (active, reason)
    """
    from core.dag.time_window_utils import is_within_time_window
    # v2.2: evaluate the whole schedule (time window AND day/holiday rules) in
    # the schedule's timezone. Market windows like 0930-1600 are wall-clock in
    # the market's zone; without this the window is compared against the
    # server's local time (often UTC) and shifts by several hours.
    if schedule is not None:
        eval_now = schedule.now_in_zone(now)
    elif now is None:
        eval_now = datetime.now()
    elif now.tzinfo is not None:
        eval_now = now.astimezone().replace(tzinfo=None)
    else:
        eval_now = now

    if not is_within_time_window(start_time, end_time, eval_now):
        tz_label = f" {schedule.timezone_name}" if schedule is not None \
            and getattr(schedule, 'timezone_name', None) else ""
        return False, (f"outside the daily time window "
                       f"{_fmt_hhmm(start_time)}-{_fmt_hhmm(end_time)}"
                       f"{tz_label} (now {eval_now.strftime('%H:%M')})")
    if schedule is not None:
        return schedule.is_active(eval_now, manager)
    return True, 'scheduled'


def classify_schedule_reason(reason: str) -> str:
    """Classify a schedule reason string into a stable 'kind' for the UI.

    Returns one of: 'active', 'time_window', 'day', 'excluded_day',
    'holiday', or 'other'. Keyed off stable phrases produced by
    is_schedule_active / DagSchedule.is_active so the dashboard can choose an
    icon and emphasis without re-deriving the cause.
    """
    if not reason or reason == 'scheduled':
        return 'active'
    r = reason.lower()
    if 'time window' in r:
        return 'time_window'
    if 'market holiday' in r or r.startswith('holiday'):
        return 'holiday'
    if 'excluded from the schedule' in r:
        return 'excluded_day'
    if 'not a scheduled day' in r or 'days_of_week' in r:
        return 'day'
    return 'other'
