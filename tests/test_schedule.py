"""
Tests for v2.1.0 DAG scheduling: day-of-week rules, holiday calendars
(USA + CANADA baked-in files), multi-calendar union semantics, and the
intraday-update reaction guarantee.
"""
import json
import time
from datetime import date, datetime

import pytest

from core.schedule.dag_schedule import DagSchedule, is_schedule_active
from core.schedule.holiday_calendar import (
    HolidayCalendarManager,
    ScheduleConfigError,
)
from core.storage.filesystem_storage import FileSystemStorageProvider


class _Props:
    def __init__(self, mapping):
        self._mapping = mapping

    def get(self, key, default=None):
        return self._mapping.get(key, default)


@pytest.fixture()
def manager(tmp_path):
    """Manager over an isolated filesystem store seeded with a tiny
    calendar plus the real baked-in USA/CANADA files."""
    storage = FileSystemStorageProvider(root=str(tmp_path))
    storage.write_text('holidays/test.json', json.dumps({
        'name': 'TEST',
        'holidays': {'2026-06-15': 'Test Holiday'}
    }))
    for name in ('usa', 'canada'):
        storage.write_text(
            f'holidays/{name}.json',
            open(f'config/holidays/{name}.json').read())
    return HolidayCalendarManager(
        props=_Props({'holidays.prefix': 'holidays',
                      'holidays.reload_seconds': 1}),
        storage=storage)


# ---------------------------------------------------------------------- #
# Baked-in calendars
# ---------------------------------------------------------------------- #

def test_baked_calendars_cover_15_years():
    for name in ('usa', 'canada'):
        cal = json.load(open(f'config/holidays/{name}.json'))
        years = {d[:4] for d in cal['holidays']}
        assert {str(y) for y in range(2026, 2041)} <= years
        assert len(cal['holidays']) == 150  # 10 holidays x 15 years


def test_well_known_us_holidays(manager):
    known = {
        date(2026, 1, 1): "New Year's Day",
        date(2026, 4, 3): 'Good Friday',
        date(2026, 11, 26): 'Thanksgiving Day',
        date(2026, 12, 25): 'Christmas Day',
        date(2026, 7, 3): 'Independence Day',  # Jul 4 2026 = Sat -> Fri
        date(2040, 11, 22): 'Thanksgiving Day',  # last covered year
    }
    for day, name in known.items():
        holiday, label = manager.is_holiday(day, ['USA'])
        assert holiday and name in label, f'{day}: {label}'


def test_well_known_canadian_holidays(manager):
    known = {
        date(2026, 5, 18): 'Victoria Day',
        date(2026, 7, 1): 'Canada Day',
        date(2026, 10, 12): 'Thanksgiving Day (Canada)',
        date(2026, 12, 28): 'Boxing Day',  # Dec 26 2026 = Sat -> Mon
    }
    for day, name in known.items():
        holiday, label = manager.is_holiday(day, ['CANADA'])
        assert holiday and name in label, f'{day}: {label}'


def test_multi_calendar_union(manager):
    """A DAG following USA + CANADA is down when EITHER market closes."""
    cals = ['USA', 'CANADA']
    # Canadian-only holiday
    assert manager.is_holiday(date(2026, 10, 12), cals)[0]
    assert not manager.is_holiday(date(2026, 10, 12), ['USA'])[0]
    # US-only holiday
    assert manager.is_holiday(date(2026, 11, 26), cals)[0]
    assert not manager.is_holiday(date(2026, 11, 26), ['CANADA'])[0]
    # Ordinary trading day
    assert not manager.is_holiday(date(2026, 6, 12), cals)[0]


def test_calendar_names_case_insensitive(manager):
    assert manager.is_holiday(date(2026, 6, 15), ['Test'])[0]
    assert manager.is_holiday(date(2026, 6, 15), ['TEST'])[0]


def test_missing_calendar_fails_fast(manager):
    with pytest.raises(ScheduleConfigError):
        manager.is_holiday(date(2026, 6, 15), ['atlantis'])
    with pytest.raises(ScheduleConfigError):
        DagSchedule.from_config({'holiday_calendars': ['atlantis']},
                                manager=manager)


# ---------------------------------------------------------------------- #
# Day-of-week + combined evaluation
# ---------------------------------------------------------------------- #

def _sched(cfg, manager):
    return DagSchedule.from_config(cfg, manager=manager)


def test_days_of_week_allowlist(manager):
    s = _sched({'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri']},
               manager)
    assert s.is_active(datetime(2026, 6, 12, 10, 0))[0]       # Friday
    assert not s.is_active(datetime(2026, 6, 13, 10, 0))[0]   # Saturday
    assert not s.is_active(datetime(2026, 6, 14, 10, 0))[0]   # Sunday


def test_exclude_days_denylist(manager):
    s = _sched({'exclude_days_of_week': ['Wednesday']}, manager)
    assert s.is_active(datetime(2026, 6, 9, 10, 0))[0]        # Tuesday
    assert not s.is_active(datetime(2026, 6, 10, 10, 0))[0]   # Wednesday


def test_full_day_names_accepted(manager):
    s = _sched({'days_of_week': ['Monday', 'FRIDAY']}, manager)
    assert s.is_active(datetime(2026, 6, 8, 9, 0))[0]
    assert s.is_active(datetime(2026, 6, 12, 9, 0))[0]
    assert not s.is_active(datetime(2026, 6, 9, 9, 0))[0]


def test_invalid_schedule_rejected(manager):
    with pytest.raises(ScheduleConfigError):
        _sched({'days_of_week': ['funday']}, manager)
    with pytest.raises(ScheduleConfigError):
        _sched({'days_of_week': 'mon'}, manager)
    with pytest.raises(ScheduleConfigError):
        _sched({'typo_key': []}, manager)


def test_empty_schedule_is_none(manager):
    assert _sched(None, manager) is None
    assert _sched({}, manager) is None
    assert _sched({'holiday_calendars': []}, manager) is None


def test_combined_trading_schedule(manager):
    """The headline use case: NYSE+TSX hours, weekdays, both calendars."""
    s = _sched({'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri'],
                'holiday_calendars': ['USA', 'CANADA']}, manager)
    check = lambda dt: is_schedule_active('0930', '1600', s, dt, manager)

    assert check(datetime(2026, 6, 12, 10, 0))[0]              # Fri, open
    assert not check(datetime(2026, 6, 12, 17, 0))[0]          # after close
    assert not check(datetime(2026, 6, 13, 10, 0))[0]          # Saturday
    assert not check(datetime(2026, 11, 26, 10, 0))[0]         # Thanksgiving
    assert not check(datetime(2026, 10, 12, 10, 0))[0]         # CA Thanksgiving
    ok, reason = check(datetime(2026, 4, 3, 10, 0))            # Good Friday
    assert not ok and 'Good Friday' in reason


def test_schedule_without_time_window(manager):
    """Day/holiday rules work for DAGs with no time window at all."""
    s = _sched({'holiday_calendars': ['USA']}, manager)
    assert is_schedule_active(None, None, s,
                              datetime(2026, 6, 12, 3, 0), manager)[0]
    assert not is_schedule_active(None, None, s,
                                  datetime(2026, 12, 25, 3, 0), manager)[0]


# ---------------------------------------------------------------------- #
# Intraday updates (<5-minute reaction)
# ---------------------------------------------------------------------- #

def test_intraday_calendar_edit_takes_effect(tmp_path):
    """Editing a calendar file is picked up after the TTL elapses."""
    storage = FileSystemStorageProvider(root=str(tmp_path))
    storage.write_text('holidays/intra.json', json.dumps({
        'name': 'INTRA', 'holidays': {}}))
    mgr = HolidayCalendarManager(
        props=_Props({'holidays.prefix': 'holidays',
                      'holidays.reload_seconds': 1}),
        storage=storage)

    today = date(2026, 6, 12)
    assert not mgr.is_holiday(today, ['INTRA'])[0]

    # Intraday edit: declare an ad-hoc market closure.
    storage.write_text('holidays/intra.json', json.dumps({
        'name': 'INTRA',
        'holidays': {'2026-06-12': 'Emergency Market Closure'}}))

    # Within the TTL the cached copy is still served...
    assert not mgr.is_holiday(today, ['INTRA'])[0]
    # ...and after it elapses the new version is in effect.
    time.sleep(1.1)
    holiday, label = mgr.is_holiday(today, ['INTRA'])
    assert holiday and 'Emergency Market Closure' in label


def test_reload_failure_keeps_last_good_copy(tmp_path):
    storage = FileSystemStorageProvider(root=str(tmp_path))
    storage.write_text('holidays/frail.json', json.dumps({
        'name': 'FRAIL', 'holidays': {'2026-06-12': 'Holiday'}}))
    mgr = HolidayCalendarManager(
        props=_Props({'holidays.prefix': 'holidays',
                      'holidays.reload_seconds': 1}),
        storage=storage)
    assert mgr.is_holiday(date(2026, 6, 12), ['FRAIL'])[0]

    # Corrupt the file, let the TTL elapse: last good copy keeps serving.
    storage.write_text('holidays/frail.json', '{not json')
    time.sleep(1.1)
    assert mgr.is_holiday(date(2026, 6, 12), ['FRAIL'])[0]


def test_ttl_capped_at_five_minutes(tmp_path):
    storage = FileSystemStorageProvider(root=str(tmp_path))
    mgr = HolidayCalendarManager(
        props=_Props({'holidays.prefix': 'holidays',
                      'holidays.reload_seconds': 9999}),
        storage=storage)
    assert mgr.reload_seconds <= 300


def test_apply_schedule_update_changes_live_evaluation(manager):
    """ComputeGraph.apply_schedule_update swaps rules in place - this is
    what the DAG server's refresh loop calls on an intraday config edit."""
    from core.dag.compute_graph_support import TimeWindowMixin

    class _FakeDag(TimeWindowMixin):
        name = 'fake'
        start_time = None
        end_time = None
        schedule = None

    dag = _FakeDag()
    friday = datetime(2026, 6, 12, 10, 0)
    assert dag.is_within_schedule(friday)[0]  # perpetual

    new_sched = DagSchedule.from_config(
        {'exclude_days_of_week': ['fri']}, manager=manager)
    dag.apply_schedule_update('0900', '1700', new_sched)
    active, reason = dag.is_within_schedule(friday)
    assert not active and 'excluded' in reason.lower()
    assert dag.is_within_schedule(datetime(2026, 6, 11, 10, 0))[0]  # Thu


def test_server_refresh_applies_intraday_dag_edit(tmp_path, manager):
    """End-to-end of the DAG server's refresh path: an intraday edit of a
    DAG's schedule block in storage is detected and applied to the live
    DAG object via apply_schedule_update."""
    import threading
    from core.dag.compute_graph_support import TimeWindowMixin
    from core.dag.dag_server_support import DAGLoaderMixin, DAGMonitorMixin

    storage = FileSystemStorageProvider(root=str(tmp_path))
    original = {
        'name': 'intraday_dag',
        'config_filename': 'intraday_dag.json',
        'start_time': '0930', 'duration': '6h30m',
        'schedule': {'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri']},
    }
    storage.write_text('dags/intraday_dag.json', json.dumps(original))

    class _FakeDag(TimeWindowMixin):
        name = 'intraday_dag'
        start_time = '0930'
        end_time = '1600'
        schedule = DagSchedule.from_config(original['schedule'],
                                           manager=manager)
        config = original

    class _Props:
        @staticmethod
        def resolve_string_json_content(raw):
            return json.loads(raw)

    class _FakeServer(DAGLoaderMixin, DAGMonitorMixin):
        def __init__(self):
            self._storage = storage
            self._props = _Props()
            self.dag_config_prefix = 'dags'
            self._lock = threading.RLock()
            self.dags = {'intraday_dag': _FakeDag()}

    server = _FakeServer()
    dag = server.dags['intraday_dag']

    # No edit -> no change applied.
    server._refresh_dag_schedule('intraday_dag')
    assert dag.start_time == '0930' and dag.end_time == '1600'

    # Intraday edit: shorten the session and add holiday calendars.
    edited = dict(original)
    edited['start_time'] = '0930'
    edited['duration'] = '3h'
    edited['schedule'] = {'days_of_week': ['mon', 'tue', 'wed', 'thu',
                                           'fri'],
                          'holiday_calendars': []}
    storage.write_text('dags/intraday_dag.json', json.dumps(edited))

    server._refresh_dag_schedule('intraday_dag')
    assert dag.end_time == '1230'  # 0930 + 3h, applied live

    # A BROKEN intraday edit must be rejected, keeping the last good rules.
    broken = dict(edited)
    broken['schedule'] = {'days_of_week': ['funday']}
    storage.write_text('dags/intraday_dag.json', json.dumps(broken))
    server._refresh_dag_schedule('intraday_dag')
    assert dag.schedule is not None
    assert dag.schedule.days_of_week == {0, 1, 2, 3, 4}  # unchanged


# --------------------------------------------------------------- timezone (v2.2)

def test_schedule_defaults_to_eastern(manager):
    """Market schedules evaluate in US/Eastern by default, regardless of the
    server's own timezone."""
    sched = DagSchedule.from_config(
        {'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri']}, manager=manager)
    assert sched.timezone_name == 'America/New_York'


def test_window_evaluated_in_schedule_timezone(manager):
    """A 0930-1600 Eastern window must be active at 10:25 Eastern even when
    that instant is 14:25 UTC (the bug: window was compared against UTC)."""
    from zoneinfo import ZoneInfo
    sched = DagSchedule.from_config(
        {'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri'],
         'timezone': 'America/New_York'}, manager=manager)
    # Friday 2026-06-12 10:25 Eastern == 14:25 UTC
    et = datetime(2026, 6, 12, 10, 25, tzinfo=ZoneInfo('America/New_York'))
    active, reason = is_schedule_active('0930', '1600', sched, now=et,
                                        manager=manager)
    assert active, reason
    # The same instant expressed in UTC must give the same answer.
    utc = et.astimezone(ZoneInfo('UTC'))
    active_utc, _ = is_schedule_active('0930', '1600', sched, now=utc,
                                       manager=manager)
    assert active_utc


def test_window_before_open_in_eastern(manager):
    """09:00 Eastern is before a 0930 open and must be inactive, even though
    the naive UTC equivalent (13:00) would be 'inside' a naive comparison."""
    from zoneinfo import ZoneInfo
    sched = DagSchedule.from_config(
        {'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri']}, manager=manager)
    et = datetime(2026, 6, 12, 9, 0, tzinfo=ZoneInfo('America/New_York'))
    active, reason = is_schedule_active('0930', '1600', sched, now=et,
                                        manager=manager)
    assert not active
    assert 'time window' in reason


def test_explicit_non_eastern_timezone(manager):
    """An explicit timezone overrides the Eastern default."""
    from zoneinfo import ZoneInfo
    sched = DagSchedule.from_config(
        {'days_of_week': ['mon', 'tue', 'wed', 'thu', 'fri'],
         'timezone': 'Asia/Kolkata'}, manager=manager)
    assert sched.timezone_name == 'Asia/Kolkata'
    # 12:00 IST on a Friday, inside a 0930-1600 IST window.
    ist = datetime(2026, 6, 12, 12, 0, tzinfo=ZoneInfo('Asia/Kolkata'))
    active, _ = is_schedule_active('0930', '1600', sched, now=ist,
                                   manager=manager)
    assert active


def test_invalid_timezone_fails_fast(manager):
    with pytest.raises(ScheduleConfigError):
        DagSchedule.from_config({'timezone': 'Mars/Olympus_Mons'},
                                manager=manager)


# ----------------------------------------------------- reason classifier (v2.2)

def test_classify_schedule_reason_kinds():
    from core.schedule.dag_schedule import classify_schedule_reason
    assert classify_schedule_reason('scheduled') == 'active'
    assert classify_schedule_reason(
        'Sat is not a scheduled day (runs Mon,Tue,Wed,Thu,Fri)') == 'day'
    assert classify_schedule_reason(
        'Sun is excluded from the schedule') == 'excluded_day'
    assert classify_schedule_reason(
        'market holiday (USA: Independence Day)') == 'holiday'
    assert classify_schedule_reason(
        'outside the daily time window 09:30-16:00 America/New_York '
        '(now 08:00)') == 'time_window'
    assert classify_schedule_reason('') == 'active'


def test_apply_schedule_update_refreshes_duration():
    """v2.2 BUGFIX: editing a DAG's window intraday must also refresh
    self.duration, not just end_time. Previously a new 1100/01h00m window
    kept the old duration and displayed as 11:00-17:30 (start + stale
    6h30m) even though it correctly ran 11:00-12:00."""
    from core.dag.compute_graph_support import TimeWindowMixin
    from core.dag.time_window_utils import (calculate_end_time,
                                             get_time_window_info)

    class _FakeDag(TimeWindowMixin):
        name = 'fake'
        start_time = '0930'
        end_time = '1600'
        duration = '6h30m'
        schedule = None

    dag = _FakeDag()
    # Simulate the refresh recomputing for the new 1100 / 01h00m config.
    new_end = calculate_end_time('1100', '01h00m')
    assert new_end == '1200'
    dag.apply_schedule_update('1100', new_end, None, duration='01h00m')

    assert dag.start_time == '1100'
    assert dag.end_time == '1200'
    assert dag.duration == '01h00m'   # the bug: this stayed '6h30m'
    info = get_time_window_info(dag.start_time, dag.duration)
    assert info['display_end'] == '12:00'


def test_apply_schedule_update_duration_defaults_preserved():
    """Omitting duration keeps the existing value (backward compatible)."""
    from core.dag.compute_graph_support import TimeWindowMixin

    class _FakeDag(TimeWindowMixin):
        name = 'fake'
        start_time = '0930'
        end_time = '1600'
        duration = '6h30m'
        schedule = None

    dag = _FakeDag()
    dag.apply_schedule_update('1000', '1100', None)  # no duration arg
    assert dag.duration == '6h30m'   # unchanged
