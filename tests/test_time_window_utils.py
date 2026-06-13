"""Tests for time window utilities, including the v2.0.0 midnight
wrap-around fix (legacy integer HHMM comparison could never match an
overnight window like 2200-0600)."""
from datetime import datetime

import pytest

from core.dag.time_window_utils import (
    calculate_end_time,
    hhmm_to_minutes,
    is_within_time_window,
    parse_duration,
)


def test_parse_duration():
    assert parse_duration('1h') == 60
    assert parse_duration('1h30m') == 90
    assert parse_duration('30m') == 30
    assert parse_duration('2h15m') == 135
    assert parse_duration('') is None
    assert parse_duration('bogus') is None


def test_calculate_end_time_same_day():
    assert calculate_end_time('0600', '1h') == '0700'
    assert calculate_end_time('06:00', '1h30m') == '0730'
    assert calculate_end_time('0600', None) == '0555'   # -5m default
    assert calculate_end_time(None, '1h') is None        # perpetual


def test_calculate_end_time_wraps_past_midnight():
    # 2200 + 8h -> 0600 next day; window must be honoured (see below).
    assert calculate_end_time('2200', '8h') == '0600'
    assert calculate_end_time('2330', '1h') == '0030'


def test_hhmm_to_minutes():
    assert hhmm_to_minutes('0000') == 0
    assert hhmm_to_minutes('06:00') == 360
    assert hhmm_to_minutes('2359') == 1439
    with pytest.raises(ValueError):
        hhmm_to_minutes('2460')
    with pytest.raises(ValueError):
        hhmm_to_minutes('6am')


def _at(hour, minute):
    return datetime(2026, 1, 1, hour, minute)


def test_same_day_window():
    assert is_within_time_window('0900', '1700', _at(9, 0))      # start incl.
    assert is_within_time_window('0900', '1700', _at(12, 0))
    assert is_within_time_window('0900', '1700', _at(17, 0))     # end incl.
    assert not is_within_time_window('0900', '1700', _at(8, 59))
    assert not is_within_time_window('0900', '1700', _at(17, 1))


def test_overnight_window_2200_0600():
    """THE bug: this window could never match with integer comparison."""
    win = ('2200', '0600')
    assert is_within_time_window(*win, _at(22, 0))    # start inclusive
    assert is_within_time_window(*win, _at(23, 30))   # before midnight
    assert is_within_time_window(*win, _at(0, 0))     # midnight itself
    assert is_within_time_window(*win, _at(3, 0))     # after midnight
    assert is_within_time_window(*win, _at(6, 0))     # end inclusive
    assert not is_within_time_window(*win, _at(6, 1))
    assert not is_within_time_window(*win, _at(12, 0))
    assert not is_within_time_window(*win, _at(21, 59))


def test_default_minus_five_window_wraps():
    """start=0600, duration=None -> end=0555: a 23h55m wrap window that is
    inactive only 0556-0559 (legacy intent: 'almost always on')."""
    win = ('0600', '0555')
    assert is_within_time_window(*win, _at(6, 0))
    assert is_within_time_window(*win, _at(12, 0))
    assert is_within_time_window(*win, _at(5, 55))
    assert not is_within_time_window(*win, _at(5, 57))


def test_no_window_always_active():
    assert is_within_time_window(None, None, _at(3, 0))
    assert is_within_time_window('0900', None, _at(3, 0))
    assert is_within_time_window(None, '1700', _at(3, 0))


def test_colon_format_accepted():
    assert is_within_time_window('22:00', '06:00', _at(23, 0))
