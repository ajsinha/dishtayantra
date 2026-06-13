"""
DAG Scheduling Package (v2.1.0)
===============================

Optional day-of-week and holiday-calendar scheduling for DAGs - built for
stock-trading workloads. See dag_schedule.py for the configuration format
and semantics, holiday_calendar.py for the calendar manager, and
scripts/generate_holiday_calendars.py for the baked-in USA/CANADA files.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""
from core.schedule.dag_schedule import DagSchedule, is_schedule_active
from core.schedule.holiday_calendar import (
    HolidayCalendarManager,
    ScheduleConfigError,
)

__all__ = ['DagSchedule', 'HolidayCalendarManager', 'ScheduleConfigError',
           'is_schedule_active']
