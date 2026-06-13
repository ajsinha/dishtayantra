#!/usr/bin/env python3
"""
Holiday Calendar Generator for DishtaYantra (v2.1.0)
====================================================

Generates the baked-in USA and CANADA holiday calendar files under
config/holidays/ for a configurable year range (default 2026-2040, i.e.
the next 15 years). Re-run this script to extend or regenerate them:

    python scripts/generate_holiday_calendars.py [start_year] [end_year]

Holiday sets (market-oriented, suitable for stock-trading schedules):

USA (NYSE-observed set):
    New Year's Day (Jan 1), Martin Luther King Jr. Day (3rd Mon Jan),
    Presidents' Day (3rd Mon Feb), Good Friday (computus), Memorial Day
    (last Mon May), Juneteenth (Jun 19), Independence Day (Jul 4),
    Labor Day (1st Mon Sep), Thanksgiving (4th Thu Nov),
    Christmas Day (Dec 25).

CANADA (TSX-observed set):
    New Year's Day (Jan 1), Family Day (3rd Mon Feb), Good Friday
    (computus), Victoria Day (Mon on/before May 24), Canada Day (Jul 1),
    Civic Holiday (1st Mon Aug), Labour Day (1st Mon Sep), Thanksgiving
    (2nd Mon Oct), Christmas Day (Dec 25), Boxing Day (Dec 26).

Observance rules (documented in each generated file):
    USA fixed-date holidays: Saturday -> observed Friday before,
        Sunday -> observed Monday after.
    CANADA fixed-date holidays: Saturday/Sunday -> observed the following
        Monday (Boxing Day shifts to the next business day after the
        observed Christmas when they collide).
    Floating holidays (Nth weekday) never need shifting.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import sys
from datetime import date, timedelta
from pathlib import Path

MON, TUE, WED, THU, FRI, SAT, SUN = range(7)


def easter_sunday(year: int) -> date:
    """Anonymous Gregorian computus (Meeus/Jones/Butcher algorithm)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def good_friday(year: int) -> date:
    return easter_sunday(year) - timedelta(days=2)


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th given weekday of a month (n=1 -> first)."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def last_weekday(year: int, month: int, weekday: int) -> date:
    """The last given weekday of a month."""
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def victoria_day(year: int) -> date:
    """Monday on or before May 24."""
    d = date(year, 5, 24)
    return d - timedelta(days=(d.weekday() - MON) % 7)


def us_observed(d: date) -> date:
    """US rule: Sat -> preceding Fri, Sun -> following Mon."""
    if d.weekday() == SAT:
        return d - timedelta(days=1)
    if d.weekday() == SUN:
        return d + timedelta(days=1)
    return d


def ca_observed(d: date) -> date:
    """Canada rule: Sat/Sun -> following Monday."""
    if d.weekday() == SAT:
        return d + timedelta(days=2)
    if d.weekday() == SUN:
        return d + timedelta(days=1)
    return d


def usa_holidays(year: int) -> dict:
    out = {}

    def add(d, name, actual=None):
        label = name if actual is None or actual == d \
            else f"{name} (observed; actual {actual.isoformat()})"
        out[d.isoformat()] = label

    ny = date(year, 1, 1)
    add(us_observed(ny), "New Year's Day", ny)
    add(nth_weekday(year, 1, MON, 3), "Martin Luther King Jr. Day")
    add(nth_weekday(year, 2, MON, 3), "Presidents' Day")
    add(good_friday(year), "Good Friday")
    add(last_weekday(year, 5, MON), "Memorial Day")
    jt = date(year, 6, 19)
    add(us_observed(jt), "Juneteenth National Independence Day", jt)
    j4 = date(year, 7, 4)
    add(us_observed(j4), "Independence Day", j4)
    add(nth_weekday(year, 9, MON, 1), "Labor Day")
    add(nth_weekday(year, 11, THU, 4), "Thanksgiving Day")
    xmas = date(year, 12, 25)
    add(us_observed(xmas), "Christmas Day", xmas)
    return out


def canada_holidays(year: int) -> dict:
    out = {}

    def add(d, name, actual=None):
        label = name if actual is None or actual == d \
            else f"{name} (observed; actual {actual.isoformat()})"
        out[d.isoformat()] = label

    ny = date(year, 1, 1)
    add(ca_observed(ny), "New Year's Day", ny)
    add(nth_weekday(year, 2, MON, 3), "Family Day")
    add(good_friday(year), "Good Friday")
    add(victoria_day(year), "Victoria Day")
    cd = date(year, 7, 1)
    add(ca_observed(cd), "Canada Day", cd)
    add(nth_weekday(year, 8, MON, 1), "Civic Holiday")
    add(nth_weekday(year, 9, MON, 1), "Labour Day")
    add(nth_weekday(year, 10, MON, 2), "Thanksgiving Day (Canada)")

    xmas = date(year, 12, 25)
    box = date(year, 12, 26)
    xmas_obs = ca_observed(xmas)
    box_obs = ca_observed(box)
    if box_obs <= xmas_obs:
        # Collision (Christmas on Sat -> both observed Monday): push Boxing
        # Day to the next business day after observed Christmas.
        box_obs = xmas_obs + timedelta(days=1)
        while box_obs.weekday() in (SAT, SUN):
            box_obs += timedelta(days=1)
    add(xmas_obs, "Christmas Day", xmas)
    add(box_obs, "Boxing Day", box)
    return out


def build_calendar(name, description, generator, start_year, end_year):
    holidays = {}
    for year in range(start_year, end_year + 1):
        holidays.update(generator(year))
    return {
        "name": name,
        "description": description,
        "years": f"{start_year}-{end_year}",
        "observance_rules": (
            "USA: fixed-date holidays falling on Saturday are observed the "
            "preceding Friday, Sunday the following Monday. CANADA: "
            "fixed-date holidays falling on a weekend are observed the "
            "following Monday (Boxing Day shifts past observed Christmas). "
            "Floating holidays (Nth weekday) never shift."
            if name == "USA" else
            "Fixed-date holidays falling on a weekend are observed the "
            "following Monday; Boxing Day shifts to the next business day "
            "after observed Christmas when they collide. Floating holidays "
            "never shift."
        ),
        "generated_by": "scripts/generate_holiday_calendars.py",
        "holidays": dict(sorted(holidays.items())),
    }


def main():
    start_year = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    end_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2040
    out_dir = Path(__file__).resolve().parent.parent / 'config' / 'holidays'
    out_dir.mkdir(parents=True, exist_ok=True)

    usa = build_calendar(
        "USA",
        "United States market holidays (NYSE-observed set) for stock "
        "trading schedules.",
        usa_holidays, start_year, end_year)
    canada = build_calendar(
        "CANADA",
        "Canadian market holidays (TSX-observed set) for stock trading "
        "schedules.",
        canada_holidays, start_year, end_year)

    (out_dir / 'usa.json').write_text(json.dumps(usa, indent=2) + '\n')
    (out_dir / 'canada.json').write_text(json.dumps(canada, indent=2) + '\n')
    print(f"Wrote {out_dir / 'usa.json'} "
          f"({len(usa['holidays'])} holidays)")
    print(f"Wrote {out_dir / 'canada.json'} "
          f"({len(canada['holidays'])} holidays)")


if __name__ == '__main__':
    main()
