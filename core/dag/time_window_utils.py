"""
Utility functions for time window calculations with duration support.

Duration format:
- "1h" = 1 hour
- "1h30m" = 1 hour 30 minutes  
- "30m" = 30 minutes

If start_time is provided but duration is missing, default to -5 minutes (end_time before start_time).
If start_time is missing, ignore duration - DAG runs perpetually.
"""

import re
from datetime import datetime, timedelta


def parse_duration(duration_str):
    """
    Parse duration string to total minutes.

    Args:
        duration_str: String like "1h", "1h30m", "30m", etc.

    Returns:
        int: Total minutes, or None if invalid

    Examples:
        >>> parse_duration("1h")
        60
        >>> parse_duration("1h30m")
        90
        >>> parse_duration("30m")
        30
        >>> parse_duration("2h15m")
        135
    """
    if not duration_str:
        return None

    duration_str = duration_str.strip().lower()

    # Pattern to match hours and/or minutes
    pattern = r'^(?:(\d+)h)?(?:(\d+)m)?$'
    match = re.match(pattern, duration_str)

    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0

    if hours == 0 and minutes == 0:
        return None

    return hours * 60 + minutes


def calculate_end_time(start_time, duration=None):
    """
    Calculate end_time from start_time and duration.

    Args:
        start_time: String in HHMM or HH:MM format (24-hour)
        duration: String like "1h", "1h30m", "30m", or None

    Returns:
        str: end_time in HHMM format, or None if start_time is None

    Rules:
        - If start_time is None, return None (perpetual running)
        - If duration is None, default to -5 minutes from start_time
        - Otherwise, calculate end_time = start_time + duration

    Examples:
        >>> calculate_end_time("0600", "1h")
        '0700'
        >>> calculate_end_time("06:00", "1h30m")
        '0730'
        >>> calculate_end_time("0600", None)
        '0555'
        >>> calculate_end_time("0600", "30m")
        '0630'
        >>> calculate_end_time(None, "1h")
        None
    """
    if start_time is None:
        return None

    # Clean start_time - remove colons if present
    start_time = start_time.replace(':', '')

    if len(start_time) != 4 or not start_time.isdigit():
        raise ValueError(f"Invalid start_time format: {start_time}. Expected HHMM or HH:MM")

    # Parse start_time
    start_hour = int(start_time[:2])
    start_minute = int(start_time[2:])

    if start_hour > 23 or start_minute > 59:
        raise ValueError(f"Invalid start_time: {start_time}. Hour must be 0-23, minute must be 0-59")

    # Parse duration
    duration_minutes = parse_duration(duration) if duration else None

    # Default to -5 minutes if duration not provided
    if duration_minutes is None:
        duration_minutes = -5

    # Calculate end time
    start_dt = datetime.strptime(start_time, '%H%M')
    end_dt = start_dt + timedelta(minutes=duration_minutes)

    # Format as HHMM
    end_time = end_dt.strftime('%H%M')

    return end_time


def hhmm_to_minutes(time_str):
    """
    Normalize an HHMM / HH:MM string to minutes since midnight.

    Args:
        time_str: String like "0600", "06:00", "2230".

    Returns:
        int: Minutes since midnight (0-1439).

    Raises:
        ValueError: when the input is not a valid HHMM time.

    Examples:
        >>> hhmm_to_minutes("0600")
        360
        >>> hhmm_to_minutes("22:30")
        1350
    """
    if time_str is None:
        raise ValueError("time_str must not be None")
    cleaned = str(time_str).replace(':', '').strip()
    if len(cleaned) != 4 or not cleaned.isdigit():
        raise ValueError(
            f"Invalid time format: {time_str!r}. Expected HHMM or HH:MM")
    hour, minute = int(cleaned[:2]), int(cleaned[2:])
    if hour > 23 or minute > 59:
        raise ValueError(
            f"Invalid time: {time_str!r}. Hour must be 0-23, "
            f"minute must be 0-59")
    return hour * 60 + minute


def is_within_time_window(start_time, end_time, now=None):
    """
    True when *now* falls inside the [start_time, end_time] window,
    correctly handling windows that wrap across midnight.

    v2.0.0 BUGFIX: the legacy comparison was performed on integer HHMM
    values (``start <= now <= end``), so an overnight window such as
    2200-0600 could NEVER match. Times are now normalized to
    minutes-since-midnight with an explicit wrap-around branch:

        - start <= end  -> normal same-day window: start <= now <= end
        - start >  end  -> overnight window:       now >= start OR now <= end

    Both boundaries are inclusive, matching the legacy ``<=`` semantics.
    A window where start == end matches only that exact minute.

    Args:
        start_time: Window start, HHMM or HH:MM (None -> always inside).
        end_time:   Window end,   HHMM or HH:MM (None -> always inside).
        now:        Optional datetime for testing (default: datetime.now()).

    Returns:
        bool

    Examples:
        >>> from datetime import datetime
        >>> is_within_time_window("0900", "1700", datetime(2026, 1, 1, 12, 0))
        True
        >>> is_within_time_window("2200", "0600", datetime(2026, 1, 1, 23, 30))
        True
        >>> is_within_time_window("2200", "0600", datetime(2026, 1, 1, 3, 0))
        True
        >>> is_within_time_window("2200", "0600", datetime(2026, 1, 1, 12, 0))
        False
    """
    if not start_time or not end_time:
        # No window configured: always active (legacy behaviour).
        return True

    start_minutes = hhmm_to_minutes(start_time)
    end_minutes = hhmm_to_minutes(end_time)
    if now is None:
        now = datetime.now()
    now_minutes = now.hour * 60 + now.minute

    if start_minutes <= end_minutes:
        # Same-day window, e.g. 0900-1700.
        return start_minutes <= now_minutes <= end_minutes
    # Overnight window, e.g. 2200-0600: inside before midnight OR after.
    return now_minutes >= start_minutes or now_minutes <= end_minutes


def format_time_display(time_str):
    """
    Format time string for display (HH:MM).

    Args:
        time_str: String in HHMM format

    Returns:
        str: Time in HH:MM format

    Examples:
        >>> format_time_display("0600")
        '06:00'
        >>> format_time_display("1530")
        '15:30'
    """
    if not time_str or len(time_str) != 4:
        return time_str

    return f"{time_str[:2]}:{time_str[2:]}"


def format_duration_display(duration_str):
    """
    Format duration string for display.

    Args:
        duration_str: String like "1h", "1h30m", "30m"

    Returns:
        str: Formatted duration string

    Examples:
        >>> format_duration_display("1h")
        '1h'
        >>> format_duration_display("1h30m")
        '1h 30m'
        >>> format_duration_display("30m")
        '30m'
    """
    if not duration_str:
        return duration_str

    duration_str = duration_str.strip().lower()

    # Add space between hours and minutes for readability
    duration_str = duration_str.replace('h', 'h ')

    return duration_str.strip()


def get_time_window_info(start_time, duration=None):
    """
    Get complete time window information.

    Args:
        start_time: String in HHMM or HH:MM format
        duration: String like "1h", "1h30m", "30m", or None

    Returns:
        dict: {
            'start_time': str (HHMM format),
            'end_time': str (HHMM format),
            'duration': str (original duration string),
            'duration_minutes': int (total minutes),
            'display_start': str (HH:MM format),
            'display_end': str (HH:MM format),
            'display_duration': str (formatted duration),
            'is_perpetual': bool
        }
    """
    if start_time is None:
        return {
            'start_time': None,
            'end_time': None,
            'duration': None,
            'duration_minutes': None,
            'display_start': None,
            'display_end': None,
            'display_duration': None,
            'is_perpetual': True
        }

    # Clean start_time
    start_time = start_time.replace(':', '')

    # Calculate end_time
    end_time = calculate_end_time(start_time, duration)

    # Parse duration
    duration_minutes = parse_duration(duration) if duration else -5

    return {
        'start_time': start_time,
        'end_time': end_time,
        'duration': duration,
        'duration_minutes': duration_minutes,
        'display_start': format_time_display(start_time),
        'display_end': format_time_display(end_time),
        'display_duration': format_duration_display(duration) if duration else '-5m (default)',
        'is_perpetual': False
    }


if __name__ == '__main__':
    # Test the functions
    print("Testing parse_duration:")
    print(f"  1h -> {parse_duration('1h')} minutes")
    print(f"  1h30m -> {parse_duration('1h30m')} minutes")
    print(f"  30m -> {parse_duration('30m')} minutes")
    print(f"  2h15m -> {parse_duration('2h15m')} minutes")

    print("\nTesting calculate_end_time:")
    print(f"  0600 + 1h -> {calculate_end_time('0600', '1h')}")
    print(f"  0600 + 1h30m -> {calculate_end_time('0600', '1h30m')}")
    print(f"  0600 + 30m -> {calculate_end_time('0600', '30m')}")
    print(f"  0600 + None -> {calculate_end_time('0600', None)} (default -5m)")
    print(f"  None + 1h -> {calculate_end_time(None, '1h')} (perpetual)")

    print("\nTesting get_time_window_info:")
    info = get_time_window_info('0600', '1h')
    print(f"  0600 + 1h -> {info}")

    info = get_time_window_info('0600', None)
    print(f"  0600 + None -> {info}")

    info = get_time_window_info(None, '1h')
    print(f"  None + 1h -> {info}")