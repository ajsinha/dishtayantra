# AutoClone Feature - Complete Documentation

## Overview

AutoClone is a powerful feature that automatically creates, starts, and manages temporary DAG clones based on time windows. This is useful for:
- Load distribution during peak hours
- Temporary scaling for high-demand periods
- Automated testing scenarios
- Resource ramping strategies

## How It Works

```
Timeline View:
═══════════════════════════════════════════════════════════════════════

00:00          ramp_up_time              ramp_down_time         23:59
  │                 │                          │                  │
  │   IDLE          │      ACTIVE              │      IDLE        │
  │                 │                          │                  │
  │                 ▼ Create clone #1          ▼ Stop clone #1    │
  │                 │ (wait 1 min)             │ (wait 1 min)     │
  │                 ▼ Create clone #2          ▼ Stop clone #2    │
  │                 │ (wait 1 min)             │ (wait 1 min)     │
  │                 ▼ Create clone #3          ▼ Stop clone #3    │
  │                 │ ...                      │ ...              │
  │                 ▼ Create clone #N          ▼ Stop clone #N    │
  │                 │                          │                  │
  │                 └──► All clones running ◄──┘                  │
  │                                                                │
═══════════════════════════════════════════════════════════════════════
```

## Configuration

### Enable AutoClone in DAG Config

Add an `autoclone` dictionary to your DAG configuration JSON file. There are
two equivalent ways to define the active window — a **duration** (preferred)
or a legacy explicit **ramp_down_time**.

**Preferred — duration based:**

```json
{
  "name": "my_production_dag",
  "start_time": "0900",
  "duration": "8h",
  "nodes": [...],
  "edges": [...],

  "autoclone": {
    "ramp_up_time": "0930",
    "duration": "1h30m",
    "ramp_count": 5
  }
}
```

Here clones ramp up starting at 09:30 and the ramp-down begins at
09:30 + 1h30m = 11:00.

**Legacy — explicit ramp_down_time (still supported):**

```json
"autoclone": {
  "ramp_up_time": "0900",
  "ramp_down_time": "1700",
  "ramp_count": 5
}
```

### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ramp_up_time` | String | **Yes** | Wall-clock time to start creating clones (HHMM, e.g. `"0930"`) |
| `ramp_count` | Integer | **Yes** | Number of clones to create at peak (must be > 0) |
| `duration` | String | One of these | How long the ramped-up state lasts (e.g. `"1h30m"`, `"8h"`, `"45m"`). Ramp-down begins at `ramp_up_time + duration`. **Preferred.** |
| `ramp_down_time` | String | One of these | Explicit wall-clock time to begin tearing clones down (HHMM). **Legacy** alternative to `duration`. |
| `timezone` | String | No | IANA timezone the ramp times are interpreted in (e.g. `"America/New_York"`). Defaults to the DAG's schedule timezone, or `America/New_York` if the DAG has no schedule. |

**Required vs optional:**

- `ramp_up_time` and `ramp_count` are **always required**.
- You should provide **either** `duration` (preferred) **or** `ramp_down_time`
  (legacy). If you provide **neither**, AutoClone uses a **default duration of
  8h** (ramp-down at `ramp_up_time + 8h`).
- If `duration` is present it **takes precedence** over any `ramp_down_time`.
- If `ramp_up_time` or `ramp_count` is missing/empty, or `ramp_count` is not a
  positive integer, or a supplied `duration` is invalid, AutoClone is treated
  as **disabled** for that DAG (fail-safe — it simply does nothing rather than
  erroring).

### Timezone behavior

`ramp_up_time` and the ramp-down time are **wall-clock times** evaluated in a
timezone, exactly like the DAG scheduling feature — **not** the server's local
time. This matters because servers commonly run in UTC: `ramp_up_time: "0930"`
means 09:30 in the configured zone, so ramping happens at the right local time
whether the host clock is UTC or anything else. Resolution order for the zone:

1. `autoclone.timezone` if set;
2. otherwise the DAG's `schedule.timezone` if the DAG has a schedule;
3. otherwise the default `America/New_York`.

DST transitions are handled automatically.

## Behavior Details

### Ramp Up Phase (Starting at ramp_up_time)

1. **First clone created** on the first manager pass at/after `ramp_up_time`
2. **One additional clone per minute** thereafter
3. Repeat until `ramp_count` clones exist
4. **Each clone is started automatically** after creation
5. **All clones have no time window** (always active while they exist)

The manager thread wakes every 10 seconds, so a clone is created on the first
pass once at least 60 seconds have elapsed since the previous one (the first
clone is created immediately when the window opens).

**Example:**
- ramp_up_time: "0930", ramp_count: 3
- Timeline (in the configured timezone):
  - 09:30 - Create and start clone #1
  - 09:31 - Create and start clone #2
  - 09:32 - Create and start clone #3
  - Status: "active" with 3 clones running

### Active Phase (Between ramp_up_time and ramp_down_time)

- All clones are running
- Parent DAG status shows: "active"
- No new clones created
- Existing clones continue to run

### Ramp Down Phase (Starting at ramp_down_time)

`ramp_down_time` is `ramp_up_time + duration` (or the explicit legacy value,
or `ramp_up_time + 8h` by default).

1. **First clone stopped and deleted** on the first pass at/after ramp-down
2. **One clone removed per minute** thereafter
3. Repeat until all clones are removed
4. System returns to **idle** state

**Example:**
- ramp_up_time "0930" + duration "1h30m" -> ramp_down at 11:00
- Timeline:
  - 11:00 - Stop and delete clone #1
  - 11:01 - Stop and delete clone #2
  - 11:02 - Stop and delete clone #3
  - Status: "idle"

### Idle Phase (Outside the ramp window)

- No clones exist
- Status: "idle"
- Waiting for the next `ramp_up_time`

## Clone Characteristics

AutoClone creates clones with these properties:

1. **Name Format:** `{parent_dag_name}_{timestamp}`
   - Example: `my_dag_20250125143022`

2. **Time Window:** None (always active)
   - start_time: None
   - end_time: None
   - Runs 24/7 once started

3. **Configuration:** Exact copy of parent DAG (except time window)

4. **Management:** Fully automated
   - No manual start/stop/delete allowed
   - Managed by AutoClone system

## Dashboard Visualization

### Parent DAG with AutoClone Enabled

```
┌──────────────────────────────────────────────────────────────────┐
│ Name: production_etl                                             │
│ Status: [Running]                                                │
│ Time Window: 09:00 - 17:00                                       │
│ Nodes: 5                                                         │
│ AutoClone: [✓ Enabled]                                           │
│            Status: active (3/3)                                  │
│            [3 clone(s)]                                          │
│ Actions: [Details] [State] [Clone] [⏸ Suspend] [⏹ Stop]        │
└──────────────────────────────────────────────────────────────────┘
```

### Auto-Created Clone DAG

```
┌──────────────────────────────────────────────────────────────────┐
│ Name: production_etl_20250125090000                              │
│       ↳ Clone of: production_etl                                 │
│ Status: [Running]                                                │
│ Time Window: Always Active                                       │
│ Nodes: 5                                                         │
│ AutoClone: [🤖 Auto-Created]                                     │
│ Actions: [Details] [State] [Auto-managed]                       │
└──────────────────────────────────────────────────────────────────┘
```

**Note:** Blue background indicates auto-created clones

## AutoClone Status Values

| Status | Meaning |
|--------|---------|
| `idle` | No clones, outside time window |
| `ramping_up (N/M)` | Creating clones (N created out of M total) |
| `active` | All clones created and running |
| `ramping_down (N remaining)` | Deleting clones (N still remaining) |

## Example Use Cases

### Use Case 1: Peak Hour Scaling

**Scenario:** E-commerce site needs 5x processing during business hours

```json
{
  "name": "order_processing",
  "autoclone": {
    "ramp_up_time": "0800",
    "duration": "10h",
    "ramp_count": 4,
    "timezone": "America/New_York"
  }
}
```

(Equivalent legacy form: `"ramp_down_time": "1800"` instead of `duration`.)

**Result:**
- 08:00-08:04: 4 clones created (1 per minute)
- 08:04-18:00: 5 DAGs total running (1 parent + 4 clones)
- 18:00-18:04: 4 clones deleted (1 per minute)
- 18:04+: Back to 1 DAG (parent only)

### Use Case 2: Overnight Batch Processing

**Scenario:** Heavy processing needed during off-hours

```json
{
  "name": "batch_analytics",
  "autoclone": {
    "ramp_up_time": "2200",
    "ramp_down_time": "0600",
    "ramp_count": 10
  }
}
```

**Result:**
- 22:00-22:10: 10 clones created
- 22:10-06:00: 11 DAGs running
- 06:00-06:10: 10 clones deleted

### Use Case 3: Testing Environment

**Scenario:** Create temporary test instances

```json
{
  "name": "test_pipeline",
  "autoclone": {
    "ramp_up_time": "1400",
    "ramp_down_time": "1500",
    "ramp_count": 3
  }
}
```

**Result:**
- One hour of testing with 3 clones (14:00-15:00)
- Automatic cleanup after testing

## Important Notes

### Restrictions on Auto-Created Clones

Auto-created clones **CANNOT** be:
- ❌ Started manually
- ❌ Stopped manually
- ❌ Suspended manually
- ❌ Resumed manually
- ❌ Deleted manually
- ❌ Cloned

Auto-created clones **CAN** be:
- ✅ Viewed (Details)
- ✅ Monitored (State)

### Time Format

- Time must be in **HHMM** format (24-hour)
- Examples:
  - `"0900"` = 9:00 AM
  - `"1730"` = 5:30 PM
  - `"0000"` = Midnight
  - `"2359"` = 11:59 PM
- Times are **wall-clock in the configured timezone** (see "Timezone behavior"
  above) — `America/New_York` by default — not the server's local clock.

### Duration Format

When using `duration` (preferred over `ramp_down_time`):

- `"8h"` = 8 hours
- `"1h30m"` = 1 hour 30 minutes
- `"45m"` = 45 minutes
- `"2h15m"` = 2 hours 15 minutes

Ramp-down occurs at `ramp_up_time + duration`. If the computed ramp-down
crosses midnight, the window wraps correctly.

### Timing Precision

- Check interval: Every 10 seconds
- Clone creation interval: 1 minute
- Clone deletion interval: 1 minute

### Primary/Secondary Servers

- AutoClone only runs on the **PRIMARY** instance, so an HA fleet never
  double-creates clones.
- PRIMARY election is handled by the configured HA provider
  (`none`, `zookeeper`, `redis`, `s3`, or `socket`). With `ha.provider=none`
  a standalone server is always PRIMARY.
- On demotion the manager loop simply stops acting (it checks `is_primary`
  every pass); the newly-elected PRIMARY takes over AutoClone management.

## Monitoring

### Log Messages

**Ramp Up:**
```
INFO - AutoClone: Created production_etl_20250125090000 for production_etl (1/5)
INFO - AutoClone: Created production_etl_20250125090100 for production_etl (2/5)
...
INFO - AutoClone: Ramp up completed for production_etl
```

**Ramp Down:**
```
INFO - AutoClone: Starting ramp down for production_etl
INFO - AutoClone: Stopping and deleting production_etl_20250125090000
...
INFO - AutoClone: Ramp down completed for production_etl
```

**Errors:**
```
ERROR - AutoClone: Error creating clone for production_etl: <error message>
ERROR - AutoClone: Error deleting clone: <error message>
```

### Checking Status via API

```python
# Get autoclone status for a specific DAG
status = dag_server.get_autoclone_status("my_dag")

# Returns:
{
    'clones': ['my_dag_20250125090000', 'my_dag_20250125090100'],
    'config': {'ramp_up_time': '0900', 'ramp_down_time': '1700', 'ramp_count': 5},
    'status': 'active',
    'last_clone_time': <datetime>,
    'ramp_up_completed': True,
    'ramp_down_started': False
}
```

## Disabling AutoClone

To disable AutoClone for a DAG:

**Option 1:** Remove the `autoclone` section from config
```json
{
  "name": "my_dag",
  // Remove the entire autoclone section
  "nodes": [...]
}
```

**Option 2:** Remove any required field
```json
{
  "name": "my_dag",
  "autoclone": {
    "ramp_up_time": "",  // Empty = disabled
    "ramp_down_time": "1700",
    "ramp_count": 5
  }
}
```

When disabled:
- Existing clones are automatically stopped and deleted
- Status changes to "idle"
- No new clones will be created

## Troubleshooting

### AutoClone Not Working

**Check:**
1. ✅ `ramp_up_time` and `ramp_count` are present and non-empty
2. ✅ `ramp_count` is a positive integer
3. ✅ Either `duration` or `ramp_down_time` is set (or accept the 8h default)
4. ✅ A supplied `duration` is valid (e.g. `"1h30m"`, not `"90"`)
5. ✅ Time format is HHMM
6. ✅ Server is PRIMARY (check logs)
7. ✅ Current time **in the configured timezone** is within the ramp window

### Ramp Happens at the Wrong Time

**Most common cause: timezone.** Ramp times are wall-clock in the configured
zone (default `America/New_York`), not the server's local clock. If your
server runs in UTC and clones ramp several hours early or late:

1. ✅ Set `autoclone.timezone` (or the DAG's `schedule.timezone`) explicitly to
   the zone you intend the HHMM times to mean.
2. ✅ Confirm in the logs which ramp-down time was computed
   (`AutoClone <dag>: Using duration ... ramp_down_time=...`).

### Clones Not Being Created

**Check:**
1. ✅ Check server logs for errors (filter the log viewer by this DAG)
2. ✅ Verify time format and timezone
3. ✅ Check if `ramp_up_time` (in the configured zone) has passed
4. ✅ Verify the server has necessary permissions

### Clones Not Being Deleted

**Check:**
1. ✅ Verify `duration` / `ramp_down_time` configuration
2. ✅ Check server logs
3. ✅ Ensure clones are not stuck in running state

## Best Practices

1. **Test first:** Try with small ramp_count (2-3) before scaling up
2. **Monitor logs:** Watch for errors during initial setup
3. **Time windows:** Ensure adequate time between ramp_up and ramp_down
4. **Resource planning:** Each clone uses full DAG resources
5. **Naming:** Use descriptive parent DAG names for easy identification

## Architecture

### Thread Management

```
DAGComputeServer
├── Main Thread (FastAPI/uvicorn)
├── Zookeeper Thread (Leader Election)
└── AutoClone Manager Thread
    └── Runs every 10 seconds
        ├── Check current time
        ├── Check each DAG for autoclone config
        ├── Create/Start clones (ramp up)
        └── Stop/Delete clones (ramp down)
```

### Data Structures

```python
autoclone_info = {
    'parent_dag_name': {
        'clones': ['clone1', 'clone2'],
        'config': {...},
        'status': 'active',
        'last_clone_time': datetime,
        'ramp_up_completed': True,
        'ramp_down_started': False
    }
}
```

## Summary

AutoClone provides:
- ✅ Automated scaling based on time
- ✅ Gradual ramp up/down
- ✅ No manual intervention needed
- ✅ Full lifecycle management
- ✅ Dashboard visibility
- ✅ Log monitoring

Perfect for peak hour scaling, scheduled processing, and temporary capacity needs!

## Visual Reference

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DishtaYantra Compute Server                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌───────────────┐      ┌────────────────────────────────────┐    │
│  │  FastAPI App  │      │   AutoClone Manager Thread         │    │
│  │  (Main)       │      │   (Runs every 10 seconds)          │    │
│  └───────────────┘      └────────────────────────────────────┘    │
│                                       │                             │
│                                       │                             │
│  ┌────────────────────────────────────▼──────────────────────┐    │
│  │                                                             │    │
│  │           DAG Dictionary (self.dags)                       │    │
│  │                                                             │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │    │
│  │  │  Parent DAG  │  │  Clone #1    │  │  Clone #2    │    │    │
│  │  │  (Original)  │  │ (AutoClone)  │  │ (AutoClone)  │    │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘    │    │
│  │                                                             │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │        AutoClone Info Dictionary                            │    │
│  │                                                             │    │
│  │  parent_dag_name: {                                        │    │
│  │    clones: ['clone1', 'clone2'],                          │    │
│  │    status: 'active',                                       │    │
│  │    ramp_up_completed: True                                │    │
│  │  }                                                          │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## State Machine Diagram

```
                    ┌──────────────────┐
                    │                  │
                    │      IDLE        │
                    │  No clones       │
                    │                  │
                    └────────┬─────────┘
                             │
            Time reaches     │
            ramp_up_time     │
                             │
                             ▼
                    ┌──────────────────┐
                    │                  │
        ┌──────────►│   RAMPING UP     │
        │           │  Creating clones │
        │           │  (1 per minute)  │
        │           │                  │
        │           └────────┬─────────┘
        │                    │
        │    All clones      │
        │    created         │
        │                    │
        │                    ▼
        │           ┌──────────────────┐
        │           │                  │
        │           │     ACTIVE       │
        │           │  All clones run  │
        │           │                  │
        │           └────────┬─────────┘
        │                    │
        │    Time reaches    │
        │    ramp_down_time  │
        │                    │
        │                    ▼
        │           ┌──────────────────┐
        │           │                  │
        │           │  RAMPING DOWN    │
        │           │ Deleting clones  │
        │           │  (1 per minute)  │
        │           │                  │
        │           └────────┬─────────┘
        │                    │
        │    All clones      │
        │    deleted         │
        │                    │
        └────────────────────┘
```

## Configuration Decision Tree

```
                    Read DAG Config
                          │
                          ▼
              ┌─────────────────────┐
              │ Has 'autoclone'     │
              │ section?            │
              └──────┬──────────────┘
                     │
          ┌──────────┴──────────┐
         NO                    YES
          │                     │
          ▼                     ▼
    ┌──────────┐    ┌─────────────────────┐
    │ AutoClone│    │ Check required      │
    │ DISABLED │    │ fields:             │
    └──────────┘    │ - ramp_up_time      │
                    │ - ramp_down_time    │
                    │ - ramp_count        │
                    └──────┬──────────────┘
                           │
                ┌──────────┴──────────┐
               ALL                  MISSING
              PRESENT             OR EMPTY
                │                     │
                ▼                     ▼
    ┌──────────────────┐    ┌──────────────┐
    │ Validate         │    │ AutoClone    │
    │ ramp_count > 0   │    │ DISABLED     │
    └──────┬───────────┘    └──────────────┘
           │
    ┌──────┴──────┐
   YES           NO
    │             │
    ▼             ▼
┌─────────┐  ┌──────────┐
│AutoClone│  │AutoClone │
│ ENABLED │  │ DISABLED │
└─────────┘  └──────────┘
```

## Key Takeaways

1. **Automatic Management** - No manual intervention needed
2. **Gradual Scaling** - 1 clone per minute (gentle ramp)
3. **Time-Based** - Follows configured schedule
4. **Always Active Clones** - No time window restrictions
5. **Visual Feedback** - Clear dashboard indication
6. **Safe Operations** - Auto-clones cannot be manually modified

Perfect for peak hour scaling! 🚀
