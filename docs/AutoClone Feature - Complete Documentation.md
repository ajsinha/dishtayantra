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

Add an `autoclone` dictionary to your DAG configuration JSON file:

```json
{
  "name": "my_production_dag",
  "start_time": "0900",
  "end_time": "1700",
  "nodes": [...],
  "edges": [...],
  
  "autoclone": {
    "ramp_up_time": "0900",
    "ramp_down_time": "1700",
    "ramp_count": 5
  }
}
```

### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ramp_up_time` | String | Yes | Time to start creating clones (HHMM format, e.g., "0900") |
| `ramp_down_time` | String | Yes | Time to start stopping/deleting clones (HHMM format, e.g., "1700") |
| `ramp_count` | Integer | Yes | Number of clones to create (must be > 0) |

**IMPORTANT:** All three parameters MUST be present and non-empty. If any are missing or empty, AutoClone is disabled.

## Behavior Details

### Ramp Up Phase (Starting at ramp_up_time)

1. **First clone created immediately** at ramp_up_time
2. **Wait 1 minute**
3. **Second clone created**
4. Repeat until `ramp_count` clones are created
5. **Each clone is started automatically** after creation
6. **All clones have no time window** (always active)

**Example:**
- ramp_up_time: "0900"
- ramp_count: 3
- Timeline:
  - 09:00 - Create and start clone #1
  - 09:01 - Create and start clone #2
  - 09:02 - Create and start clone #3
  - Status: "active" with 3 clones running

### Active Phase (Between ramp_up_time and ramp_down_time)

- All clones are running
- Parent DAG status shows: "active (3/3)"
- No new clones created
- Existing clones continue to run

### Ramp Down Phase (Starting at ramp_down_time)

1. **First clone stopped and deleted** at ramp_down_time
2. **Wait 1 minute**
3. **Second clone stopped and deleted**
4. Repeat until all clones are removed
5. System returns to **idle** state

**Example:**
- ramp_down_time: "1700"
- Timeline:
  - 17:00 - Stop and delete clone #1
  - 17:01 - Stop and delete clone #2
  - 17:02 - Stop and delete clone #3
  - Status: "idle"

### Idle Phase (Outside time window)

- No clones exist
- Status: "idle"
- Waiting for next ramp_up_time

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
    "ramp_down_time": "1800",
    "ramp_count": 4
  }
}
```

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

### Timing Precision

- Check interval: Every 10 seconds
- Clone creation interval: 1 minute
- Clone deletion interval: 1 minute

### Primary/Secondary Servers

- AutoClone only runs on **PRIMARY** server
- If Zookeeper is unavailable, server defaults to PRIMARY
- Secondary servers do not manage AutoClone

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
1. ✅ All three parameters present and non-empty
2. ✅ ramp_count is a positive integer
3. ✅ Time format is HHMM
4. ✅ Server is PRIMARY (check logs)
5. ✅ Current time is within ramp_up window

### Clones Not Being Created

**Check:**
1. ✅ Check server logs for errors
2. ✅ Verify time format
3. ✅ Check if ramp_up_time has passed
4. ✅ Verify server has necessary permissions

### Clones Not Being Deleted

**Check:**
1. ✅ Verify ramp_down_time configuration
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
├── Main Thread (Flask)
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