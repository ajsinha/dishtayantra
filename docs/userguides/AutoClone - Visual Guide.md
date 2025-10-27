# AutoClone - Visual Guide

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DishtaYantra Compute Server                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌───────────────┐      ┌────────────────────────────────────┐    │
│  │  Flask App    │      │   AutoClone Manager Thread         │    │
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

## Timeline Visualization

### Complete 24-Hour Cycle

```
Time:  00:00    06:00    09:00    12:00    17:00    20:00    23:59
       ─────────────────────────────────────────────────────────────
State: │  IDLE  │  IDLE  │RAMP UP│ACTIVE │RAMP DOWN│  IDLE  │ IDLE │
       ─────────────────────────────────────────────────────────────
                          ▲                ▲
                          │                │
                    ramp_up_time      ramp_down_time
                      (0900)              (1700)
                          
Clones:                  0 → 1 → 2 → 3     3 → 2 → 1 → 0
                        (1 min intervals)  (1 min intervals)

Parent: [─────────────────RUNNING────────────────────────────────]
Clone1:                  [────────RUNNING────────────]
Clone2:                      [────────RUNNING────────]
Clone3:                          [────────RUNNING────────]
```

### Detailed Ramp Up Sequence

```
Configuration:
  ramp_up_time: "0900"
  ramp_count: 5

Timeline:
09:00:00  ┐
          │  Manager checks time
          │  Triggers clone creation
          └─► Create clone_1 (my_dag_20250125090000)
              Start clone_1
              Status: "ramping_up (1/5)"
              
09:01:00  ┐
          │  Manager checks again (1 min passed)
          └─► Create clone_2 (my_dag_20250125090100)
              Start clone_2
              Status: "ramping_up (2/5)"
              
09:02:00  ┐
          └─► Create clone_3
              Status: "ramping_up (3/5)"
              
09:03:00  ┐
          └─► Create clone_4
              Status: "ramping_up (4/5)"
              
09:04:00  ┐
          └─► Create clone_5
              Status: "active"
              ramp_up_completed = True
              
09:05:00+     No more clones created
              All 5 clones running
              Parent + 5 clones = 6 total DAGs
```

### Detailed Ramp Down Sequence

```
Configuration:
  ramp_down_time: "1700"
  Current clones: 5

Timeline:
17:00:00  ┐
          │  Manager detects ramp_down_time
          │  Sets status: "ramping_down"
          └─► Stop clone_1
              Wait 2 seconds
              Delete clone_1
              Status: "ramping_down (4 remaining)"
              
17:01:00  ┐
          │  Manager checks again (1 min passed)
          └─► Stop clone_2
              Delete clone_2
              Status: "ramping_down (3 remaining)"
              
17:02:00  ┐
          └─► Stop clone_3
              Delete clone_3
              Status: "ramping_down (2 remaining)"
              
17:03:00  ┐
          └─► Stop clone_4
              Delete clone_4
              Status: "ramping_down (1 remaining)"
              
17:04:00  ┐
          └─► Stop clone_5
              Delete clone_5
              Status: "idle"
              ramp_up_completed = False
              ramp_down_started = False
              
17:05:00+     Back to 1 DAG (parent only)
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

## Dashboard State Visualization

### State 1: IDLE (Before Ramp Up)

```
╔═══════════════════════════════════════════════════════════════╗
║  DAG DASHBOARD                                    [PRIMARY]   ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Name              Status    AutoClone         Actions        ║
║  ────────────────  ────────  ───────────────   ─────────────  ║
║                                                               ║
║  production_etl    Running   [✓ Enabled]       [Details]     ║
║                              idle               [State]       ║
║                                                 [Clone]       ║
║                                                 [Stop]        ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Total DAGs: 1 (parent only)
Clones: 0
```

### State 2: RAMPING UP (Creating Clones)

```
╔═══════════════════════════════════════════════════════════════╗
║  DAG DASHBOARD                                    [PRIMARY]   ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Name                      Status    AutoClone    Actions     ║
║  ───────────────────────   ───────   ──────────   ─────────   ║
║                                                               ║
║  production_etl            Running   [✓ Enabled]  [Details]  ║
║                                      ramping_up   [State]     ║
║                                      (2/5)        [Clone]     ║
║                                      [2 clone(s)] [Stop]      ║
║                                                               ║
║  production_etl_090000     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                   [Auto-man]  ║
║                                                               ║
║  production_etl_090100     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                   [Auto-man]  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Total DAGs: 3 (1 parent + 2 clones, more coming...)
Status: Creating 1 clone per minute
```

### State 3: ACTIVE (All Clones Running)

```
╔═══════════════════════════════════════════════════════════════╗
║  DAG DASHBOARD                                    [PRIMARY]   ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Name                      Status    AutoClone    Actions     ║
║  ───────────────────────   ───────   ──────────   ─────────   ║
║                                                               ║
║  production_etl            Running   [✓ Enabled]  [Details]  ║
║                                      active       [State]     ║
║                                      [5 clone(s)] [Clone]     ║
║                                                   [Stop]      ║
║                                                               ║
║  production_etl_090000     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  production_etl_090100     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  production_etl_090200     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  production_etl_090300     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  production_etl_090400     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Total DAGs: 6 (1 parent + 5 clones)
All clones running at full capacity
```

### State 4: RAMPING DOWN (Deleting Clones)

```
╔═══════════════════════════════════════════════════════════════╗
║  DAG DASHBOARD                                    [PRIMARY]   ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Name                      Status    AutoClone    Actions     ║
║  ───────────────────────   ───────   ──────────   ─────────   ║
║                                                               ║
║  production_etl            Running   [✓ Enabled]  [Details]  ║
║                                      ramping_dn   [State]     ║
║                                      (3 remain)   [Clone]     ║
║                                      [3 clone(s)] [Stop]      ║
║                                                               ║
║  production_etl_090200     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  production_etl_090300     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  production_etl_090400     Running   [🤖 Auto]   [Details]   ║
║  ↳ Clone of production_etl           Created     [State]     ║
║                                                               ║
║  (clones 090000 and 090100 have been deleted)                ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Total DAGs: 4 (1 parent + 3 clones, decreasing...)
Status: Deleting 1 clone per minute
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

## AutoClone Manager Flow

```
┌──────────────────────────────────────────────────────────┐
│  AutoClone Manager Thread Loop (every 10 seconds)       │
└──────────────────────────────────────────────────────────┘
                          │
                          ▼
                ┌──────────────────┐
                │ Get current time │
                └────────┬─────────┘
                         │
                         ▼
            ┌───────────────────────┐
            │ For each DAG:         │
            └────────┬──────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │ Is autoclone enabled?      │
        └─────┬──────────────────────┘
              │
      ┌───────┴───────┐
     NO              YES
      │               │
      ▼               ▼
  ┌───────┐   ┌──────────────────┐
  │ Skip  │   │ Check time window│
  └───────┘   └────────┬─────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
  ┌──────────┐                ┌──────────┐
  │ Ramp Up  │                │Ramp Down │
  │ Window?  │                │ Window?  │
  └────┬─────┘                └────┬─────┘
       │                           │
      YES                         YES
       │                           │
       ▼                           ▼
  ┌────────────┐            ┌──────────────┐
  │ Create     │            │ Stop & Delete│
  │ Clone      │            │ Clone        │
  │ (1/minute) │            │ (1/minute)   │
  └────────────┘            └──────────────┘
```

## Resource Usage Visualization

```
Without AutoClone:
═══════════════════════════════════════════════════════════

Resources: ████░░░░░░░░░░░░░░░░ (1 DAG)
           └─ Parent DAG



With AutoClone (ramp_count = 5):
═══════════════════════════════════════════════════════════

Resources: ████████████████████████████████ (6 DAGs)
           │   │   │   │   │   └─ Clone 5
           │   │   │   │   └───── Clone 4
           │   │   │   └───────── Clone 3
           │   │   └───────────── Clone 2
           │   └───────────────── Clone 1
           └───────────────────── Parent DAG

6x Processing Capacity!
```

## Key Takeaways

1. **Automatic Management** - No manual intervention needed
2. **Gradual Scaling** - 1 clone per minute (gentle ramp)
3. **Time-Based** - Follows configured schedule
4. **Always Active Clones** - No time window restrictions
5. **Visual Feedback** - Clear dashboard indication
6. **Safe Operations** - Auto-clones cannot be manually modified

Perfect for peak hour scaling! 🚀