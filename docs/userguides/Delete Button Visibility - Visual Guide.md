# Delete Button Visibility - Visual Guide

## © 2025-2030 Ashutosh Sinha


## DAG State Transitions and Button Availability

```
┌─────────────────────────────────────────────────────────────┐
│                    DAG LIFECYCLE                            │
└─────────────────────────────────────────────────────────────┘

   STOPPED                RUNNING (Active)           RUNNING (Suspended)
   ┌──────┐              ┌────────────────┐         ┌───────────────────┐
   │      │   START      │                │ SUSPEND │                   │
   │      │ ─────────────>                │────────>│                   │
   │      │              │                │         │                   │
   │      │   STOP       │                │ STOP    │                   │
   │      │<─────────────│                │<────────│                   │
   │      │              │                │ RESUME  │                   │
   │      │              │                │<────────│                   │
   └──────┘              └────────────────┘         └───────────────────┘
   
   DELETE ✅             DELETE ❌                    DELETE ❌
```

## Button Availability by State

### 1. STOPPED State

```
┌─────────────────────────────────────────────────────────────┐
│ Actions for: my_dag                                          │
├─────────────────────────────────────────────────────────────┤
│  [Details] [State] [Clone] [▶ Start] [🗑️ Delete]           │
└─────────────────────────────────────────────────────────────┘

Available buttons:
✅ Details  - View DAG configuration
✅ State    - View current state
✅ Clone    - Clone this DAG (admin only)
✅ Start    - Start the DAG (admin only)
✅ Delete   - Delete the DAG (admin only) ⭐ NEW: Only shown when stopped
```

### 2. RUNNING (Active) State

```
┌─────────────────────────────────────────────────────────────┐
│ Actions for: my_dag                                          │
├─────────────────────────────────────────────────────────────┤
│  [Details] [State] [Clone] [⏸ Suspend] [⏹ Stop]            │
└─────────────────────────────────────────────────────────────┘

Available buttons:
✅ Details  - View DAG configuration
✅ State    - View current state
✅ Clone    - Clone this DAG (admin only)
✅ Suspend  - Suspend the DAG (admin only)
✅ Stop     - Stop the DAG (admin only)
❌ Delete   - NOT SHOWN (DAG must be stopped first)
```

### 3. RUNNING (Suspended) State

```
┌─────────────────────────────────────────────────────────────┐
│ Actions for: my_dag                                          │
├─────────────────────────────────────────────────────────────┤
│  [Details] [State] [Clone] [▶ Resume] [⏹ Stop]             │
└─────────────────────────────────────────────────────────────┘

Available buttons:
✅ Details  - View DAG configuration
✅ State    - View current state
✅ Clone    - Clone this DAG (admin only)
✅ Resume   - Resume the DAG (admin only)
✅ Stop     - Stop the DAG (admin only)
❌ Delete   - NOT SHOWN (DAG must be stopped first)
```

## Complete Dashboard View Example

```
╔═══════════════════════════════════════════════════════════════════════════╗
║                          DAG DASHBOARD                                    ║
╠═══════════════════════════════════════════════════════════════════════════╣
║                                                                           ║
║  Name          Status      Time Window      Nodes   Actions              ║
║  ───────────── ─────────── ───────────────  ─────── ──────────────────── ║
║                                                                           ║
║  etl_daily     [Running]   08:00 - 17:00    5       [Details] [State]   ║
║                                                      [Clone] [⏸ Suspend] ║
║                                                      [⏹ Stop]             ║
║                                                                           ║
║  reports_dag   [Stopped]   Always Active    3       [Details] [State]   ║
║                                                      [Clone] [▶ Start]   ║
║                                                      [🗑️ Delete]   ⭐    ║
║                                                                           ║
║  analytics     [Suspended] 00:00 - 06:00    8       [Details] [State]   ║
║                                                      [Clone] [▶ Resume]  ║
║                                                      [⏹ Stop]             ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝

⭐ Notice: Delete button ONLY appears for "reports_dag" (stopped state)
```

## User Workflow Examples

### Scenario 1: User wants to delete a running DAG

```
Step 1: User sees running DAG
┌──────────────────────────────────────────────────┐
│ etl_daily  [Running]  [⏸ Suspend] [⏹ Stop]     │
└──────────────────────────────────────────────────┘
                              ↓
                         User clicks Stop
                              ↓
Step 2: DAG stops, Delete button appears
┌──────────────────────────────────────────────────┐
│ etl_daily  [Stopped]  [▶ Start] [🗑️ Delete]    │
└──────────────────────────────────────────────────┘
                              ↓
                       User clicks Delete
                              ↓
Step 3: Confirmation dialog
┌──────────────────────────────────────────────────┐
│  Are you sure you want to delete DAG             │
│  "etl_daily"?                                    │
│                                                   │
│  This action cannot be undone.                   │
│                                                   │
│           [Cancel]    [OK]                       │
└──────────────────────────────────────────────────┘
                              ↓
                         User clicks OK
                              ↓
Step 4: DAG is deleted and removed from list
```

### Scenario 2: User wants to delete a stopped DAG

```
Step 1: User sees stopped DAG with Delete button
┌──────────────────────────────────────────────────┐
│ old_dag  [Stopped]  [▶ Start] [🗑️ Delete]       │
└──────────────────────────────────────────────────┘
                              ↓
                       User clicks Delete
                              ↓
Step 2: Confirmation dialog
┌──────────────────────────────────────────────────┐
│  Are you sure you want to delete DAG             │
│  "old_dag"?                                      │
│                                                   │
│  This action cannot be undone.                   │
│                                                   │
│           [Cancel]    [OK]                       │
└──────────────────────────────────────────────────┘
                              ↓
                         User clicks OK
                              ↓
Step 3: DAG is deleted and removed from list
```

## Admin vs Non-Admin View

### Admin User View (Stopped DAG)
```
[Details] [State] [Clone] [▶ Start] [🗑️ Delete]
└─────────────────┘       └─────────────────────┘
  Available to all           Admin only
```

### Non-Admin User View (Stopped DAG)
```
[Details] [State]
└─────────────────┘
  Available to all

No Clone, Start, or Delete buttons visible
```

## Safety Features

```
                     DELETE REQUEST
                           │
                           ▼
        ┌──────────────────────────────────┐
        │  Is user an admin?                │
        └────────────┬─────────────────────┘
                     │
           ┌─────────┴─────────┐
          NO                  YES
           │                   │
           ▼                   ▼
    ┌──────────┐      ┌──────────────────┐
    │ REJECT   │      │ Is DAG stopped?  │
    └──────────┘      └────────┬─────────┘
                                │
                      ┌─────────┴─────────┐
                     NO                  YES
                      │                   │
                      ▼                   ▼
               ┌───────────┐      ┌──────────────┐
               │ REJECT    │      │ Show confirm │
               │ (no btn)  │      │ dialog       │
               └───────────┘      └──────┬───────┘
                                         │
                                    User confirms?
                                         │
                               ┌─────────┴─────────┐
                              NO                  YES
                               │                   │
                               ▼                   ▼
                        ┌──────────┐      ┌──────────────┐
                        │ CANCEL   │      │ DELETE DAG   │
                        └──────────┘      └──────────────┘
```

## Key Points

1. ⭐ **Delete button only visible when DAG is STOPPED**
2. 🔒 **Admin-only feature** (non-admins never see it)
3. ⚠️ **Confirmation required** before deletion
4. 🛡️ **Server-side validation** in delete_dag() route
5. 🧹 **Clean shutdown** - DAG must be stopped first

## Benefits

✅ Prevents accidental deletion of running DAGs
✅ Ensures clean resource cleanup before deletion
✅ Intuitive UI - delete only when it's safe
✅ Consistent with common UI patterns
✅ Multiple layers of protection


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.
