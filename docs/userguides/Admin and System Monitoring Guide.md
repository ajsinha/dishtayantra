# Admin & System Monitoring Guide

## Version 1.1.1

© 2025-2030 Ashutosh Sinha

---

## Overview

DishtaYantra v1.1.1 introduces comprehensive admin features including real-time system monitoring, log viewing, and enhanced navigation. These features are accessible only to users with the **admin** role.

---

## Admin Navigation

### Accessing Admin Features

Admin features are consolidated in a dropdown menu in the navigation bar:

```
┌──────────────────────────────────────────────────────────────┐
│ DishtaYantra  Dashboard  DAG Designer  Cache  [Admin ▼] Help │
│                                              ├─────────────┤ │
│                                              │ User Mgmt   │ │
│                                              │ System Mon  │ │
│                                              │─────────────│ │
│                                              │ System Logs │ │
│                                              └─────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

The Admin dropdown contains:
- **User Management** - Create, edit, delete users
- **System Monitoring** - Real-time system metrics
- **System Logs** - View and download application logs

---

## System Monitoring

### Accessing System Monitoring

Navigate to **Admin → System Monitoring** or directly to `/admin/monitoring`.

### Dashboard Overview

The system monitoring dashboard displays real-time metrics organized in cards:

```
┌─────────────────────────────────────────────────────────────────┐
│  System Monitoring                    [Auto-refresh ✓] [Refresh]│
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │   CPU    │  │  Memory  │  │   Disk   │  │  System  │        │
│  │   45%    │  │   67%    │  │   34%    │  │  Info    │        │
│  │ 8 cores  │  │ 16GB tot │  │ 100GB    │  │ Ubuntu   │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│                                                                  │
│  ┌────────────────────────┐  ┌────────────────────────┐        │
│  │   DAG Server Stats     │  │   Application Stats    │        │
│  │ Total: 5  Running: 3   │  │ PID: 12345            │        │
│  │ Stopped: 1  Error: 1   │  │ Memory: 256MB         │        │
│  │ Threads: 15            │  │ Threads: 12           │        │
│  └────────────────────────┘  └────────────────────────┘        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Metrics Displayed

#### CPU Metrics
| Metric | Description |
|--------|-------------|
| Usage % | Current CPU utilization |
| Core Count | Number of CPU cores |
| Load Avg (1m) | 1-minute load average |

#### Memory Metrics
| Metric | Description |
|--------|-------------|
| Usage % | Current memory utilization |
| Total | Total system memory |
| Available | Free memory available |

#### Disk Metrics
| Metric | Description |
|--------|-------------|
| Usage % | Current disk utilization |
| Total | Total disk space |
| Free | Available disk space |

#### System Info
| Metric | Description |
|--------|-------------|
| Hostname | Server hostname |
| Platform | OS and version |
| Python | Python version |
| Uptime | System uptime |

#### DAG Server Statistics
| Metric | Description |
|--------|-------------|
| Total DAGs | Number of loaded DAGs |
| Running | Currently executing DAGs |
| Stopped | Stopped DAGs |
| Error | DAGs in error state |
| Active Threads | Thread count |

#### Application Statistics
| Metric | Description |
|--------|-------------|
| App Version | DishtaYantra version |
| Process ID | Current PID |
| Process Memory | Memory usage |
| Thread Count | Application threads |
| Open Files | File descriptors |

### Health Checks

The dashboard includes service health checks:

| Service | Status | Description |
|---------|--------|-------------|
| DAG Server | ● Healthy | Server is running |
| System Metrics (psutil) | ● Healthy | Metrics collection available |
| Java Integration (Py4J) | ● Unknown | Library not installed |
| REST Calculator Support | ● Healthy | requests library available |

Status indicators:
- 🟢 **Healthy** - Service operational
- 🟡 **Unknown** - Cannot determine status
- 🔴 **Unhealthy** - Service unavailable

### Calculator Integrations Status

Shows availability of multi-language calculator support:

| Integration | Status | Requirements |
|-------------|--------|--------------|
| Java (Py4J) | Available/Not Installed | py4j package |
| C++ (pybind11) | Available/Not Compiled | Compiled .so module |
| Rust (PyO3) | Available/Not Compiled | Compiled module |
| REST (requests) | Available/Not Installed | requests package |

### Python Environment

| Metric | Description |
|--------|-------------|
| Python Version | e.g., 3.13.1 |
| Flask Version | e.g., 3.1.2 |
| Free-Threading | Enabled/Disabled |
| GIL Status | Enabled/Disabled |
| Executable | Python path |

### Auto-Refresh

- Toggle auto-refresh with the checkbox
- Metrics update every 5 seconds when enabled
- Manual refresh available via "Refresh Now" button

### Top Processes

Displays top 10 processes by memory usage:

| PID | Name | CPU % | Memory % | Status |
|-----|------|-------|----------|--------|
| 1234 | python | 5.2% | 2.1% | running |
| 5678 | gunicorn | 3.1% | 1.5% | running |

### Disk Partitions

Shows mounted disk partitions with usage:

```
/
├── Progress bar: ████████░░░░░░░░ 52%
├── Used: 52GB
├── Free: 48GB
└── Total: 100GB
```

### Network Statistics

| Metric | Description |
|--------|-------------|
| Bytes Sent | Total bytes transmitted |
| Bytes Received | Total bytes received |
| Active Connections | Current network connections |
| Errors | Network error count |

---

## System Logs

### Accessing System Logs

Navigate to **Admin → System Logs** or directly to `/admin/logs`.

### Log Viewer Interface

```
┌─────────────────────────────────────────────────────────────────┐
│  System Logs                                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [Total: 1234] [Errors: 12] [Warnings: 45] [Info: 1177]         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Log Output                                                │   │
│  │ ┌────────────┬─────────────┬─────────────────┬──────────┐│   │
│  │ │dagserver.log│application│ Search...        │All|I|W|E ││   │
│  │ └────────────┴─────────────┴─────────────────┴──────────┘│   │
│  │                                                           │   │
│  │ 2025-12-18 12:00:01 INFO  DAG my_dag started             │   │
│  │ 2025-12-18 12:00:02 INFO  Processing node input_node     │   │
│  │ 2025-12-18 12:00:03 WARN  Queue depth high: 85%          │   │
│  │ 2025-12-18 12:00:04 ERROR Connection timeout to Kafka    │   │
│  │ ...                                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Log Files

| File | Description |
|------|-------------|
| dagserver.log | DAG server operations |
| application.log | Application events |
| error.log | Error-level messages only |

### Filtering Logs

#### By Level
- **All** - Show all log entries
- **Info** - Information messages
- **Warning** - Warning messages
- **Error** - Error messages only

#### By Search
- Enter text in search box
- Filters in real-time
- Combines with level filter

### Log Entry Format

```
TIMESTAMP - LEVEL - MESSAGE
2025-12-18 12:00:01 - INFO - DAG my_dag started successfully
```

Color coding:
- **DEBUG** - Gray
- **INFO** - Blue
- **WARNING** - Yellow/Orange
- **ERROR** - Red
- **CRITICAL** - Red background

### Log Statistics

| Stat | Description |
|------|-------------|
| Total | Total log lines |
| Errors | Error-level entries |
| Warnings | Warning-level entries |
| Info | Info-level entries |
| File Size | Log file size |

### Downloading Logs

Click the download button to save logs:
- Filename format: `{logfile}_{timestamp}.log`
- Example: `dagserver_20251218_120000.log`

### Refreshing Logs

Click the refresh button to reload log entries from disk.

---

## User Management

### Accessing User Management

Navigate to **Admin → User Management** or directly to `/users`.

### User List

| Username | Full Name | Roles | Actions |
|----------|-----------|-------|---------|
| admin | System Admin | admin, user | Edit, Delete |
| operator | DAG Operator | user | Edit, Delete |

### Creating Users

1. Click "Create User"
2. Fill in:
   - Username (required)
   - Password (required)
   - Full Name (optional)
   - Roles (select admin and/or user)
3. Click "Create"

### Editing Users

1. Click "Edit" on user row
2. Modify fields:
   - Full Name
   - Password (leave blank to keep)
   - Roles
3. Click "Save"

### Deleting Users

1. Click "Delete" on user row
2. Confirm deletion
3. Note: Cannot delete yourself

### Roles

| Role | Permissions |
|------|-------------|
| admin | All operations, user management, system monitoring |
| user | View DAGs, start/stop/clone, cache management |

---

## Requirements

### System Monitoring Dependencies

```bash
# Required for full functionality
pip install psutil

# Already included in requirements.txt
# psutil==6.1.0
```

Without psutil, monitoring shows limited information.

### Log File Configuration

Logs are stored in the `logs/` directory:
- `logs/dagserver.log`
- `logs/application.log`
- `logs/error.log`

Configure logging in your application:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dagserver.log'),
        logging.StreamHandler()
    ]
)
```

---

## API Endpoints

### System Monitoring API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/monitoring` | GET | Monitoring dashboard |
| `/admin/monitoring/api` | GET | JSON metrics (for auto-refresh) |

Example API response:
```json
{
  "cpu_percent": 45.2,
  "memory_percent": 67.8,
  "disk_percent": 34.1,
  "current_time": "2025-12-18 12:00:00"
}
```

### System Logs API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/logs` | GET | Log viewer page |
| `/admin/logs/api` | GET | JSON log entries |
| `/admin/logs/download` | GET | Download log file |

Query parameters:
- `file` - Log file name (dagserver, application, error)

---

## Troubleshooting

### Monitoring Not Working

| Issue | Solution |
|-------|----------|
| All metrics show N/A | Install psutil: `pip install psutil` |
| Permission denied | Run with appropriate user permissions |
| Load average N/A | Not available on Windows |

### Logs Not Appearing

| Issue | Solution |
|-------|----------|
| Empty log viewer | Check logs/ directory exists |
| Permission denied | Check file permissions |
| Old logs only | Click Refresh button |

### Access Denied

| Issue | Solution |
|-------|----------|
| Cannot access admin | Ensure user has admin role |
| Login required | Session may have expired |

---

## Best Practices

1. **Monitor regularly** - Check system health daily
2. **Set up alerts** - Configure external monitoring for production
3. **Rotate logs** - Implement log rotation for large deployments
4. **Review errors** - Check error logs after deployments
5. **Track trends** - Monitor memory/CPU trends over time
6. **Clean up** - Archive old logs periodically

---

## Security Considerations

1. **Restrict admin access** - Only essential personnel
2. **Audit logging** - Log admin actions
3. **Secure logs** - Protect log files from unauthorized access
4. **Network security** - Restrict admin endpoints to internal network
5. **Session timeout** - Configure appropriate session expiry

---

## See Also

- [User Management Architecture](User%20Management%20Architecture.md)
- [Multi-Role User System](Multi-Role%20User%20System%20-%20Implementation%20Summary.md)
- [Help: Getting Started](/help/getting-started)

---

© 2025-2030 Ashutosh Sinha. All rights reserved.
