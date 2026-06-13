"""
Admin Routes - Log Viewing & Live Streaming (FastAPI, v2.0.0)
=============================================================

Admin-only log pages: static viewer with statistics, raw-file download, and
the live Server-Sent-Events stream that tails ``logs/dagserver.log``,
``logs/application.log`` and ``logs/error.log``.  Split out of the
monitoring module per the v2.0.0 architecture mandate (logical grouping,
small files).

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import os
import time
import traceback
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from routes.admin_routes import format_bytes
from web.fastapi_compat import AuthGuards, flash, redirect_to, render

logger = logging.getLogger(__name__)

# Logical name -> path mapping shared by all log endpoints.
LOG_PATHS = {
    'dagserver': 'logs/dagserver.log',
    'application': 'logs/application.log',
    'error': 'logs/error.log',
}

VALID_LEVELS = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')


class AdminLogRoutes:
    """Admin log viewing and live streaming routes handler."""

    def __init__(self, app: FastAPI, guards: AuthGuards):
        self.app = app
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/admin/logs', self.system_logs, methods=['GET'],
            name='system_logs', include_in_schema=False)
        add('/admin/logs/api', self.system_logs_api, methods=['GET'],
            name='system_logs_api')
        add('/admin/logs/download', self.download_logs, methods=['GET'],
            name='download_logs', include_in_schema=False)
        add('/admin/logs/live', self.live_logs, methods=['GET'],
            name='live_logs', include_in_schema=False)
        add('/admin/logs/live/stream', self.live_logs_stream,
            methods=['GET'], name='live_logs_stream',
            include_in_schema=False)

    # ------------------------------------------------------------------ #
    # Parsing helpers
    # ------------------------------------------------------------------ #

    def _parse_log_file(self, log_path, max_lines=500):
        """Parse the tail of a log file into structured entries."""
        entries = []
        if not os.path.exists(log_path):
            return entries
        try:
            with open(log_path, 'r', encoding='utf-8',
                      errors='ignore') as f:
                lines = f.readlines()
            lines = lines[-max_lines:]

            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Expected: TIMESTAMP - LEVEL - MESSAGE
                entry = {'timestamp': '', 'level': 'INFO', 'message': line}
                if ' - ' in line:
                    parts = line.split(' - ', 2)
                    if len(parts) >= 2:
                        entry['timestamp'] = parts[0][:19]
                        if len(parts) >= 3:
                            level = parts[1].strip().upper()
                            if level in VALID_LEVELS:
                                entry['level'] = level
                                entry['message'] = parts[2]
                            else:
                                entry['message'] = ' - '.join(parts[1:])
                        else:
                            entry['message'] = parts[1]
                entries.append(entry)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error parsing log file {log_path}: {e}")
            logger.error(traceback.format_exc())
        return entries

    @staticmethod
    def _get_log_stats(entries):
        """Calculate counters for the viewer's statistics cards."""
        return {'total': len(entries),
                'errors': sum(1 for e in entries if e['level'] == 'ERROR'),
                'warnings': sum(1 for e in entries
                                if e['level'] == 'WARNING'),
                'info': sum(1 for e in entries if e['level'] == 'INFO'),
                'file_size': 'N/A'}

    def _parse_log_line(self, line, source_file):
        """Parse one log line into the structured live-stream format."""
        try:
            # Common format: 2024-01-15 10:30:45,123 - INFO - message
            parts = line.split(' - ', 2)
            if len(parts) >= 3:
                timestamp = parts[0].strip()
                level = parts[1].strip().upper()
                message = parts[2].strip()
            elif len(parts) == 2:
                head = parts[0].strip().upper()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                level = head if head in VALID_LEVELS else 'INFO'
                message = parts[1].strip() if head in VALID_LEVELS else line
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                level = 'INFO'
                message = line.strip()

            if level not in VALID_LEVELS:
                level = 'INFO'
            source_name = os.path.basename(source_file).replace('.log', '')
            return {'timestamp': timestamp, 'level': level,
                    'message': message, 'source': source_name}
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error parsing log line: {e}")
            logger.error(traceback.format_exc())
            return {'timestamp': datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'),
                    'level': 'INFO', 'message': line.strip(),
                    'source': 'unknown'}

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #

    def system_logs(self, request: Request):
        """System logs viewer page."""
        self.guards.admin_required(request)
        log_file = request.query_params.get('file', 'dagserver')
        log_path = LOG_PATHS.get(log_file, LOG_PATHS['dagserver'])
        entries = self._parse_log_file(log_path)
        stats = self._get_log_stats(entries)
        if os.path.exists(log_path):
            stats['file_size'] = format_bytes(os.path.getsize(log_path))
        return render(request, 'admin/system_logs.html',
                      log_entries=entries,
                      log_stats=stats,
                      selected_file=log_file)

    def system_logs_api(self, request: Request):
        """API endpoint for log refreshes."""
        self.guards.admin_required(request)
        log_file = request.query_params.get('file', 'dagserver')
        log_path = LOG_PATHS.get(log_file, LOG_PATHS['dagserver'])
        entries = self._parse_log_file(log_path)
        return JSONResponse({'entries': entries, 'total': len(entries)})

    def download_logs(self, request: Request):
        """Download the selected raw log file."""
        self.guards.admin_required(request)
        log_file = request.query_params.get('file', 'dagserver')
        log_path = LOG_PATHS.get(log_file, LOG_PATHS['dagserver'])
        if os.path.exists(log_path):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            return FileResponse(
                log_path,
                media_type='text/plain',
                filename=f'{log_file}_{timestamp}.log')
        flash(request, 'Log file not found.', 'error')
        return redirect_to(request, 'system_logs')

    def live_logs(self, request: Request):
        """Render the live logs page."""
        self.guards.admin_required(request)
        return render(request, 'admin/live_logs.html')

    def live_logs_stream(self, request: Request):
        """Server-Sent Events endpoint for live log streaming."""
        self.guards.admin_required(request)

        def generate_log_events():
            """Tail all known log files and yield SSE data frames."""
            log_files = list(LOG_PATHS.values())

            # Track file positions (start at the current end of file)
            file_positions = {}
            for log_file in log_files:
                file_positions[log_file] = os.path.getsize(log_file) \
                    if os.path.exists(log_file) else 0

            # Send initial heartbeat
            yield (f"data: "
                   f"{json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})}"
                   f"\n\n")

            while True:
                try:
                    new_entries = []
                    for log_file in log_files:
                        if not os.path.exists(log_file):
                            continue
                        current_size = os.path.getsize(log_file)
                        # Detect truncation/rotation
                        if current_size < file_positions[log_file]:
                            file_positions[log_file] = 0
                        if current_size > file_positions[log_file]:
                            try:
                                with open(log_file, 'r', encoding='utf-8',
                                          errors='replace') as f:
                                    f.seek(file_positions[log_file])
                                    new_content = f.read()
                                    file_positions[log_file] = current_size
                                    for line in \
                                            new_content.strip().split('\n'):
                                        if line.strip():
                                            entry = self._parse_log_line(
                                                line, log_file)
                                            if entry:
                                                new_entries.append(entry)
                            except Exception as e:  # noqa: BLE001
                                logger.error(f"Error reading log file "
                                             f"{log_file}: {e}")
                                logger.error(traceback.format_exc())

                    for entry in new_entries:
                        yield f"data: {json.dumps(entry)}\n\n"

                    if not new_entries:
                        # Small sleep to prevent CPU spinning
                        time.sleep(0.5)
                except GeneratorExit:
                    # Client disconnected
                    break
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Error in log stream: {e}")
                    logger.error(traceback.format_exc())
                    time.sleep(1)

        return StreamingResponse(
            generate_log_events(),
            media_type='text/event-stream',
            headers={'Cache-Control': 'no-cache',
                     'Connection': 'keep-alive',
                     'X-Accel-Buffering': 'no'})
