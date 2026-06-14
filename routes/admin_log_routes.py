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

# Matches the conventional DAG-tagged message prefix used throughout the
# engine, e.g. "DAG scheduled_heartbeat: started". The name is the token(s)
# up to the first colon. Kept deliberately strict (word/.-/ chars) so a
# stray "DAG: foo" or prose mentioning "DAG" does not false-match.
import re as _re
_DAG_TAG_RE = _re.compile(r'^DAG\s+([A-Za-z0-9_.\-]+)\s*[:]\s*(.*)$',
                          _re.DOTALL)


def extract_dag_name(message):
    """Return the DAG name a log message refers to, or None.

    Recognizes the engine's "DAG <name>: ..." convention. Returns the bare
    name so the viewer can filter/group by DAG without per-DAG logfiles.
    """
    if not message:
        return None
    m = _DAG_TAG_RE.match(message.strip())
    return m.group(1) if m else None


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

    def _parse_log_file(self, log_path, max_lines=500, dag_filter=None):
        """Parse the tail of a log file into structured entries.

        When ``dag_filter`` is given, only entries attributed to that DAG
        (via the "DAG <name>:" convention) are returned.
        """
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
                entry['dag'] = extract_dag_name(entry['message'])
                if dag_filter and entry['dag'] != dag_filter:
                    continue
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
                    'message': message, 'source': source_name,
                    'dag': extract_dag_name(message)}
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
        dag_filter = request.query_params.get('dag') or None
        log_path = LOG_PATHS.get(log_file, LOG_PATHS['dagserver'])
        # Parse unfiltered once to discover which DAGs appear in the log
        # (for the filter dropdown), then apply the filter for display.
        all_entries = self._parse_log_file(log_path)
        dag_names = sorted({e['dag'] for e in all_entries if e.get('dag')})
        entries = [e for e in all_entries if e['dag'] == dag_filter] \
            if dag_filter else all_entries
        stats = self._get_log_stats(entries)
        if os.path.exists(log_path):
            stats['file_size'] = format_bytes(os.path.getsize(log_path))
        return render(request, 'admin/system_logs.html',
                      log_entries=entries,
                      log_stats=stats,
                      selected_file=log_file,
                      dag_names=dag_names,
                      selected_dag=dag_filter or '')

    def system_logs_api(self, request: Request):
        """API endpoint for log refreshes."""
        self.guards.admin_required(request)
        log_file = request.query_params.get('file', 'dagserver')
        dag_filter = request.query_params.get('dag') or None
        log_path = LOG_PATHS.get(log_file, LOG_PATHS['dagserver'])
        all_entries = self._parse_log_file(log_path)
        dag_names = sorted({e['dag'] for e in all_entries if e.get('dag')})
        entries = [e for e in all_entries if e['dag'] == dag_filter] \
            if dag_filter else all_entries
        return JSONResponse({'entries': entries, 'total': len(entries),
                             'dag_names': dag_names})

    def download_logs(self, request: Request):
        """Download the selected raw log file, optionally filtered to one DAG.

        A ``dag`` query param produces an on-demand filtered download (lines
        attributed to that DAG only) rather than maintaining a per-DAG file.
        """
        self.guards.admin_required(request)
        log_file = request.query_params.get('file', 'dagserver')
        dag_filter = request.query_params.get('dag') or None
        log_path = LOG_PATHS.get(log_file, LOG_PATHS['dagserver'])
        if not os.path.exists(log_path):
            flash(request, 'Log file not found.', 'error')
            return redirect_to(request, 'system_logs')

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if dag_filter:
            # Build a filtered text body on the fly. Read the whole file and
            # keep lines attributed to this DAG.
            lines = []
            try:
                with open(log_path, 'r', encoding='utf-8',
                          errors='ignore') as f:
                    for line in f:
                        msg = line
                        if ' - ' in line:
                            parts = line.split(' - ', 2)
                            msg = parts[2] if len(parts) >= 3 else line
                        if extract_dag_name(msg.strip()) == dag_filter:
                            lines.append(line.rstrip('\n'))
            except Exception as e:  # noqa: BLE001
                logger.error(f"Error filtering log for DAG "
                             f"{dag_filter}: {e}")
                flash(request, 'Could not build filtered log.', 'error')
                return redirect_to(request, 'system_logs')
            body = '\n'.join(lines) + ('\n' if lines else '')
            safe_dag = ''.join(c for c in dag_filter
                               if c.isalnum() or c in '_.-')
            return StreamingResponse(
                iter([body]),
                media_type='text/plain',
                headers={'Content-Disposition':
                         f'attachment; filename='
                         f'"{log_file}_{safe_dag}_{timestamp}.log"'})

        return FileResponse(
            log_path,
            media_type='text/plain',
            filename=f'{log_file}_{timestamp}.log')

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

            # Send initial heartbeat. The leading 2KB comment padding helps
            # proxies that buffer until a threshold flush the stream promptly
            # (a well-known SSE-behind-reverse-proxy workaround).
            yield ":" + (" " * 2048) + "\n\n"
            yield (f"data: "
                   f"{json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})}"
                   f"\n\n")

            # v3.1.0: backfill the most recent lines on connect so the page
            # shows recent context immediately instead of a blank screen until
            # the next log line is written. We read the tail of each known log
            # file, parse and merge, then emit the last N across all sources.
            try:
                BACKFILL_LINES = 50
                backfill = []
                for log_file in log_files:
                    if not os.path.exists(log_file):
                        continue
                    try:
                        with open(log_file, 'r', encoding='utf-8',
                                  errors='replace') as f:
                            tail = f.readlines()[-BACKFILL_LINES:]
                        for line in tail:
                            if line.strip():
                                entry = self._parse_log_line(
                                    line.rstrip('\n'), log_file)
                                if entry:
                                    backfill.append(entry)
                    except Exception:  # noqa: BLE001 - skip unreadable file
                        continue
                # Emit at most BACKFILL_LINES, marked so the client can style
                # them distinctly (historical context, not live arrivals).
                for entry in backfill[-BACKFILL_LINES:]:
                    entry['backfill'] = True
                    yield f"data: {json.dumps(entry)}\n\n"
            except Exception as e:  # noqa: BLE001
                logger.error(f"Error during live-log backfill: {e}")

            last_heartbeat = time.time()
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
                        # Emit a heartbeat every ~15s so the connection keeps
                        # producing traffic - this prevents idle-connection
                        # timeouts and prompts buffering proxies to flush.
                        now = time.time()
                        if now - last_heartbeat >= 15:
                            yield (f"data: "
                                   f"{json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})}"
                                   f"\n\n")
                            last_heartbeat = now
                        # Small sleep to prevent CPU spinning
                        time.sleep(0.5)
                    else:
                        last_heartbeat = time.time()
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
