#!/usr/bin/env python3
"""
dyadmin - command-line admin client for a running DishtaYantra server.

Talks to the server's HTTP admin API over a logged-in session (the same auth the
web UI uses: POST /login -> signed-cookie session). Every admin action exposed in
the web UI is available here for scripting and automation.

Auth/connection (flags or environment):
    --url        server base URL         (env DY_URL,            default http://localhost:5002)
    --username   admin username          (env DY_ADMIN_USER)
    --password   admin password          (env DY_ADMIN_PASSWORD)
    --dry-run    print the HTTP request that WOULD be sent, then exit (no login needed)
    --json       print raw JSON responses instead of the formatted summary
    --timeout    per-request timeout seconds (default 30)

Command groups:
    monitor                     system metrics + free-threading status
    dag                         list / start / stop / suspend / resume / reload
    maintenance                 drain status / freeze / unfreeze (maintenance windows)
    logging                     set runtime log levels (root or a single logger)
    logs                        tail / summarize server logs
    workers                     worker-pool status and control
    native                      JVM / C++ / Rust module + gateway management

Examples:
    dyadmin monitor
    dyadmin dag list
    dyadmin dag stop perftest_trade_etl_arrow
    dyadmin maintenance freeze --scope global
    dyadmin maintenance status
    dyadmin logging set --scope logger --logger core.dag --level DEBUG
    dyadmin workers status
    dyadmin native status jvm
    dyadmin --dry-run maintenance unfreeze --scope dag --dag orders
"""

import argparse
import json
import os
import sys

try:
    import requests
except ImportError:  # pragma: no cover
    sys.exit("dyadmin requires the 'requests' package (pip install requests).")

DEFAULT_URL = "http://localhost:5002"
VALID_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
RUNTIMES = ("jvm", "cpp", "rust")


class AdminClient:
    """Thin HTTP client: logs in once, then issues admin requests."""

    def __init__(self, url, username, password, timeout=30,
                 dry_run=False, raw_json=False, api_key=None):
        self.base = url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.dry_run = dry_run
        self.raw_json = raw_json
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            # API-key auth: send it on every request; no session login needed.
            self.session.headers["Authorization"] = f"Bearer {api_key}"
            self._logged_in = True
        else:
            self._logged_in = False

    # -- transport -------------------------------------------------------
    def _full(self, path):
        return self.base + path

    def request(self, method, path, data=None):
        """Issue a request (or, under --dry-run, print and return None)."""
        url = self._full(path)
        if self.dry_run:
            line = f"{method} {url}"
            if data:
                line += "  form=" + json.dumps(data)
            print(line)
            return None
        self._ensure_login()
        return self._send(method, path, data)

    def _send(self, method, path, data=None):
        url = self._full(path)
        resp = self.session.request(method, url, data=data,
                                    timeout=self.timeout, allow_redirects=False)
        # Auth failures surface as a redirect to /login (HTML) or 401/403 (/api).
        loc = resp.headers.get("location", "")
        if resp.status_code in (301, 302, 303, 307, 308) and "/login" in loc:
            sys.exit("Authentication required or session expired (redirected to "
                     "login). Check --username/--password and the admin role.")
        if resp.status_code in (401, 403):
            sys.exit(f"Not authorized ({resp.status_code}). The user must hold the "
                     f"'admin' role.")
        return resp

    def _ensure_login(self):
        if self._logged_in:
            return
        if self.api_key:
            return
        if not self.username or not self.password:
            sys.exit("Missing credentials: pass --api-key (or DY_API_KEY), or "
                     "--username/--password (or DY_ADMIN_USER / DY_ADMIN_PASSWORD).")
        resp = self.session.post(self._full("/login"),
                                 data={"username": self.username,
                                       "password": self.password},
                                 timeout=self.timeout, allow_redirects=False)
        # The server uses post/redirect/get: a successful login redirects AWAY
        # from /login (to the dashboard); a failed one redirects back to /login
        # (or re-renders it). So success == a redirect whose target isn't /login.
        loc = resp.headers.get("location", "")
        ok = resp.status_code in (301, 302, 303, 307, 308) and "/login" not in loc
        if not ok:
            sys.exit("Login failed: invalid username/password, or the user lacks "
                     "the 'admin' role.")
        self._logged_in = True

    # -- output ----------------------------------------------------------
    def show(self, resp, formatter=None):
        if resp is None:  # dry-run
            return
        if resp.status_code in (301, 302, 303, 307, 308):
            # Action endpoints use post/redirect/get. Reaching here means the
            # redirect target was NOT /login (that case exits in _send), so the
            # action was accepted by the server.
            print("OK - action applied.")
            return
        try:
            payload = resp.json()
        except ValueError:
            print(f"[{resp.status_code}] {resp.text[:500]}")
            return
        if self.raw_json or formatter is None:
            print(json.dumps(payload, indent=2, default=str))
        else:
            formatter(payload)


# --------------------------------------------------------------------------
# Formatters (compact, human-readable summaries; --json shows the full payload)
# --------------------------------------------------------------------------
def _fmt_monitor(p):
    sysm = p.get("system", p)
    print("System:")
    for k in ("cpu_percent", "memory_percent", "memory_used_mb", "process_count",
              "uptime", "gil_enabled", "free_threading", "python_version"):
        if k in sysm:
            print(f"  {k:18}: {sysm[k]}")
    if "dags" in p:
        print(f"DAGs: {len(p['dags'])}")


def _fmt_maint(p):
    print(f"all_drained    : {p.get('all_drained')}")
    print(f"any_frozen     : {p.get('any_frozen')}")
    for d in p.get("dags", []):
        print(f"  - {d.get('dag_name')}: frozen={d.get('any_frozen')} "
              f"drained={d.get('drained')} "
              f"sub_q={d.get('subscriber_queue_total')} "
              f"pub_q={d.get('publisher_queue_total')} "
              f"wal_pending={d.get('wal_pending')}")


def _fmt_workers(p):
    workers = p.get("workers", p if isinstance(p, list) else [])
    if isinstance(workers, dict):
        workers = workers.get("workers", [])
    print(f"Workers: {len(workers)}")
    for w in workers:
        print(f"  #{w.get('worker_id', w.get('id'))}: pid={w.get('pid')} "
              f"status={w.get('status')} dags={w.get('dag_count', w.get('dags'))}")


def _fmt_dags(p):
    # Sourced from /admin/maintenance/status (lists every DAG with drain/freeze
    # state); there is no separate JSON dag-list endpoint.
    dags = p.get("dags", [])
    if not dags:
        print("(no DAGs reported)")
        return
    for d in sorted(dags, key=lambda x: x.get("dag_name", "")):
        print(f"  {d.get('dag_name', '?'):40} "
              f"frozen={d.get('any_frozen')} drained={d.get('drained')}")


def _fmt_result(p):
    # generic action result
    status = p.get("status") or p.get("success")
    msg = p.get("message") or p.get("detail") or ""
    print(f"status: {status}" + (f"  {msg}" if msg else ""))


# --------------------------------------------------------------------------
# Command handlers
# --------------------------------------------------------------------------
def cmd_monitor(c, a):
    c.show(c.request("GET", "/admin/monitoring/api"), _fmt_monitor)


def cmd_dag(c, a):
    if a.dag_cmd == "list":
        c.show(c.request("GET", "/admin/maintenance/status"), _fmt_dags)
    elif a.dag_cmd == "reload":
        c.show(c.request("POST", "/dags/reload"), _fmt_result)
    else:  # start/stop/suspend/resume
        c.show(c.request("POST", f"/dag/{a.name}/{a.dag_cmd}"), _fmt_result)


def cmd_maintenance(c, a):
    if a.maint_cmd == "status":
        c.show(c.request("GET", "/admin/maintenance/status"), _fmt_maint)
    else:  # freeze | unfreeze
        form = {"action": a.maint_cmd, "scope": a.scope,
                "dag_name": a.dag or "", "subscribers": a.subscribers}
        c.show(c.request("POST", "/admin/maintenance/apply", form), _fmt_result)


def cmd_logging(c, a):
    form = {"scope": a.scope, "level": a.level.upper(), "logger": a.logger or ""}
    c.show(c.request("POST", "/admin/logging/apply", form), _fmt_result)


def cmd_logs(c, a):
    path = f"/admin/logs/api?file={a.file}"
    if a.dag:
        path += f"&dag={a.dag}"
    c.show(c.request("GET", path))


def cmd_workers(c, a):
    cmd = a.workers_cmd
    if cmd == "status":
        c.show(c.request("GET", "/api/workers/status"), _fmt_workers)
    elif cmd == "assignments":
        c.show(c.request("GET", "/api/workers/assignments"))
    elif cmd == "load":
        c.show(c.request("GET", "/api/workers/load-summary"))
    elif cmd == "info":
        c.show(c.request("GET", f"/api/workers/{a.id}"))
    elif cmd == "pool-start":
        c.show(c.request("POST", "/api/workers/pool/start"), _fmt_result)
    elif cmd == "pool-stop":
        c.show(c.request("POST", "/api/workers/pool/stop"), _fmt_result)
    elif cmd == "restart":
        c.show(c.request("POST", f"/api/workers/{a.id}/restart"), _fmt_result)
    elif cmd == "migrate":
        c.show(c.request("POST", f"/api/workers/dag/{a.dag}/migrate"), _fmt_result)


def cmd_native(c, a):
    rt = a.runtime
    cmd = a.native_cmd
    if cmd == "status":
        c.show(c.request("GET", f"/api/{rt}/status"))
    elif cmd == "calculators":
        c.show(c.request("GET", f"/api/{rt}/calculators"))
    elif cmd == "modules":
        c.show(c.request("GET", f"/api/{rt}/modules"))
    elif cmd == "reload-config":
        c.show(c.request("POST", f"/api/{rt}/reload-config"), _fmt_result)
    elif cmd in ("module-load", "module-unload", "module-reload", "module-build"):
        op = cmd.split("-", 1)[1]
        c.show(c.request("POST", f"/api/{rt}/modules/{a.name}/{op}"), _fmt_result)
    elif cmd == "gateways":
        c.show(c.request("GET", f"/api/{rt}/gateways"))
    elif cmd in ("gateway-restart", "gateway-stop", "gateway-reconnect"):
        op = cmd.split("-", 1)[1]
        c.show(c.request("POST", f"/api/{rt}/gateways/{a.name}/{op}"), _fmt_result)


# --------------------------------------------------------------------------
# Argument parser
# --------------------------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="dyadmin",
        description="Command-line admin client for a running DishtaYantra server.")
    p.add_argument("--url", default=os.environ.get("DY_URL", DEFAULT_URL),
                   help=f"server base URL (env DY_URL, default {DEFAULT_URL})")
    p.add_argument("--username", default=os.environ.get("DY_ADMIN_USER"),
                   help="admin username (env DY_ADMIN_USER)")
    p.add_argument("--password", default=os.environ.get("DY_ADMIN_PASSWORD"),
                   help="admin password (env DY_ADMIN_PASSWORD)")
    p.add_argument("--api-key", default=os.environ.get("DY_API_KEY"),
                   help="API key for auth (env DY_API_KEY); preferred for "
                        "automation. Mint one with tools/dyapikey.py. When set, "
                        "username/password are not needed.")
    p.add_argument("--timeout", type=float, default=30, help="request timeout (s)")
    p.add_argument("--dry-run", action="store_true",
                   help="print the HTTP request that would be sent, then exit")
    p.add_argument("--json", dest="raw_json", action="store_true",
                   help="print raw JSON responses")
    sub = p.add_subparsers(dest="group", required=True)

    # monitor
    sub.add_parser("monitor", help="system metrics + free-threading status"
                   ).set_defaults(func=cmd_monitor)

    # dag
    d = sub.add_parser("dag", help="DAG lifecycle")
    ds = d.add_subparsers(dest="dag_cmd", required=True)
    ds.add_parser("list", help="list DAGs and their state")
    ds.add_parser("reload", help="re-scan DAG folders and reconcile")
    for op in ("start", "stop", "suspend", "resume"):
        sp = ds.add_parser(op, help=f"{op} a DAG")
        sp.add_argument("name", help="DAG name")
    d.set_defaults(func=cmd_dag)

    # maintenance
    m = sub.add_parser("maintenance", help="drain mode / freeze for maintenance")
    ms = m.add_subparsers(dest="maint_cmd", required=True)
    ms.add_parser("status", help="show drain status")
    for op in ("freeze", "unfreeze"):
        sp = ms.add_parser(op, help=f"{op} subscribers")
        sp.add_argument("--scope", choices=("dag", "global"), default="dag")
        sp.add_argument("--dag", help="DAG name (required for --scope dag)")
        sp.add_argument("--subscribers", default="ALL",
                        help="ALL or comma-separated subscriber names")
    m.set_defaults(func=cmd_maintenance)

    # logging
    lg = sub.add_parser("logging", help="runtime log-level control")
    lgs = lg.add_subparsers(dest="logging_cmd", required=True)
    sp = lgs.add_parser("set", help="set a log level (broadcast to workers)")
    sp.add_argument("--scope", choices=("root", "logger"), default="root")
    sp.add_argument("--level", required=True, choices=VALID_LEVELS)
    sp.add_argument("--logger", help="logger name (required for --scope logger)")
    lg.set_defaults(func=cmd_logging)

    # logs
    lo = sub.add_parser("logs", help="tail / summarize server logs")
    los = lo.add_subparsers(dest="logs_cmd", required=True)
    sp = los.add_parser("tail", help="recent log entries (JSON)")
    sp.add_argument("--file", default="dagserver", help="log file key")
    sp.add_argument("--dag", help="filter by DAG name")
    lo.set_defaults(func=cmd_logs)

    # workers
    w = sub.add_parser("workers", help="worker-pool status and control")
    ws = w.add_subparsers(dest="workers_cmd", required=True)
    ws.add_parser("status", help="worker pool status")
    ws.add_parser("assignments", help="DAG -> worker assignments")
    ws.add_parser("load", help="load summary")
    ws.add_parser("pool-start", help="start the worker pool")
    ws.add_parser("pool-stop", help="stop the worker pool")
    sp = ws.add_parser("info", help="details for one worker")
    sp.add_argument("id", type=int)
    sp = ws.add_parser("restart", help="restart one worker")
    sp.add_argument("id", type=int)
    sp = ws.add_parser("migrate", help="migrate a DAG to another worker")
    sp.add_argument("dag")
    w.set_defaults(func=cmd_workers)

    # native
    n = sub.add_parser("native", help="JVM / C++ / Rust modules and gateways")
    n.add_argument("runtime", choices=RUNTIMES)
    ns = n.add_subparsers(dest="native_cmd", required=True)
    for simple in ("status", "calculators", "modules", "reload-config", "gateways"):
        ns.add_parser(simple, help=simple)
    for op in ("module-load", "module-unload", "module-reload", "module-build"):
        sp = ns.add_parser(op, help=op)
        sp.add_argument("name", help="module name")
    for op in ("gateway-restart", "gateway-stop", "gateway-reconnect"):
        sp = ns.add_parser(op, help=op)
        sp.add_argument("name", help="gateway name")
    n.set_defaults(func=cmd_native)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    client = AdminClient(args.url, args.username, args.password,
                         timeout=args.timeout, dry_run=args.dry_run,
                         raw_json=args.raw_json, api_key=args.api_key)
    args.func(client, args)


if __name__ == "__main__":
    main()
