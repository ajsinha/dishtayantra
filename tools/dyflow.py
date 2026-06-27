#!/usr/bin/env python3
"""
dyflow - command-line client for DishtaYantra Flow Time-Travel history.

Queries and downloads the recorded DAG change-log from a running server, over
the same auth the web UI and dyadmin use (API key, or a logged-in session).
The 'download' command streams the export straight to a file, so a 24h window
never has to fit in memory.

Auth/connection (flags or environment):
    --url        server base URL          (env DY_URL,        default http://localhost:5002)
    --api-key    API key (Bearer)         (env DY_API_KEY)
    --username   admin username           (env DY_ADMIN_USER)
    --password   admin password           (env DY_ADMIN_PASSWORD)
    --dry-run    print the HTTP request that WOULD be sent, then exit
    --json       print raw JSON responses instead of the formatted summary
    --timeout    per-request timeout seconds (default 60)

Time windows (anywhere a --from/--to/--last is accepted):
    --last 24h          relative to now: <int><s|m|h|d>     (e.g. 90m, 2d)
    --from / --to       absolute: ISO 8601 (2026-06-22T09:30), epoch ms, or epoch s
    (default window, if none given, is the last 24h)

Commands:
    status                              recorder on/off, queue depth, drops, store
    enable | disable [--dag DAG]         turn recording on/off at runtime (admin);
                                         omit --dag for all DAGs, or name one DAG
    alerts                              SLO / staleness alert status
    dags [--last 24h]                   list DAGs that have history in the window
    events <dag> [window] [--nodes a,b] [--limit N] [--after SEQ]
                                        fetch events as JSON
    state-at <dag> --at <when>          reconstruct latest output per node at a time
    download <dag> [window] [--nodes a,b] [--format jsonl|csv] [-o FILE]
                                        stream the window to a file (or stdout)

Examples:
    dyflow status
    dyflow dags --last 24h
    dyflow events trade_etl --last 1h --nodes validate,fx --limit 500
    dyflow state-at trade_etl --at 2026-06-22T10:00
    dyflow download trade_etl --last 24h --format csv -o flow.csv
    dyflow --dry-run download trade_etl --last 30m
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    import requests
except ImportError:  # pragma: no cover
    sys.exit("dyflow requires the 'requests' package (pip install requests).")

DEFAULT_URL = "http://localhost:5002"
DAY_MS = 24 * 60 * 60 * 1000
_UNIT_MS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


# --------------------------------------------------------------------------
# Time parsing: relative durations, ISO 8601, or epoch (ms or s)
# --------------------------------------------------------------------------
def parse_duration_ms(text):
    """'90m' -> 5_400_000. Accepts s/m/h/d. Raises ValueError otherwise."""
    t = (text or "").strip().lower()
    if len(t) < 2 or t[-1] not in _UNIT_MS or not t[:-1].isdigit():
        raise ValueError(f"bad duration '{text}' (use e.g. 30s, 90m, 24h, 2d)")
    return int(t[:-1]) * _UNIT_MS[t[-1]]


def parse_when_ms(text, now_ms):
    """Absolute time -> epoch ms. ISO 8601, epoch ms (>=1e12), or epoch s."""
    t = (text or "").strip()
    if not t:
        raise ValueError("empty timestamp")
    if t.isdigit():
        n = int(t)
        # 13+ digits is already ms; 10-12 digits is seconds -> scale up.
        return n if n >= 1_000_000_000_000 else n * 1000
    iso = t.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        raise ValueError(f"unrecognized time '{text}' (use ISO 8601 or epoch)")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def resolve_window(args, now_ms):
    """Turn --last / --from / --to into (t0_ms, t1_ms). Default = last 24h."""
    if getattr(args, "last", None):
        span = parse_duration_ms(args.last)
        return now_ms - span, now_ms
    t1 = parse_when_ms(args.to, now_ms) if getattr(args, "to", None) else now_ms
    if getattr(args, "from_", None):
        t0 = parse_when_ms(args.from_, now_ms)
    else:
        t0 = t1 - DAY_MS
    return t0, t1


def _fmt_ts(ms):
    try:
        return datetime.fromtimestamp(ms / 1000, timezone.utc) \
            .strftime("%Y-%m-%d %H:%M:%SZ")
    except (TypeError, ValueError, OverflowError):
        return str(ms)


# --------------------------------------------------------------------------
# HTTP client (mirrors tools/dyadmin.py: API key Bearer, or session login)
# --------------------------------------------------------------------------
class FlowClient:
    def __init__(self, url, username, password, api_key, timeout=60,
                 dry_run=False, raw_json=False):
        self.base = url.rstrip("/")
        self.username = username
        self.password = password
        self.api_key = api_key
        self.timeout = timeout
        self.dry_run = dry_run
        self.raw_json = raw_json
        self.session = requests.Session()
        if api_key:
            self.session.headers["Authorization"] = f"Bearer {api_key}"
            self._logged_in = True
        else:
            self._logged_in = False

    def _full(self, path):
        return self.base + path

    @staticmethod
    def _qs(params):
        items = [(k, v) for k, v in (params or {}).items() if v not in (None, "")]
        if not items:
            return ""
        from urllib.parse import urlencode
        return "?" + urlencode(items)

    def request(self, method, path, params=None, stream=False):
        url = self._full(path) + self._qs(params)
        if self.dry_run:
            print(f"{method} {url}")
            return None
        self._ensure_login()
        resp = self.session.request(method, url, timeout=self.timeout,
                                    allow_redirects=False, stream=stream)
        loc = resp.headers.get("location", "")
        if resp.status_code in (301, 302, 303, 307, 308) and "/login" in loc:
            sys.exit("Authentication required or session expired. Pass "
                     "--api-key or --username/--password.")
        if resp.status_code in (401, 403):
            sys.exit(f"Not authorized ({resp.status_code}).")
        if resp.status_code >= 400:
            sys.exit(f"Server error [{resp.status_code}]: {resp.text[:300]}")
        return resp

    def _ensure_login(self):
        if self._logged_in or self.api_key:
            return
        if not self.username or not self.password:
            sys.exit("Missing credentials: pass --api-key (or DY_API_KEY), or "
                     "--username/--password (or DY_ADMIN_USER / DY_ADMIN_PASSWORD).")
        resp = self.session.post(self._full("/login"),
                                 data={"username": self.username,
                                       "password": self.password},
                                 timeout=self.timeout, allow_redirects=False)
        loc = resp.headers.get("location", "")
        ok = resp.status_code in (301, 302, 303, 307, 308) and "/login" not in loc
        if not ok:
            sys.exit("Login failed: invalid credentials or missing admin role.")
        self._logged_in = True

    def get_json(self, path, params=None):
        resp = self.request("GET", path, params=params)
        if resp is None:
            return None
        try:
            return resp.json()
        except ValueError:
            sys.exit(f"Expected JSON from {path}, got: {resp.text[:200]}")

    def emit(self, payload, formatter=None):
        if payload is None:
            return
        if self.raw_json or formatter is None:
            print(json.dumps(payload, indent=2, default=str))
        else:
            formatter(payload)


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def cmd_status(c, _args):
    c.emit(c.get_json("/api/flow/status"), _fmt_status)


def cmd_alerts(c, _args):
    c.emit(c.get_json("/api/alerts"), _fmt_alerts)


def cmd_enable(c, args):
    dag = getattr(args, "dag", None)
    params = {"dag": dag} if dag else None
    if c.request("POST", "/api/flow/enable", params=params) is not None:
        print("Flow recording ENABLED" + (f" for dag={dag}." if dag else " (all DAGs)."))


def cmd_disable(c, args):
    dag = getattr(args, "dag", None)
    params = {"dag": dag} if dag else None
    if c.request("POST", "/api/flow/disable", params=params) is not None:
        print("Flow recording DISABLED" + (f" for dag={dag}." if dag else " (all DAGs)."))


def cmd_dags(c, args):
    t0, t1 = resolve_window(args, _now_ms())
    c.emit(c.get_json("/api/flow/dags", {"from": t0, "to": t1}), _fmt_dags)


def cmd_events(c, args):
    t0, t1 = resolve_window(args, _now_ms())
    params = {"from": t0, "to": t1, "nodes": args.nodes,
              "limit": args.limit, "after": args.after}
    c.emit(c.get_json(f"/api/flow/{args.dag}", params), _fmt_events)


def cmd_state_at(c, args):
    ts = parse_when_ms(args.at, _now_ms())
    c.emit(c.get_json(f"/api/flow/{args.dag}/state-at", {"ts": ts}), _fmt_state)


def cmd_download(c, args):
    t0, t1 = resolve_window(args, _now_ms())
    fmt = (args.format or "jsonl").lower()
    if fmt not in ("jsonl", "csv"):
        sys.exit("--format must be jsonl or csv")
    params = {"from": t0, "to": t1, "nodes": args.nodes,
              "format": fmt, "download": 1}
    resp = c.request("GET", f"/api/flow/{args.dag}/export", params=params,
                     stream=True)
    if resp is None:  # dry-run already printed the URL
        return
    out_path = args.output or f"flow_{args.dag}_{t0}-{t1}.{fmt}"
    written = 0
    if out_path == "-":
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                sys.stdout.buffer.write(chunk)
                written += len(chunk)
        sys.stdout.buffer.flush()
    else:
        with open(out_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    fh.write(chunk)
                    written += len(chunk)
        print(f"Wrote {written:,} bytes to {out_path}  "
              f"(window {_fmt_ts(t0)} .. {_fmt_ts(t1)})")


def _now_ms():
    return int(time.time() * 1000)


# --------------------------------------------------------------------------
# Formatters
# --------------------------------------------------------------------------
def _fmt_status(p):
    print("Flow recorder:")
    for k in ("enabled", "store", "queued", "maxsize", "fired", "written",
              "dropped", "errors", "sample_rate"):
        if k in p:
            print(f"  {k:12}: {p[k]}")
    ov = p.get("dag_overrides") or {}
    if ov:
        print("  per-DAG overrides:")
        for dag, on in sorted(ov.items()):
            print(f"    {dag:20}: {'on' if on else 'off'}")


def _fmt_alerts(p):
    if not p.get("enabled"):
        print("SLO alerting: disabled (set alerts.enabled=true + alerts.rules_file)")
        return
    s = p.get("summary") or {}
    print(f"SLO alerts: {s.get('total',0)} rule(s) - "
          f"{s.get('ok',0)} OK, {s.get('breach',0)} BREACH")
    rows = p.get("alerts") or []
    if not rows:
        print("  (no rules configured)")
        return
    print(f"  {'STATUS':7} {'RULE':28} {'TARGET':28} {'AGE':>8}  reason")
    for a in rows:
        tgt = a["dag"] + (("/" + a["node"]) if a.get("node") else "")
        age = a.get("age_seconds")
        age_s = "-" if age is None else (f"{int(age)}s" if age < 120
                                         else f"{int(age // 60)}m")
        print(f"  {a['status']:7} {a['name'][:28]:28} {tgt[:28]:28} "
              f"{age_s:>8}  {a.get('reason','')}")


def _fmt_dags(p):
    dags = p.get("dags", p) if isinstance(p, dict) else p
    if not dags:
        print("No DAG history in this window.")
        return
    print(f"{'dag_id':32} {'events':>10}  window")
    for d in dags:
        if isinstance(d, dict):
            print(f"{d.get('dag_id',''):32} {d.get('count',0):>10}  "
                  f"{_fmt_ts(d.get('min_ts'))} .. {_fmt_ts(d.get('max_ts'))}")
        else:
            print(d)


def _fmt_events(p):
    events = p.get("events", p) if isinstance(p, dict) else p
    if not events:
        print("No events in this window.")
        return
    print(f"{'seq':>8} {'time':22} {'node':18} -> targets")
    for e in events:
        tgt = ",".join(e.get("targets") or [])
        print(f"{e.get('seq',''):>8} {_fmt_ts(e.get('ts_ms')):22} "
              f"{str(e.get('node_id','')):18} -> {tgt}")
    print(f"\n{len(events)} event(s).")


def _fmt_state(p):
    nodes = p.get("nodes", {})
    print(f"State at {_fmt_ts(p.get('ts_ms'))}  ({len(nodes)} node(s)):")
    for name, ev in nodes.items():
        out = json.dumps(ev.get("output"), default=str)
        if len(out) > 80:
            out = out[:77] + "..."
        print(f"  {name:18}: {out}")


# --------------------------------------------------------------------------
# Argument parsing
# --------------------------------------------------------------------------
def _add_window_flags(sp):
    sp.add_argument("--last", help="relative window, e.g. 90m, 24h, 2d")
    sp.add_argument("--from", dest="from_", help="window start (ISO or epoch)")
    sp.add_argument("--to", help="window end (ISO or epoch)")


def build_parser():
    p = argparse.ArgumentParser(prog="dyflow",
                                description="DishtaYantra Flow Time-Travel CLI.")
    p.add_argument("--url", default=os.environ.get("DY_URL", DEFAULT_URL))
    p.add_argument("--api-key", default=os.environ.get("DY_API_KEY"))
    p.add_argument("--username", default=os.environ.get("DY_ADMIN_USER"))
    p.add_argument("--password", default=os.environ.get("DY_ADMIN_PASSWORD"))
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--dry-run", action="store_true",
                   help="print the HTTP request that would be sent, then exit")
    p.add_argument("--json", dest="raw_json", action="store_true",
                   help="print raw JSON instead of the formatted summary")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="recorder status").set_defaults(fn=cmd_status)
    sub.add_parser("alerts", help="SLO / staleness alert status").set_defaults(fn=cmd_alerts)
    pe = sub.add_parser("enable", help="turn recording on (admin); --dag for one DAG")
    pe.add_argument("--dag", help="only this DAG (default: all DAGs)")
    pe.set_defaults(fn=cmd_enable)
    pd = sub.add_parser("disable", help="turn recording off (admin); --dag for one DAG")
    pd.add_argument("--dag", help="only this DAG (default: all DAGs)")
    pd.set_defaults(fn=cmd_disable)

    sp = sub.add_parser("dags", help="list DAGs with history in a window")
    _add_window_flags(sp)
    sp.set_defaults(fn=cmd_dags)

    sp = sub.add_parser("events", help="fetch events as JSON")
    sp.add_argument("dag")
    _add_window_flags(sp)
    sp.add_argument("--nodes", help="comma-separated node filter")
    sp.add_argument("--limit", type=int, default=5000)
    sp.add_argument("--after", type=int, help="only seq greater than this")
    sp.set_defaults(fn=cmd_events)

    sp = sub.add_parser("state-at", help="reconstruct state at a point in time")
    sp.add_argument("dag")
    sp.add_argument("--at", required=True, help="ISO 8601 or epoch")
    sp.set_defaults(fn=cmd_state_at)

    sp = sub.add_parser("download", help="stream a window to a file")
    sp.add_argument("dag")
    _add_window_flags(sp)
    sp.add_argument("--nodes", help="comma-separated node filter")
    sp.add_argument("--format", default="jsonl", help="jsonl (default) or csv")
    sp.add_argument("-o", "--output", help="output file ('-' for stdout)")
    sp.set_defaults(fn=cmd_download)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    client = FlowClient(args.url, args.username, args.password, args.api_key,
                        timeout=args.timeout, dry_run=args.dry_run,
                        raw_json=args.raw_json)
    try:
        args.fn(client, args)
    except ValueError as e:
        sys.exit(f"Error: {e}")
    except requests.RequestException as e:
        sys.exit(f"Connection error: {e}")


if __name__ == "__main__":
    main()
