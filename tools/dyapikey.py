#!/usr/bin/env python3
"""
dyapikey - local API-key management for DishtaYantra (run on the server host).

API keys let CLIs and automation (e.g. dyadmin --api-key) authenticate without a
username/password or a browser session. Minting a key needs direct database
access, so this tool runs locally on the server host (like createsuperuser) and
talks to the database directly rather than over HTTP - it is the bootstrap path
that breaks the chicken-and-egg of "you need a key to get a key".

A key inherits the roles of the user it belongs to: an admin user's key grants
admin access. The clear key is shown exactly once, at creation - store it
securely; it cannot be recovered afterward.

Usage (run from the project root, with the same env the server uses, e.g.
SECRET_KEY set):
    python3 tools/dyapikey.py create <username> <key_name> [--expires-days N]
    python3 tools/dyapikey.py list <username>
    python3 tools/dyapikey.py revoke <key_id> [--by <actor>]

Examples:
    python3 tools/dyapikey.py create admin ci-runner
    python3 tools/dyapikey.py list admin
    python3 tools/dyapikey.py revoke 4 --by admin
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

# Allow running as `python3 tools/dyapikey.py` from the project root: ensure the
# repo root (parent of tools/) is importable so `core.*` resolves.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _registry():
    """Initialize application config (singleton) then the user registry."""
    try:
        from core.config_parsers import find_default_config
        from core.properties_configurator import PropertiesConfigurator
        PropertiesConfigurator([find_default_config('config')])
        from core.user_registry import UserRegistry
        return UserRegistry()
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Could not initialize the registry/database: {exc}\n"
                 f"Run this from the project root on the server host, with the "
                 f"server's environment (e.g. SECRET_KEY) set.")


def cmd_create(reg, a):
    expires_at = None
    if a.expires_days:
        expires_at = (datetime.now(timezone.utc)
                      + timedelta(days=a.expires_days)).replace(tzinfo=None)
    try:
        clear_key, record = reg.create_api_key(
            a.username, a.key_name, created_by=a.by or a.username,
            expires_at=expires_at) if _accepts_expiry(reg) else \
            reg.create_api_key(a.username, a.key_name, created_by=a.by or a.username)
    except TypeError:
        # registry wrapper without expires_at support
        clear_key, record = reg.create_api_key(a.username, a.key_name,
                                               created_by=a.by or a.username)
    except ValueError as exc:
        sys.exit(str(exc))
    print("API key created. Store it now - it is shown only once:\n")
    print(f"  {clear_key}\n")
    print(f"  name    : {record.get('name')}")
    print(f"  prefix  : {record.get('key_prefix')}")
    print(f"  user    : {a.username}")
    if expires_at:
        print(f"  expires : {expires_at.isoformat()}Z")
    print("\nUse it with: dyadmin --api-key <key> ...  (or env DY_API_KEY)")


def _accepts_expiry(reg):
    import inspect
    try:
        return "expires_at" in inspect.signature(reg.create_api_key).parameters
    except (TypeError, ValueError):
        return False


def cmd_list(reg, a):
    keys = reg.list_api_keys(a.username)
    if not keys:
        print(f"(no API keys for '{a.username}')")
        return
    for k in keys:
        print(f"  id={k.get('id')}  name={k.get('name')}  "
              f"prefix={k.get('key_prefix')}  active={k.get('is_active')}  "
              f"last_used={k.get('last_used_at')}  expires={k.get('expires_at')}")


def cmd_revoke(reg, a):
    try:
        reg.revoke_api_key(a.key_id, revoked_by=a.by or "dyapikey")
    except TypeError:
        reg.revoke_api_key(a.key_id, a.by or "dyapikey")
    print(f"Revoked API key id={a.key_id}.")


def build_parser():
    p = argparse.ArgumentParser(
        prog="dyapikey",
        description="Local API-key management for DishtaYantra (run on the host).")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create", help="mint a new API key (clear key shown once)")
    c.add_argument("username")
    c.add_argument("key_name")
    c.add_argument("--expires-days", type=int, help="optional expiry in days")
    c.add_argument("--by", help="actor recorded as creator (default: the user)")
    c.set_defaults(func=cmd_create)

    l = sub.add_parser("list", help="list a user's API keys (no secrets shown)")
    l.add_argument("username")
    l.set_defaults(func=cmd_list)

    r = sub.add_parser("revoke", help="revoke an API key by id")
    r.add_argument("key_id", type=int)
    r.add_argument("--by", help="actor recorded as revoker")
    r.set_defaults(func=cmd_revoke)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(_registry(), args)


if __name__ == "__main__":
    main()
