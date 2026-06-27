#!/usr/bin/env python3
"""Turnkey wiring for the two-node dev setup.

With Node A (UI plane, :18080) and Node B (service plane, :18090) both running,
this:
  1. logs into Node B and mints an admin API key,
  2. logs into Node A and registers Node B as a trusted server using that key.

Afterwards, open Node A at http://127.0.0.1:18080, use the navbar server
switcher to select "Service-Plane-B", or open /fleet to see both planes.

Usage:  python3 twonode/bootstrap.py
Env overrides: A_URL, B_URL, ADMIN_USER, ADMIN_PASS.
"""
import os
import re
import sys

import requests

A_URL = os.environ.get("A_URL", "http://127.0.0.1:18080").rstrip("/")
B_URL = os.environ.get("B_URL", "http://127.0.0.1:18090").rstrip("/")
USER = os.environ.get("ADMIN_USER", "admin")
PASS = os.environ.get("ADMIN_PASS", "admin123")

KEY_RE = re.compile(r"dyk_[A-Za-z0-9_\-]{8,}")


def login(base):
    s = requests.Session()
    r = s.post(base + "/login", data={"username": USER, "password": PASS},
               allow_redirects=True, timeout=10)
    if r.status_code != 200 or "/login" in r.url:
        sys.exit(f"login failed at {base} (status {r.status_code})")
    return s


def mint_key_on_b():
    s = login(B_URL)
    r = s.post(B_URL + "/admin/api-keys/create",
               data={"username": USER, "key_name": "uiplane-access"},
               allow_redirects=True, timeout=10)
    keys = KEY_RE.findall(r.text)
    if not keys:
        sys.exit("could not find the new dyk_ key in Node B's response "
                 "(is the user an admin? did creation succeed?)")
    # the clear key is the longest match (prefixes are short, e.g. 'dyk_ab12')
    return max(keys, key=len)


def register_b_on_a(clear_key):
    s = login(A_URL)
    r = s.post(A_URL + "/admin/trusted-servers/add",
               data={"server_id": "node-b", "name": "Service-Plane-B",
                     "url": B_URL, "api_key": clear_key, "role": "admin",
                     "verify_tls": "off"},
               allow_redirects=True, timeout=15)
    if r.status_code not in (200, 303):
        sys.exit(f"register failed on Node A (status {r.status_code})")
    # The add path probes B's /api/service/info; a failure is flashed, so check.
    if "added and verified" not in r.text and "Service-Plane-B" not in r.text:
        print("WARNING: registration POST returned but success was not "
              "confirmed in the page; check Node A's /admin/trusted-servers.")


def main():
    print(f"Node A (UI plane):      {A_URL}")
    print(f"Node B (service plane): {B_URL}")
    print("1/2  minting an admin API key on Node B ...")
    key = mint_key_on_b()
    print(f"     got key {key[:8]}... (kept server-side only)")
    print("2/2  registering Node B as a trusted server on Node A ...")
    register_b_on_a(key)
    print("\nDone. Open Node A at " + A_URL + " and:")
    print("  - use the navbar server switcher to select 'Service-Plane-B', or")
    print("  - open " + A_URL + "/fleet for the multi-plane overview.")


if __name__ == "__main__":
    main()
