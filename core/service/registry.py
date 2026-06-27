"""Trusted-server registry - the management layer over the DAO.

Wraps :class:`core.db.trusted_dao.TrustedServerDAO` with the behaviour the
routes and the resolver need:

* ``add`` validates reachability + auth by probing the remote's
  ``/api/service/info`` before the entry is considered good (and records the
  reported version);
* ``build_client`` decrypts the stored key and constructs a
  :class:`core.service.rest_client.RestServiceClient` for the active request,
  stamping the ``on_behalf`` identity.

Keys are never returned in the clear by ``list``/``get`` (DAO ``to_dict`` omits
them); only ``build_client`` and ``probe`` touch the decrypted key, and only
server-side.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from core.db.trusted_dao import TrustedServerDAO
from core.service.rest_client import RestServiceClient

logger = logging.getLogger(__name__)


class TrustedServerRegistry:
    def __init__(self, dao: TrustedServerDAO = None, secret_env=None):
        self.dao = dao or TrustedServerDAO(secret_env=secret_env)
        # Phase 5: one pooled HTTP session per server_id for connection reuse
        # (keep-alive) across requests, instead of a fresh socket every call.
        self._sessions = {}

    # ---- reads (key never exposed) ------------------------------------
    def list(self):
        return self.dao.list()

    def get(self, server_id):
        return self.dao.get(server_id)

    def get_clear_key(self, server_id):
        """Decrypt and return the stored key. Server-side use only (proxy)."""
        return self.dao.get_clear_key(server_id)

    # ---- mutations -----------------------------------------------------
    def add(self, server_id, name, url, clear_api_key, *, role="admin",
            verify_tls=True, added_by=None, probe=True):
        """Register a trusted server. By default probes it first so a bad URL/
        key/version is caught at configure time, not on first use."""
        if probe:
            info = self._probe_raw(url, clear_api_key, verify_tls)  # raises
            rec = self.dao.add(server_id, name, url, clear_api_key, role=role,
                               verify_tls=verify_tls, added_by=added_by)
            self.dao.record_probe(server_id, True, info.version)
            rec["last_probe_ok"] = True
            rec["last_probe_version"] = info.version
            return rec
        return self.dao.add(server_id, name, url, clear_api_key, role=role,
                            verify_tls=verify_tls, added_by=added_by)

    def remove(self, server_id, removed_by=None):
        self.dao.remove(server_id, removed_by=removed_by)

    def rotate_key(self, server_id, clear_api_key, rotated_by=None):
        self.dao.rotate_key(server_id, clear_api_key, rotated_by=rotated_by)

    def test(self, server_id):
        """Re-probe a stored server; records and returns the outcome."""
        ts = self.dao.get(server_id)
        if ts is None:
            raise ValueError(f"Trusted server '{server_id}' not found")
        key = self.dao.get_clear_key(server_id)
        try:
            info = self._probe_raw(ts["url"], key, ts["verify_tls"])
        except Exception:  # noqa: BLE001 - record failure, then re-raise
            self.dao.record_probe(server_id, False, None)
            raise
        self.dao.record_probe(server_id, True, info.version)
        return info

    # ---- client construction ------------------------------------------
    def _session_for(self, server_id, verify_tls):
        """A pooled requests.Session per server (Phase 5 connection reuse)."""
        sess = self._sessions.get(server_id)
        if sess is None:
            import requests
            sess = requests.Session()
            sess.verify = verify_tls
            self._sessions[server_id] = sess
        return sess

    def build_client(self, server_id, on_behalf=None, http=None):
        """Construct a RestServiceClient for an active request, or None."""
        ts = self.dao.get(server_id)
        if ts is None:
            return None
        key = self.dao.get_clear_key(server_id)
        transport = http if http is not None else \
            self._session_for(server_id, ts["verify_tls"])
        return RestServiceClient(
            ts["url"], key, role=ts["role"], verify_tls=ts["verify_tls"],
            on_behalf=on_behalf, http=transport)

    def health(self, server_id, timeout=4.0):
        """Two-signal health for fleet tiles + the switcher. Never raises.

        Splits two distinct questions that the old info()-only probe conflated:
          * ``reachable`` - is the box up and serving? (cheap, unauthenticated
            GET /health/live; a transport failure => not reachable).
          * ``manageable`` - can *we* actually manage it? (our key authenticates
            and the management contract answers, via info()).
        A server can be reachable-but-not-manageable (bad/expired key, role, or
        version), which is useful to show distinctly from "down".
        """
        out = {"reachable": False, "manageable": False,
               "version": None, "error": None}
        client = self.build_client(server_id)
        if client is None:
            out["error"] = "not found"
            return out
        client.timeout = timeout
        try:
            client.live()                       # reachability (liveness)
            out["reachable"] = True
        except Exception as e:  # noqa: BLE001 - transport failure => down
            out["error"] = str(e)
            return out
        try:
            out["version"] = client.info().version   # manageability (auth)
            out["manageable"] = True
        except Exception as e:  # noqa: BLE001 - up, but we can't manage it
            out["error"] = f"reachable, not manageable: {e}"
        return out

    # ---- internals -----------------------------------------------------
    @staticmethod
    def _probe_raw(url, clear_api_key, verify_tls):
        """Build a throwaway client and fetch /api/service/info (raises on fail)."""
        probe_client = RestServiceClient(url, clear_api_key,
                                         verify_tls=verify_tls)
        return probe_client.info()
