"""Persistence for trusted remote servers (UI-plane / service-plane split).

Kept separate from :mod:`core.db.dao` so that module stays under the project's
500-line code-file limit. Follows the same patterns: a thin DAO over
``DatabaseManager.session_scope``; ``to_dict`` results never expose the key.

The clear API key is symmetric-encrypted on write (:mod:`core.service.crypto`)
and only decrypted on demand by :meth:`get_clear_key`, which the registry calls
when constructing a ``RestServiceClient``. If encryption is misconfigured (no
secret, or the ``cryptography`` package missing) writes/reads of the key fail
loudly rather than storing or returning anything in the clear.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import select

from core.db.database_manager import DatabaseManager
from core.db.models import TrustedServer
from core.service import crypto

logger = logging.getLogger(__name__)


class TrustedServerDAO:
    """All trusted-server persistence operations."""

    def __init__(self, db: DatabaseManager = None, secret_env=None):
        self.db = db or DatabaseManager.get_instance()
        self.secret_env = secret_env or crypto.DEFAULT_SECRET_ENV

    # ------------------------------------------------------------- mutations
    def add(self, server_id, name, url, clear_api_key, *, role="admin",
            verify_tls=True, added_by=None) -> dict:
        """Register a trusted server. Encrypts the key before storing.

        Raises ValueError on duplicate server_id; crypto errors propagate
        loudly (SecretNotConfigured / CryptoUnavailable).
        """
        enc = crypto.encrypt(clear_api_key, self.secret_env)
        with self.db.session_scope() as session:
            existing = session.execute(
                select(TrustedServer).where(
                    TrustedServer.server_id == server_id)
            ).scalar_one_or_none()
            if existing is not None:
                raise ValueError(f"Trusted server '{server_id}' already exists")
            rec = TrustedServer(
                server_id=server_id, name=name, url=url, api_key_enc=enc,
                role=role, verify_tls=verify_tls, added_by=added_by)
            session.add(rec)
            session.flush()
            logger.info("Trusted server '%s' (%s) added by '%s'",
                        server_id, url, added_by)
            return rec.to_dict()

    def remove(self, server_id, removed_by=None) -> None:
        with self.db.session_scope() as session:
            rec = session.execute(
                select(TrustedServer).where(
                    TrustedServer.server_id == server_id)
            ).scalar_one_or_none()
            if rec is None:
                raise ValueError(f"Trusted server '{server_id}' not found")
            session.delete(rec)
            logger.info("Trusted server '%s' removed by '%s'",
                        server_id, removed_by)

    def rotate_key(self, server_id, clear_api_key, rotated_by=None) -> None:
        enc = crypto.encrypt(clear_api_key, self.secret_env)
        with self.db.session_scope() as session:
            rec = self._get(session, server_id)
            rec.api_key_enc = enc
            logger.info("Trusted server '%s' key rotated by '%s'",
                        server_id, rotated_by)

    def record_probe(self, server_id, ok, version=None) -> None:
        with self.db.session_scope() as session:
            rec = self._get(session, server_id)
            rec.last_probe_at = datetime.utcnow()
            rec.last_probe_ok = bool(ok)
            rec.last_probe_version = version

    # ------------------------------------------------------------- reads
    def list(self) -> List[dict]:
        with self.db.session_scope() as session:
            rows = session.execute(
                select(TrustedServer).order_by(TrustedServer.name)).scalars()
            return [r.to_dict() for r in rows]

    def get(self, server_id) -> Optional[dict]:
        with self.db.session_scope() as session:
            rec = session.execute(
                select(TrustedServer).where(
                    TrustedServer.server_id == server_id)
            ).scalar_one_or_none()
            return rec.to_dict() if rec else None

    def get_clear_key(self, server_id) -> Optional[str]:
        """Decrypt and return the stored key. Crypto errors propagate loudly."""
        with self.db.session_scope() as session:
            rec = session.execute(
                select(TrustedServer).where(
                    TrustedServer.server_id == server_id)
            ).scalar_one_or_none()
            if rec is None:
                return None
            return crypto.decrypt(rec.api_key_enc, self.secret_env)

    @staticmethod
    def _get(session, server_id) -> TrustedServer:
        rec = session.execute(
            select(TrustedServer).where(
                TrustedServer.server_id == server_id)
        ).scalar_one_or_none()
        if rec is None:
            raise ValueError(f"Trusted server '{server_id}' not found")
        return rec
