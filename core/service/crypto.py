"""At-rest encryption for trusted-server API keys.

Trusted-server credentials are stored symmetric-encrypted in the database
(decision A: encrypted-in-DB). Encryption uses Fernet (AES-128-CBC + HMAC) from
the ``cryptography`` package, with the key derived from a secret supplied via the
environment - never committed, never in config files, never sent to the browser.

Design rules (consistent with "no silent defaults / fail loudly"):

* ``cryptography`` is an **optional** dependency: the base install stays frugal
  and air-gapped-friendly. It is only needed if you actually register trusted
  servers. If it is missing when encryption is required, we raise a clear error.
* The secret comes from the env var named by ``service.secret_env`` (default
  ``DY_SECRET_KEY``). If trusted servers exist but no secret is configured, the
  registry fails loudly rather than storing or reading keys in the clear.
* The stored secret may be a 32-byte url-safe base64 Fernet key, or any
  passphrase (we derive a Fernet key from it via SHA-256). This keeps operator
  setup simple while remaining a real key.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import base64
import hashlib
import os

DEFAULT_SECRET_ENV = "DY_SECRET_KEY"


class SecretNotConfigured(RuntimeError):
    """Raised when encryption is required but no secret/key material exists."""


class CryptoUnavailable(RuntimeError):
    """Raised when the optional ``cryptography`` dependency is not installed."""


def _fernet(secret_env: str = DEFAULT_SECRET_ENV):
    """Build a Fernet from the configured secret. Raises loudly on misconfig."""
    try:
        from cryptography.fernet import Fernet
    except Exception as exc:  # noqa: BLE001
        raise CryptoUnavailable(
            "Trusted-server key encryption requires the 'cryptography' package "
            "(pip install cryptography --break-system-packages). Not installed: "
            f"{exc}")
    secret = os.environ.get(secret_env)
    if not secret:
        raise SecretNotConfigured(
            f"Trusted-server keys are encrypted at rest but no secret is set. "
            f"Set the {secret_env} environment variable (a passphrase or a "
            f"url-safe base64 32-byte Fernet key).")
    # Accept a ready-made Fernet key, else derive one deterministically from the
    # passphrase via SHA-256 -> 32 bytes -> url-safe base64.
    raw = secret.encode("utf-8")
    try:
        if len(raw) == 44 and base64.urlsafe_b64decode(raw):  # looks like a key
            return Fernet(raw)
    except Exception:  # noqa: BLE001 - not a ready key; derive below
        pass
    derived = base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
    return Fernet(derived)


def secret_available(secret_env: str = DEFAULT_SECRET_ENV) -> bool:
    """True if both the dependency and the secret are present (no raise)."""
    try:
        _fernet(secret_env)
        return True
    except (CryptoUnavailable, SecretNotConfigured):
        return False


def encrypt(plaintext: str, secret_env: str = DEFAULT_SECRET_ENV) -> str:
    """Encrypt a clear string to a storable token. Raises on misconfig."""
    return _fernet(secret_env).encrypt(plaintext.encode("utf-8")).decode("ascii")


def decrypt(token: str, secret_env: str = DEFAULT_SECRET_ENV) -> str:
    """Decrypt a stored token back to the clear string. Raises on misconfig."""
    return _fernet(secret_env).decrypt(token.encode("ascii")).decode("utf-8")


def mask(clear_key: str) -> str:
    """Render a key for display: prefix + last 4, middle redacted."""
    if not clear_key:
        return ""
    if len(clear_key) <= 10:
        return clear_key[:2] + "***"
    return f"{clear_key[:8]}...{clear_key[-4:]}"
