"""Typed errors raised by remote service clients.

A ``RestServiceClient`` maps transport/HTTP failures onto these so route code
can render meaningful messages instead of opaque tracebacks. Mutations that fail
business-logic validation on the remote (e.g. unknown DAG) raise ``ValueError``
- mirroring ``LocalServiceClient`` - so handlers behave identically regardless
of flavour; these typed errors are reserved for the *transport* concerns a local
client simply cannot have.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""


class ServiceError(Exception):
    """Base class for remote service-plane failures."""


class ServiceUnavailable(ServiceError):
    """The remote plane could not be reached (connection refused/DNS/network)."""


class ServiceTimeout(ServiceError):
    """The remote plane did not respond within the timeout."""


class ServiceAuthError(ServiceError):
    """The remote plane rejected our credentials (401/403)."""


class ServiceProtocolError(ServiceError):
    """The remote plane responded, but not in the way the contract expects."""
