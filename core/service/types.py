"""Result types crossing the ``ServiceClient`` seam.

These are deliberately plain and JSON-serialisable: the local flavour returns
them directly, and the remote flavour (later phase) will deserialise the JSON
form back into them, so route code is identical regardless of flavour.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from dataclasses import dataclass, field


@dataclass
class ServiceInfo:
    """Identity + capability advertisement for a service plane."""

    name: str
    version: str
    build_date: str
    schema_version: str
    capabilities: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "build_date": self.build_date,
            "schema_version": self.schema_version,
            "capabilities": list(self.capabilities),
        }


@dataclass
class OpResult:
    """Outcome of a mutation (start/stop/reload/...).

    ``ok`` drives control flow; ``message`` + ``level`` ('success' | 'info' |
    'warning' | 'error') drive UI presentation; ``data`` carries any structured
    payload (e.g. the resulting state).
    """

    ok: bool
    message: str = ""
    level: str = "success"
    data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "message": self.message,
            "level": self.level,
            "data": self.data,
        }

    @classmethod
    def success(cls, message="", **data) -> "OpResult":
        return cls(ok=True, message=message, level="success", data=dict(data))

    @classmethod
    def error(cls, message, **data) -> "OpResult":
        return cls(ok=False, message=message, level="error", data=dict(data))
