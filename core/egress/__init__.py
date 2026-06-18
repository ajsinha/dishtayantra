"""Async egress subsystem (roadmap A5): WAL-backed, decoupled publication.

Off by default and additive. See docs/design/A5-async-egress-subsystem.md and the
"Async Egress (WAL-Backed Publication) Guide" in Help.
"""

from core.egress.async_publisher import (AsyncPublisher, maybe_wrap_publisher,
                                         read_egress_config)
from core.egress.drainer import AsyncEgressManager, get_manager
from core.egress.wal import WalFull, create_wal

__all__ = [
    "AsyncPublisher", "maybe_wrap_publisher", "read_egress_config",
    "AsyncEgressManager", "get_manager", "create_wal", "WalFull",
]
