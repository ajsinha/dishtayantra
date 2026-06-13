"""
core.ha - Configurable High Availability for DishtaYantra v2.0.0.

Primary/secondary automated failover selected via ``ha.provider``:
none | zookeeper | redis | s3 | socket.  See ha_manager for details.
"""
from core.ha.ha_manager import (
    HAProvider,
    HAConfigurationError,
    AlwaysPrimaryHAProvider,
    create_ha_provider,
    SUPPORTED_HA_PROVIDERS,
)

__all__ = [
    "HAProvider", "HAConfigurationError", "AlwaysPrimaryHAProvider",
    "create_ha_provider", "SUPPORTED_HA_PROVIDERS",
]
