"""
LMDB Calculator Mixin & Helpers (v2.2 module split)
===================================================

LMDBCalculatorMixin (zero-copy I/O for calculators) and the module-level
convenience helpers, extracted verbatim from lmdb_transport.py to respect
the 500-line architecture limit. Re-exported from core.lmdb.lmdb_transport.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
from typing import Any, Optional

from core.lmdb.lmdb_types import DataFormat, LMDBConfig
# Safe partial-init import: lmdb_transport imports this module at its
# BOTTOM, after LMDBTransport is already defined.
from core.lmdb.lmdb_transport import LMDBTransport

logger = logging.getLogger(__name__)


class LMDBCalculatorMixin:
    """
    Mixin class for calculators that support LMDB transport.
    
    This mixin provides methods for calculators to:
    - Check if LMDB transport is enabled
    - Read input data from LMDB
    - Write output data to LMDB
    - Exchange large payloads without serialization overhead
    """
    
    def __init__(self):
        self._lmdb_transport: Optional[LMDBTransport] = None
        self._lmdb_enabled: bool = False
        self._lmdb_input_key: Optional[str] = None
        self._lmdb_output_key: Optional[str] = None
        self._lmdb_db_name: str = "default"
    
    def enable_lmdb(
        self,
        db_path: Optional[str] = None,
        input_key: Optional[str] = None,
        output_key: Optional[str] = None,
        db_name: str = "default"
    ) -> bool:
        """
        Enable LMDB transport for this calculator.
        
        Args:
            db_path: Path to LMDB database (uses config default if None)
            input_key: Key to read input data from
            output_key: Key to write output data to
            db_name: Named database to use
            
        Returns:
            True if LMDB was successfully enabled
        """
        try:
            self._lmdb_transport = LMDBTransport.get_instance(db_path)
            self._lmdb_enabled = True
            self._lmdb_input_key = input_key
            self._lmdb_output_key = output_key
            self._lmdb_db_name = db_name
            return True
        except Exception as e:
            logger.error(f"Failed to enable LMDB: {e}")
            return False
    
    def is_lmdb_enabled(self) -> bool:
        """Check if LMDB transport is enabled"""
        return self._lmdb_enabled and self._lmdb_transport is not None
    
    def lmdb_read_input(self, key: Optional[str] = None) -> Optional[Any]:
        """Read input data from LMDB"""
        if not self.is_lmdb_enabled():
            return None
        
        key = key or self._lmdb_input_key
        if key is None:
            return None
        
        return self._lmdb_transport.get(key, self._lmdb_db_name)
    
    def lmdb_write_output(
        self,
        data: Any,
        key: Optional[str] = None,
        format: Optional[DataFormat] = None
    ) -> Optional[str]:
        """Write output data to LMDB"""
        if not self.is_lmdb_enabled():
            return None
        
        key = key or self._lmdb_output_key
        if key is None:
            return None
        
        return self._lmdb_transport.put(
            key=key,
            data=data,
            format=format,
            db_name=self._lmdb_db_name
        )
    
    def lmdb_get_db_path(self) -> Optional[str]:
        """Get LMDB database path for native calculator"""
        if self.is_lmdb_enabled():
            return self._lmdb_transport.get_db_path()
        return None


# Convenience functions for direct access
_default_transport: Optional[LMDBTransport] = None


def get_transport(db_path: Optional[str] = None) -> LMDBTransport:
    """Get the default LMDB transport instance"""
    global _default_transport
    if _default_transport is None:
        _default_transport = LMDBTransport.get_instance(db_path)
    return _default_transport


def lmdb_put(key: str, data: Any, **kwargs) -> str:
    """Convenience function to store data"""
    return get_transport().put(key, data, **kwargs)


def lmdb_get(key: str, **kwargs) -> Optional[Any]:
    """Convenience function to retrieve data"""
    return get_transport().get(key, **kwargs)


def lmdb_delete(key: str, **kwargs) -> bool:
    """Convenience function to delete data"""
    return get_transport().delete(key, **kwargs)
