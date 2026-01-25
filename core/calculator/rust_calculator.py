"""
Rust PyO3 Calculator Integration for DishtaYantra
=================================================

This module provides integration with Rust calculators compiled using PyO3.

v1.7.0: Added Rust Manager support for centralized calculator management.

Rust calculators offer:
- C/C++ level performance
- Memory safety guaranteed at compile time
- Thread safety enforced by the type system
- Fearless concurrency with rayon
- Easy deployment with maturin
- LMDB memory-mapped zero-copy exchange for large payloads

Usage:
    # Via Rust Manager (RECOMMENDED - v1.7.0)
    from core.rust import create_rust_calculator
    calc = create_rust_calculator("RustMathCalculator", "my_calc", {"operation": "sum"})
    
    # Direct usage (legacy)
    config = {'rust_module': 'dishtayantra_rust', 'rust_class': 'MathCalculator'}
    calc = RustCalculator('my_calc', config)
"""

import importlib
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# LMDB support
try:
    from core.lmdb import LMDBCalculatorConfig, LMDBDataExchange, DataFormat
    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False
    logger.debug("LMDB module not available for Rust calculator")


class RustCalculator:
    """
    Wrapper for Rust calculators compiled with PyO3.
    
    v1.7.0: Now supports two initialization modes:
    
    1. Rust Manager mode (recommended):
       Uses calculator_name from rust_config.json
       ```python
       calc = RustCalculator('my_calc', {'calculator_name': 'RustMathCalculator'})
       ```
    
    2. Direct mode (legacy):
       Specifies rust_module and rust_class directly
       ```python
       calc = RustCalculator('my_calc', {
           'rust_module': 'dishtayantra_rust',
           'rust_class': 'MathCalculator'
       })
       ```
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize Rust calculator wrapper.
        
        Args:
            name: Calculator name
            config: Configuration dictionary with either:
                    - calculator_name: Reference to calculator in rust_config.json (v1.7.0)
                    OR
                    - rust_module: Name of the compiled module
                    - rust_class: Name of the Rust struct
        """
        self.name = name
        self.config = config
        self._rust_instance = None
        
        # v1.7.0: Try Rust Manager first
        calculator_name = config.get('calculator_name') or config.get('name')
        
        if calculator_name:
            if self._init_via_manager(name, calculator_name, config):
                return
        
        # Fallback to direct initialization
        self._init_direct(name, config)
    
    def _init_via_manager(self, name: str, calculator_name: str, config: Dict[str, Any]) -> bool:
        """
        Initialize calculator via Rust Manager.
        
        Returns:
            True if initialization succeeded via manager
        """
        try:
            from core.rust import get_rust_manager
            
            rust_manager = get_rust_manager()
            
            if not rust_manager._initialized:
                return False
            
            if calculator_name not in rust_manager.calculator_definitions:
                return False
            
            # Get calculator class from manager
            definition = rust_manager.calculator_definitions[calculator_name]
            module_name = definition.module
            
            # Ensure module is loaded
            if module_name not in rust_manager.loaded_modules:
                if not rust_manager.load_module(module_name):
                    return False
            
            module = rust_manager.loaded_modules[module_name]
            
            if not hasattr(module, definition.rust_class):
                return False
            
            calculator_class = getattr(module, definition.rust_class)
            
            # Merge default config with provided config
            final_config = {**definition.default_config}
            for k, v in config.items():
                if k not in ('calculator_name', 'name'):
                    final_config[k] = v
            
            # Create instance
            self._rust_instance = calculator_class(name, final_config)
            self.config = final_config
            
            # Setup LMDB if enabled in module config
            self._setup_lmdb(rust_manager.module_configs[module_name].lmdb_config)
            
            logger.info(f"RustCalculator '{name}' created via Rust Manager: {calculator_name}")
            return True
            
        except ImportError:
            return False
        except Exception as e:
            logger.debug(f"Rust Manager initialization failed, falling back to direct: {e}")
            return False
    
    def _init_direct(self, name: str, config: Dict[str, Any]):
        """Initialize calculator directly with rust_module and rust_class"""
        rust_module = config.get('rust_module', 'dishtayantra_rust')
        rust_class = config.get('rust_class')
        
        if not rust_class:
            raise ValueError("rust_class must be specified for Rust calculators")
        
        try:
            # Import the compiled module
            module = importlib.import_module(rust_module)
            calculator_class = getattr(module, rust_class)
            
            # Create Rust calculator instance
            self._rust_instance = calculator_class(name, config)
            
            logger.info(f"RustCalculator '{name}' initialized (direct): {rust_module}.{rust_class}")
            
        except ImportError as e:
            raise RuntimeError(
                f"Failed to import Rust module '{rust_module}': {e}\n"
                f"Ensure the module is compiled with maturin:\n"
                f"  cd rust && maturin develop --release"
            )
        except AttributeError as e:
            raise RuntimeError(f"Struct '{rust_class}' not found in module '{rust_module}': {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Rust calculator: {e}")
        
        # Setup LMDB from config
        self._setup_lmdb(config)
    
    def _setup_lmdb(self, lmdb_config: Dict[str, Any]):
        """Setup LMDB transport if enabled"""
        self._lmdb_exchange: Optional[LMDBDataExchange] = None
        self._lmdb_enabled = False
        
        if LMDB_AVAILABLE and lmdb_config.get('enabled', False):
            try:
                cfg = LMDBCalculatorConfig.from_dict(lmdb_config)
                self._lmdb_exchange = LMDBDataExchange(cfg, self.name)
                self._lmdb_enabled = self._lmdb_exchange.initialize()
                if self._lmdb_enabled:
                    logger.info(f"RustCalculator '{self.name}' LMDB transport enabled at {cfg.db_path}")
            except Exception as e:
                logger.warning(f"Failed to setup LMDB for RustCalculator '{self.name}': {e}")
        
        # Statistics
        self._stats = {
            'calculations': 0,
            'total_time_ns': 0,
            'lmdb_exchanges': 0
        }
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation on the Rust side.
        
        For large payloads (configurable), uses LMDB memory-mapped
        files for zero-copy data exchange.
        """
        start_time = time.time_ns()
        txn_id = None
        use_lmdb = False
        
        try:
            # Check if LMDB should be used for this payload
            if self._lmdb_enabled and self._lmdb_exchange.should_use_lmdb(data):
                use_lmdb = True
                txn_id = f"{start_time}"
                
                # Store input in LMDB
                input_key = self._lmdb_exchange.put_input(data, txn_id)
                
                if input_key:
                    # Create LMDB reference for Rust
                    lmdb_ref = {
                        '_lmdb_ref': True,
                        '_lmdb_input_key': input_key,
                        '_lmdb_output_key': self._lmdb_exchange.generate_key(
                            self._lmdb_exchange.config.output_key_prefix, txn_id
                        ),
                        '_lmdb_db_path': self._lmdb_exchange.config.db_path,
                        '_lmdb_db_name': self._lmdb_exchange.config.db_name,
                        '_lmdb_format': self._lmdb_exchange.config.data_format.value,
                        '_txn_id': txn_id
                    }
                    
                    result = dict(self._rust_instance.calculate(lmdb_ref))
                    self._stats['lmdb_exchanges'] += 1
                else:
                    use_lmdb = False
                    result = dict(self._rust_instance.calculate(data))
            else:
                result = dict(self._rust_instance.calculate(data))
            
            # If LMDB was used, check for output in LMDB
            if use_lmdb and txn_id:
                lmdb_result = self._lmdb_exchange.get_output(txn_id, wait=True)
                if lmdb_result is not None:
                    result = lmdb_result
                self._lmdb_exchange.cleanup(txn_id)
            
            # Update stats
            elapsed_ns = time.time_ns() - start_time
            self._stats['calculations'] += 1
            self._stats['total_time_ns'] += elapsed_ns
            
            return result
            
        except Exception as e:
            if use_lmdb and txn_id and self._lmdb_exchange:
                try:
                    self._lmdb_exchange.cleanup(txn_id)
                except:
                    pass
            logger.error(f"RustCalculator '{self.name}' calculate error: {e}")
            raise
    
    def details(self) -> Dict[str, Any]:
        """Get calculator details and statistics"""
        try:
            details = dict(self._rust_instance.details())
        except Exception:
            details = {}
        
        avg_time_ns = (self._stats['total_time_ns'] / self._stats['calculations']
                       if self._stats['calculations'] > 0 else 0)
        
        details.update({
            'wrapper': 'RustCalculator',
            'name': self.name,
            'language': 'Rust',
            'binding': 'PyO3',
            'rust_module': self.config.get('rust_module', 'dishtayantra_rust'),
            'rust_class': self.config.get('rust_class'),
            'lmdb_enabled': self._lmdb_enabled,
            'stats': {
                'calculations': self._stats['calculations'],
                'avg_time_ns': round(avg_time_ns, 2),
                'lmdb_exchanges': self._stats['lmdb_exchanges']
            }
        })
        
        return details
    
    def is_lmdb_enabled(self) -> bool:
        """Check if LMDB transport is enabled"""
        return self._lmdb_enabled


def is_rust_module_available(module_name: str = 'dishtayantra_rust') -> bool:
    """Check if a Rust module is available for import."""
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


def get_rust_module_info(module_name: str = 'dishtayantra_rust') -> Optional[Dict[str, Any]]:
    """Get information about a Rust module."""
    try:
        module = importlib.import_module(module_name)
        return {
            'name': module_name,
            'file': getattr(module, '__file__', 'unknown'),
            'doc': getattr(module, '__doc__', ''),
            'available_classes': [
                name for name in dir(module)
                if not name.startswith('_') and isinstance(getattr(module, name), type)
            ]
        }
    except ImportError:
        return None


def list_available_rust_calculators() -> list:
    """
    List available Rust calculators from the Rust Manager.
    
    v1.7.0: Returns calculator definitions from rust_config.json
    """
    try:
        from core.rust import get_rust_manager
        manager = get_rust_manager()
        if manager._initialized:
            return manager.list_calculators()
    except ImportError:
        pass
    return []
