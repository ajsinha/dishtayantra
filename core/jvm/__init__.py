"""
DishtaYantra JVM Management Module
Version: 1.6.0

Provides centralized management of JVM instances and Py4J gateways
for Java calculator integration.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

from .jvm_manager import (
    JVMManager,
    GatewayConfig,
    CalculatorDefinition,
    GatewayStatus,
    get_jvm_manager,
    initialize_jvm_manager,
    get_gateway,
    create_calculator,
    is_jvm_available
)

__all__ = [
    'JVMManager',
    'GatewayConfig',
    'CalculatorDefinition',
    'GatewayStatus',
    'get_jvm_manager',
    'initialize_jvm_manager',
    'get_gateway',
    'create_calculator',
    'is_jvm_available'
]
