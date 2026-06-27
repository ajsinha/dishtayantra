"""Service-plane abstraction (UI-plane / service-plane split).

This package introduces the ``ServiceClient`` seam that lets the UI act on either
the local in-process DAG server (``LocalServiceClient``) or, in a later phase, a
remote DishtaYantra instance (``RestServiceClient``) - without route code caring
which. See docs/design/service-plane-split.md and
docs/design/service-plane-roadmap.md.

Phase 0 (this release) ships the interface, the local flavour, and the resolver.
Phase 1 ships the JSON management contract that the remote flavour will target.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from core.service.client import (  # noqa: F401
    LocalServiceClient,
    ServiceClient,
    get_service_client,
)
from core.service.contract import (  # noqa: F401
    CAPABILITIES,
    SCHEMA_VERSION,
    SERVICE_OPERATIONS,
)
from core.service.types import OpResult, ServiceInfo  # noqa: F401
