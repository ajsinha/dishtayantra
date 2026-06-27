"""Service-plane contract - the single source of truth for what the management
API promises.

Both the JSON routes (:mod:`routes.service_routes`) and the contract drift guard
(:mod:`tests.test_service_contract`) read from here, and the future
``RestServiceClient`` will iterate :data:`SERVICE_OPERATIONS` as its operation
catalogue. Keeping the promised surface in one place is the same anti-drift
discipline applied to the Designer palette in v5.27.0: a contract promise that
isn't actually served must fail a test, not surface as a runtime 404.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

# Bumped when the management contract changes shape in a non-additive way.
SCHEMA_VERSION = "1"

# Capabilities advertised by GET /api/service/info. The UI (and a remote UI
# plane in a later phase) can use these to show/hide controls a peer is too old
# to support, instead of erroring on click. Grows additively across phases.
CAPABILITIES = (
    "service.info",      # GET /api/service/info
    "dag.inventory",     # list / details / status (read)
    "dag.lifecycle",     # start / stop / suspend / resume / reload
)

# The promised management operations: (method, path_template, required_role).
# required_role is advisory metadata for the contract/test; actual enforcement
# lives in the route handlers via AuthGuards. Path templates use FastAPI's
# ``{param}`` form exactly as registered, so the drift test can match them
# against the served OpenAPI document verbatim.
SERVICE_OPERATIONS = (
    ("GET",  "/api/service/info",                 "login"),
    ("GET",  "/api/service/dags",                 "login"),
    ("GET",  "/api/service/dag/{dag_id}",         "login"),
    ("GET",  "/api/service/status",               "login"),
    ("POST", "/api/service/dags/reload",          "admin"),
    ("POST", "/api/service/dag/{dag_id}/start",   "admin"),
    ("POST", "/api/service/dag/{dag_id}/stop",    "admin"),
    ("POST", "/api/service/dag/{dag_id}/suspend", "admin"),
    ("POST", "/api/service/dag/{dag_id}/resume",  "admin"),
)
