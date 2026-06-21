"""
Audit trail helper
==================

A thin, defensive front door to the audit log. Call :func:`audit` from anywhere
to record a security/admin-relevant action; it never raises into the caller, so
a logging failure can never break the operation being audited.

    from core.audit_log import audit, client_ip
    audit("apikey.create", actor=admin_user, target="alice/ci-runner",
          detail="prefix dyk_ab12", source_ip=client_ip(request))

Conventions for ``action`` (dotted, lowercase): ``auth.login``,
``auth.login_failed``, ``user.create``, ``user.update``, ``user.delete``,
``apikey.create``, ``apikey.revoke``.
"""

import logging

logger = logging.getLogger(__name__)


def audit(action, actor=None, target=None, detail=None, source_ip=None,
          success=True):
    """Record one audit event. Swallows and logs any error (never raises)."""
    try:
        from core.db.dao import AuditDAO
        AuditDAO().record(action=action, actor=actor, target=target,
                          detail=detail, source_ip=source_ip, success=success)
    except Exception:  # noqa: BLE001 - auditing must never break the action
        logger.exception("Failed to record audit event '%s' (actor=%s)",
                         action, actor)


def client_ip(request):
    """Best-effort client IP from a Starlette/FastAPI request, or None."""
    try:
        fwd = request.headers.get("x-forwarded-for")
        if fwd:
            return fwd.split(",")[0].strip()
        return request.client.host if request.client else None
    except Exception:  # noqa: BLE001
        return None
