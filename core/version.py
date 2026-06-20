"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.16.2 highlights (logout returns to the public landing page):
    - Logout now redirects to the public landing page (the root route '/', which renders
      landing.html for anonymous visitors) instead of the login form. The "Logged out
      successfully" flash carries over. One-line change in routes/auth_routes.py logout().
      235 passed (234 + 1 skipped without lmdb). Full history: docs/CHANGELOG.md.
"""

VERSION = "5.16.2"
BUILD_DATE = "2026-06-18"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
