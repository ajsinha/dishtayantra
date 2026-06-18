"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.11.3 highlights (architecture page contrast sweep + JSON logging default):
    - Architecture help page: swept the remaining fixed-light surfaces that were
      low-contrast in dark/green/ubuntu - all bg-light boxes -> theme-aware
      bg-body-tertiary, the SinkNode trash icon and a bg-light badge recoloured, and the
      hardcoded light "Light Up/Light Down" gradients -> translucent success/danger
      tints. Also fixed the same light-gradient on dag/publish_message.html.
    - Enabled JSON logging by DEFAULT (logging.format=json in application.properties +
      application.yaml). One compact JSON object per line; switch back with
      logging.format=text. Formatter/fields unchanged. 234 passed.
      Full history: docs/CHANGELOG.md.
"""

VERSION = "5.11.3"
BUILD_DATE = "2026-06-17"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
