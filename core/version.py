"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.16.1 highlights (contrast fix: dark comparison table under light-family themes):
    - Fixed poor text contrast on the /comparison Head-to-Head table under light-family themes
      (Light, Blue). The table is an intentionally dark card, but Bootstrap 5.3 paints each
      cell with --bs-table-bg (light when data-bs-theme="light"), washing out the light cell
      text. The fix pins the table's Bootstrap tokens (--bs-table-bg: transparent,
      --bs-table-color light, striped/hover variants) on .cmp-table so the dark card shows
      through and text stays readable under EVERY theme - not just Blue.
    - Reviewed the other surfaces for the same anti-pattern: the About "Competitive Advantage"
      dark card uses explicit #ffffff text (fine); standard tables use theme-aware dark-on-light
      colors (fine in Blue); code blocks use fixed light text on a dark background (fine). No
      other dark-table-with-theme-text cases were found. 235 passed (234 + 1 skipped w/o lmdb).
      Full history: docs/CHANGELOG.md.
"""

VERSION = "5.16.1"
BUILD_DATE = "2026-06-18"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
