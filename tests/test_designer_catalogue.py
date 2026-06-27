"""Guards for the Designer component catalogue and the API-driven palette.

Background: the Designer palette HTML and the route's component list used to be
two parallel hardcoded structures. They drifted - seven backend-supported
components (PublisherSinkNode, four calculators, two transformers) had silently
fallen out of the palette and were undraggable. v5.27.0 made the palette render
dynamically from ``/api/dag-designer/components``, whose catalogue lives in
``core.dag.designer_catalogue`` as a single source of truth.

These tests lock that in:
  * the catalogue module is well-formed and complete;
  * the live API returns exactly that catalogue;
  * the template no longer hardcodes palette items (so it cannot drift again);
  * the presentation map carries no stale entries for nonexistent components.
"""
import os
import re

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from core.dag.designer_catalogue import (  # noqa: E402
    PALETTE_SECTIONS,
    build_component_catalogue,
    palette_component_types,
)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DESIGNER_HTML = os.path.join(_REPO_ROOT, "web", "templates", "dag", "designer.html")

# Components that had drifted out of the old hardcoded palette; their presence
# in the catalogue (and therefore the rendered palette) is the regression we fix.
_PREVIOUSLY_DRIFTED = {
    "PublisherSinkNode",
    "NullCalculator",
    "RandomCalculator",
    "AttributeFilterAwayCalculator",
    "AttributeNameChangeCalculator",
    "NullDataTransformer",
    "AttributeFilterAwayDataTransformer",
}


@pytest.fixture()
def admin_client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    c = TestClient(webapp.app, follow_redirects=True)
    r = c.post("/login", data={"username": "admin", "password": "admin123"})
    assert r.status_code == 200
    return c


# ----------------------------------------------------------------- catalogue
def test_catalogue_has_expected_sections_and_counts():
    cat = build_component_catalogue()
    for key, _category in PALETTE_SECTIONS:
        assert key in cat, f"missing catalogue section {key}"
        assert isinstance(cat[key], list) and cat[key], f"{key} empty"
    # URI-prefix reference lists are present but not palette sections.
    assert "subscriber_sources" in cat
    assert "publisher_destinations" in cat


def test_catalogue_includes_previously_drifted_components():
    types = palette_component_types()
    missing = _PREVIOUSLY_DRIFTED - types
    assert not missing, f"catalogue regressed - missing again: {sorted(missing)}"


def test_build_returns_independent_copies():
    a = build_component_catalogue()
    b = build_component_catalogue()
    assert a == b
    a["node_types"].append({"type": "Bogus"})
    assert "Bogus" not in [e["type"] for e in b["node_types"]]


# ----------------------------------------------------------------------- API
def test_api_returns_the_catalogue(admin_client):
    r = admin_client.get("/api/dag-designer/components")
    assert r.status_code == 200
    assert r.json() == build_component_catalogue()


# ------------------------------------------------------------------ template
def _designer_html():
    with open(_DESIGNER_HTML, encoding="utf-8") as fh:
        return fh.read()


def test_template_palette_is_api_driven():
    """The palette must not hardcode any draggable item, or it can drift."""
    html = _designer_html()
    hardcoded = re.findall(r'data-type="\w+"\s+data-category="\w+"', html)
    assert not hardcoded, (
        "designer.html hardcodes palette items again: %s" % hardcoded[:5])
    # The five dynamic containers must exist and the loader must be wired.
    for _key, category in PALETTE_SECTIONS:
        assert f'id="palette-{category}"' in html, f"missing palette-{category}"
    assert "loadPalette()" in html
    assert "/api/dag-designer/components" in html


def test_presentation_map_has_no_stale_entries():
    """Every styled type must still exist in the catalogue (no stale keys)."""
    html = _designer_html()
    m = re.search(r"const PALETTE_PRESENTATION = \{(.*?)\n\};", html, re.S)
    assert m, "PALETTE_PRESENTATION map not found"
    keys = set(re.findall(r"^\s{4}(\w+):\s*\{", m.group(1), re.M))
    assert keys, "no presentation entries parsed"
    stale = keys - palette_component_types()
    assert not stale, f"presentation entries for nonexistent types: {sorted(stale)}"
