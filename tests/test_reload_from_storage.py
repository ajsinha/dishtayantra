"""
Tests for DAGComputeServer.reload_from_storage (v2.2).

Exercises the runtime reconciliation logic directly against the
DAGLoaderMixin, using a lightweight stub host object that carries only the
attributes the method touches. This isolates the reconciliation behaviour
from HA startup, background threads, and the global storage singleton.
"""
import json
import threading

import pytest

from core.dag.dag_server_loader import DAGLoaderMixin
from core.storage.storage_factory import create_storage_provider


class _Props:
    """Minimal props object for the filesystem storage provider."""
    def __init__(self, mapping):
        self._m = mapping

    def get(self, key, default=None):
        return self._m.get(key, default)

    def resolve_string_json_content(self, text):
        # No placeholders in these tests; just parse the JSON.
        return json.loads(text)


class _StubServer(DAGLoaderMixin):
    """Carries only what reload_from_storage / _load_dags need."""
    def __init__(self, storage, prefix, props):
        self._storage = storage
        self.dag_config_prefix = prefix
        self._props = props
        self.dags = {}
        self._lock = threading.RLock()
        self._worker_pool = None
        self.is_primary = True
        self.deleted = []

    def _check_primary(self):
        if not self.is_primary:
            raise RuntimeError("not primary")

    def delete(self, dag_name):
        # Mimic the real delete(): stop (n/a here) + remove from registry.
        with self._lock:
            self.deleted.append(dag_name)
            del self.dags[dag_name]


def _dag_json(name):
    return json.dumps({
        "name": name,
        "subscribers": [], "publishers": [], "calculators": [],
        "transformers": [], "nodes": [], "edges": [],
    })


@pytest.fixture
def server(tmp_path):
    prefix = "dags"
    props = _Props({
        "storage.provider": "filesystem",
        "storage.filesystem.root": str(tmp_path),
    })
    storage = create_storage_provider(props)
    storage.ensure_prefix(prefix)
    return _StubServer(storage, prefix, props)


def _write(server, filename, name):
    server._storage.write_text(f"{server.dag_config_prefix}/{filename}",
                               _dag_json(name))


def test_reload_adds_new_dag(server):
    _write(server, "alpha.json", "alpha")
    result = server.reload_from_storage()
    assert "alpha" in result["added"]
    assert "alpha" in server.dags
    assert result["removed"] == []
    assert result["errors"] == []


def test_reload_is_idempotent(server):
    _write(server, "alpha.json", "alpha")
    server.reload_from_storage()
    # Second reload with no filesystem changes -> nothing happens.
    result = server.reload_from_storage()
    assert result["added"] == []
    assert result["removed"] == []
    assert "alpha" in server.dags


def test_reload_removes_deleted_file(server):
    _write(server, "alpha.json", "alpha")
    server.reload_from_storage()
    # Remove the backing object, then reload.
    server._storage.delete(f"{server.dag_config_prefix}/alpha.json")
    result = server.reload_from_storage(remove_missing=True)
    assert "alpha" in result["removed"]
    assert "alpha" not in server.dags
    assert "alpha" in server.deleted


def test_reload_remove_missing_false_keeps_dag(server):
    _write(server, "alpha.json", "alpha")
    server.reload_from_storage()
    server._storage.delete(f"{server.dag_config_prefix}/alpha.json")
    result = server.reload_from_storage(remove_missing=False)
    assert result["removed"] == []
    assert "alpha" in server.dags


def test_reload_leaves_in_memory_dags_untouched(server):
    # Simulate a designer/clone DAG with no config_filename.
    from core.dag.compute_graph import ComputeGraph
    mem_dag = ComputeGraph(json.loads(_dag_json("in_memory_clone")),
                           lazy_init=True)
    server.dags["in_memory_clone"] = mem_dag

    _write(server, "alpha.json", "alpha")
    result = server.reload_from_storage(remove_missing=True)

    assert "alpha" in server.dags                 # file-backed added
    assert "in_memory_clone" in server.dags        # in-memory preserved
    assert "in_memory_clone" not in result["removed"]
    assert "in_memory_clone" in result["skipped_in_memory"]


def test_reload_rejected_on_secondary(server):
    server.is_primary = False
    with pytest.raises(RuntimeError):
        server.reload_from_storage()


def test_reload_ignores_subprefix_samples(server):
    # A sample under a sub-prefix must NOT be auto-loaded.
    server._storage.ensure_prefix(f"{server.dag_config_prefix}/examples")
    server._storage.write_text(
        f"{server.dag_config_prefix}/examples/sample.json",
        _dag_json("sample"))
    _write(server, "alpha.json", "alpha")
    result = server.reload_from_storage()
    assert "alpha" in result["added"]
    assert "sample" not in result["added"]
    assert "sample" not in server.dags


def test_get_dag_source_config_returns_raw_file(server):
    """get_dag_source_config returns the verbatim stored file, not a
    derived/enriched view."""
    raw_text = _dag_json("srctest")
    server._storage.write_text(
        f"{server.dag_config_prefix}/srctest.json", raw_text)
    server.reload_from_storage()
    out_text, out_cfg = server.get_dag_source_config("srctest")
    assert out_text == raw_text                 # exact bytes
    assert out_cfg["name"] == "srctest"
    # The internal bookkeeping key must not leak into the returned config.
    assert "config_filename" not in out_cfg


def test_get_dag_source_config_in_memory_dag(server):
    """For an in-memory DAG (no backing file) raw text is None but the
    config dict is still returned, minus config_filename."""
    from core.dag.compute_graph import ComputeGraph
    server.dags["mem_only"] = ComputeGraph(
        json.loads(_dag_json("mem_only")), lazy_init=True)
    out_text, out_cfg = server.get_dag_source_config("mem_only")
    assert out_text is None
    assert out_cfg["name"] == "mem_only"
    assert "config_filename" not in out_cfg
