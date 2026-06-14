"""
DAG Server Loader Mixin (v2.2)
==============================

Storage-backed loading and runtime reconciliation of DAG configurations for
:class:`core.dag.dag_server.DAGComputeServer`. Split out of
``dag_server_support.py`` to keep each module within the 500-line
architecture limit. Re-exported from ``dag_server_support`` so existing
imports keep working unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback

from core.dag.compute_graph import ComputeGraph

logger = logging.getLogger(__name__)


class DAGNameCollisionError(Exception):
    """Raised at startup when two DAG files declare the same DAG name.

    DAG names must be globally unique across every configured folder. A
    collision means the configuration is ambiguous, so the server refuses to
    boot rather than start in an undefined state.
    """


class DAGLoaderMixin:
    """Storage-backed loading of DAG configurations."""

    def _record_name_collisions(self, collisions):
        """Stash detected name collisions so the dashboard can surface them.

        ``collisions`` is a list of ``(name, first_path, dup_path)`` tuples.
        Stored on the server so the UI can render a persistent red banner and
        offer to delete the offending file. Safe to call before the lock-held
        registry exists.
        """
        existing = getattr(self, 'name_collisions', None)
        if existing is None:
            existing = []
        # De-dupe by (name, dup_path) so repeated reloads don't pile up.
        seen = {(c['name'], c['offending_file']) for c in existing}
        for name, first_path, dup_path in collisions:
            key = (name, dup_path)
            if key in seen:
                continue
            existing.append({
                'name': name,
                'existing_file': first_path,
                'offending_file': dup_path,
            })
            seen.add(key)
        self.name_collisions = existing

    def clear_name_collision(self, offending_file):
        """Remove a recorded collision (after the offending file is deleted)."""
        existing = getattr(self, 'name_collisions', None) or []
        self.name_collisions = [c for c in existing
                                if c['offending_file'] != offending_file]

    def _load_dags(self):
        """Load all DAG configurations through the storage abstraction.

        v2.0.0: DAG JSON files are read from the configured StorageProvider
        under the 'storage.dags.prefix' logical prefix. Property placeholders
        (${VAR:default}) inside the JSON are resolved before the graph is
        built. Works identically for filesystem, S3, Azure Blob and GCS.

        v1.5.2: When worker pool is enabled, DAGs are loaded with
        lazy_init=True to prevent creating duplicate subscribers in the main
        process. Components are only created when the DAG is dispatched to a
        worker.
        """
        # v3.1.0: scan ALL configured folders (config/dags + extras).
        dag_objects = self._list_storage_dag_objects()  # list of (path, base)

        if not dag_objects:
            logger.warning(
                "No JSON configuration files found under DAG folder(s): %s "
                "(provider: %s)",
                ", ".join(getattr(self, 'dag_config_prefixes',
                                  [self.dag_config_prefix])),
                self._storage.name)
            logger.info("You can add DAG configurations via the web UI or "
                        "place JSON files under a configured folder")
            return

        logger.info("Found %d DAG configuration file(s) across folder(s): %s "
                    "(provider: %s)", len(dag_objects),
                    ", ".join(getattr(self, 'dag_config_prefixes',
                                      [self.dag_config_prefix])),
                    self._storage.name)

        # v1.5.2: Use lazy initialization when worker pool is enabled
        # This prevents creating duplicate Kafka consumers in main process
        use_lazy_init = self._worker_pool is not None
        if use_lazy_init:
            logger.info("Worker pool enabled - DAGs will be loaded with lazy initialization")

        # v3.1.0: enforce global DAG-name uniqueness across ALL folders. We
        # build everything first, COLLECTING every name collision, and only
        # then decide - so the operator sees ALL problems in one startup, not
        # one-at-a-time. Any collision is FATAL: the server must not boot into
        # an ambiguous state.
        name_to_source = {}        # dag.name -> object_path (first seen)
        collisions = []            # (name, first_path, dup_path)
        staged = []                # (dag, object_path) to commit if clean

        for object_path, base in dag_objects:
            try:
                logger.info(f"Loading DAG from storage object: {object_path}")
                config = self._read_dag_config(object_path)
                # v3.1.0: store the FULL object path so DAGs in different
                # folders are distinguishable (folder-as-source-key) and
                # removal reconciliation works across folders.
                config['config_filename'] = object_path
                dag = ComputeGraph(config, lazy_init=use_lazy_init)
            except Exception as e:
                logger.error(f"Error loading DAG from {object_path}: {str(e)}")
                logger.error(traceback.format_exc())
                raise

            if dag.name in name_to_source:
                collisions.append((dag.name, name_to_source[dag.name],
                                   object_path))
            else:
                name_to_source[dag.name] = object_path
                staged.append((dag, object_path))

        if collisions:
            # Record for the dashboard banner, log loudly, then abort startup.
            self._record_name_collisions(collisions)
            lines = [f"  - DAG name '{name}' declared in BOTH '{first}' and "
                     f"'{dup}'" for name, first, dup in collisions]
            msg = ("FATAL: DAG name collision(s) detected - names must be "
                   "globally unique across all folders:\n" + "\n".join(lines) +
                   "\nRename or remove the duplicate file(s) and restart.")
            logger.critical(msg)
            raise DAGNameCollisionError(msg)

        with self._lock:
            for dag, _ in staged:
                self.dags[dag.name] = dag
                logger.info(f"Successfully loaded DAG: {dag.name}")

    def _read_dag_config(self, object_path):
        """Read a DAG JSON object via storage and resolve ${...} placeholders.

        Args:
            object_path: Logical storage path of the DAG JSON.

        Returns:
            dict: Fully resolved DAG configuration.
        """
        raw_text = self._storage.read_text(object_path)
        return self._props.resolve_string_json_content(raw_text)

    def get_dag_source_config(self, dag_name):
        """Return the *original* configuration a DAG was created from.

        This is the source-of-truth JSON, distinct from :meth:`details`
        (which returns an enriched, runtime-augmented view). Resolution
        order:

        1. If the DAG has a backing storage object (``config_filename``),
           return the **raw stored file text** verbatim - exactly what the
           user wrote, including any ``${...}`` placeholders.
        2. Otherwise (in-memory designer/clone DAGs), return the in-memory
           ``config`` dict with the internal ``config_filename`` key removed.

        Returns:
            tuple(str|None, dict|None): ``(raw_text, config_dict)``. ``raw_text``
            is the verbatim file contents when available (else None);
            ``config_dict`` is always populated as a convenience.
        """
        with self._lock:
            dag = self.dags.get(dag_name)
            if dag is None:
                raise ValueError(f"DAG {dag_name} not found")
            config = dict(getattr(dag, 'config', {}) or {})
            filename = config.get('config_filename')

        raw_text = None
        if filename:
            try:
                object_path = self._dag_object_path(filename)
                if self._storage.exists(object_path):
                    raw_text = self._storage.read_text(object_path)
            except Exception:  # noqa: BLE001 - fall back to in-memory config
                raw_text = None

        # In-memory convenience copy without the internal bookkeeping key.
        config.pop('config_filename', None)
        return raw_text, config

    def _dag_object_path(self, filename):
        """Map a DAG config filename to its logical storage path.

        v3.1.0: ``config_filename`` may now be a FULL object path (e.g.
        'config/dags/foo.json') so DAGs across multiple folders are
        distinguishable. If it already starts with a configured folder
        prefix, return it unchanged (idempotent); otherwise treat it as a
        bare filename under the primary prefix (legacy / add_dag path).
        """
        if not filename:
            return filename
        prefixes = getattr(self, 'dag_config_prefixes', [self.dag_config_prefix])
        for prefix in prefixes:
            if prefix and (filename == prefix or
                           filename.startswith(prefix + '/')):
                return filename  # already a full path
        return (f"{self.dag_config_prefix}/{filename}"
                if self.dag_config_prefix else filename)

    def _list_storage_dag_objects(self):
        """Return direct-child DAG JSON object paths across ALL configured
        folders, as a list of ``(object_path, base)`` pairs.

        v3.1.0: scans every prefix in ``self.dag_config_prefixes`` (config/dags
        plus any 'storage.dags.prefixes'). Only DIRECT children of each prefix
        are returned - sub-folders (e.g. 'config/dags/examples/') are never
        treated as live DAGs. ``base`` is the prefix-with-slash for that entry,
        used to derive the folder-relative filename.
        """
        prefixes = getattr(self, 'dag_config_prefixes', [self.dag_config_prefix])
        results = []
        for prefix in prefixes:
            try:
                self._storage.ensure_prefix(prefix)
                names = self._storage.list_names(prefix=prefix, suffix='.json')
            except Exception as e:  # noqa: BLE001
                logger.error("Error listing DAGs under '%s': %s", prefix, e)
                continue
            base = f"{prefix}/" if prefix else ""
            for n in names:
                if '/' not in n[len(base):]:  # direct children only
                    results.append((n, base))
        return results

    def reload_from_storage(self, remove_missing=True):
        """Re-scan the DAG storage prefix and reconcile the live registry.

        This is the runtime counterpart to the startup :meth:`_load_dags`.
        It is **primary-only** (mutating the registry on a secondary is
        rejected) and reconciles three groups:

        * **Added**   - JSON objects present in storage but not yet loaded
                        are added (via the same path as :meth:`add_dag`,
                        honouring worker-pool lazy initialisation).
        * **Removed** - storage-backed DAGs whose source object has
                        disappeared are stopped and removed, but only when
                        ``remove_missing`` is True. DAGs created in memory
                        (designer/clone DAGs with no ``config_filename``) are
                        never removed - they have no storage object to miss.
        * **Unchanged** - everything else is left exactly as-is. In
                        particular, this method does NOT restart or rebuild
                        DAGs whose file changed; intraday *schedule* edits are
                        still handled by the schedule-refresh loop, and a full
                        config change should be applied by deleting and
                        re-adding the DAG.

        Args:
            remove_missing: When True (default), stop and remove DAGs whose
                backing storage object is gone. When False, only add new DAGs.

        Returns:
            dict: ``{"added": [...], "removed": [...], "skipped_in_memory":
            [...], "errors": [{"object"/"dag": ..., "error": ...}]}``.
        """
        self._check_primary()

        summary = {"added": [], "removed": [],
                   "skipped_in_memory": [], "errors": [], "collisions": []}

        try:
            dag_objects = self._list_storage_dag_objects()  # [(path, base)]
        except Exception as e:  # noqa: BLE001 - surface, never crash caller
            logger.error("reload_from_storage: cannot list DAG folders: %s", e)
            logger.error(traceback.format_exc())
            summary["errors"].append({"object": "dag_folders",
                                      "error": str(e)})
            return summary

        # Full object paths currently present in storage (across all folders).
        storage_paths = {p for p, _base in dag_objects}

        # Names of DAGs already loaded, keyed by their source object path.
        with self._lock:
            loaded_by_path = {
                dag.config.get('config_filename'): name
                for name, dag in self.dags.items()
                if dag.config.get('config_filename')}

        use_lazy_init = self._worker_pool is not None
        new_collisions = []

        # ---- Additions: storage objects not yet represented in memory ----
        for object_path, _base in dag_objects:
            if object_path in loaded_by_path:
                continue  # already loaded from this exact file
            try:
                config = self._read_dag_config(object_path)
                config['config_filename'] = object_path  # full path key
                dag_name = config.get('name')
                if not dag_name:
                    raise ValueError("DAG config has no 'name'")
                with self._lock:
                    if dag_name in self.dags:
                        # v3.1.0 GLOBAL UNIQUENESS: a DAG with this name is
                        # already loaded from a different file. Incumbent wins;
                        # the newcomer is REJECTED (not booted) and recorded so
                        # the dashboard can show a persistent red banner.
                        existing_src = (self.dags[dag_name].config
                                        .get('config_filename', '<in-memory>'))
                        new_collisions.append(
                            (dag_name, existing_src, object_path))
                        raise DAGNameCollisionError(
                            f"DAG name '{dag_name}' in '{object_path}' "
                            f"collides with already-loaded '{existing_src}'; "
                            f"names must be globally unique - newcomer rejected")
                    dag = ComputeGraph(config, lazy_init=use_lazy_init)
                    self.dags[dag_name] = dag
                summary["added"].append(dag_name)
                logger.info("reload_from_storage: added DAG '%s' from '%s'",
                            dag_name, object_path)
            except DAGNameCollisionError as ce:
                logger.critical("reload_from_storage: %s", ce)
                summary["errors"].append({"object": object_path,
                                          "error": str(ce)})
            except Exception as e:  # noqa: BLE001
                logger.error("reload_from_storage: failed to add '%s': %s",
                             object_path, e)
                logger.error(traceback.format_exc())
                summary["errors"].append({"object": object_path,
                                          "error": str(e)})

        if new_collisions:
            self._record_name_collisions(new_collisions)
            summary["collisions"] = [
                {"name": n, "existing_file": f, "offending_file": d}
                for n, f, d in new_collisions]

        # ---- Removals: storage-backed DAGs whose file disappeared ----
        if remove_missing:
            for object_path, dag_name in loaded_by_path.items():
                if object_path in storage_paths:
                    continue  # still present in some folder
                try:
                    # Reuse delete() so worker-pool unload + stop are handled.
                    self.delete(dag_name)
                    summary["removed"].append(dag_name)
                    logger.info("reload_from_storage: removed DAG '%s' "
                                "(source file '%s' is gone)",
                                dag_name, object_path)
                except Exception as e:  # noqa: BLE001
                    logger.error("reload_from_storage: failed to remove "
                                 "'%s': %s", dag_name, e)
                    logger.error(traceback.format_exc())
                    summary["errors"].append({"dag": dag_name,
                                              "error": str(e)})

        # Note in-memory-only DAGs (designer/clones) that we intentionally
        # leave alone, so callers can report them.
        with self._lock:
            for name, dag in self.dags.items():
                if not dag.config.get('config_filename'):
                    summary["skipped_in_memory"].append(name)

        # ---- v3.0.0: auto-start newly added DAGs (mirrors startup) ----
        # Previously a reloaded DAG was registered but never started, so its
        # subscribers/compute loop stayed idle until a full server restart.
        # Start each just-added DAG if it is perpetual or currently within its
        # schedule, matching _auto_start_eligible_dags() behaviour.
        # Guarded: only when this object exposes start() (the full server);
        # the loader mixin alone cannot start DAGs.
        if hasattr(self, 'start') and callable(getattr(self, 'start')):
            for dag_name in summary["added"]:
                try:
                    dag = self.dags.get(dag_name)
                    if dag is None:
                        continue
                    is_perpetual = ((dag.start_time is None or dag.end_time is None)
                                    and getattr(dag, 'schedule', None) is None)
                    should_start = is_perpetual
                    if not should_start:
                        active, _reason = dag.is_within_schedule()
                        should_start = active
                    if should_start:
                        self.start(dag_name)
                        logger.info("reload_from_storage: auto-started DAG '%s'",
                                    dag_name)
                    else:
                        logger.info("reload_from_storage: DAG '%s' added but not "
                                    "started (schedule inactive)", dag_name)
                except Exception as e:  # noqa: BLE001
                    logger.error("reload_from_storage: failed to auto-start "
                                 "'%s': %s", dag_name, e)
                    logger.error(traceback.format_exc())
                    summary["errors"].append({"dag": dag_name, "error": str(e)})

        logger.info("reload_from_storage complete: +%d added, -%d removed, "
                    "%d errors", len(summary["added"]),
                    len(summary["removed"]), len(summary["errors"]))
        return summary
