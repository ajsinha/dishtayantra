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


class DAGLoaderMixin:
    """Storage-backed loading of DAG configurations."""

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
        try:
            self._storage.ensure_prefix(self.dag_config_prefix)
        except Exception as e:
            logger.error(f"Error ensuring DAG prefix "
                         f"'{self.dag_config_prefix}': {str(e)}")
            logger.error(traceback.format_exc())
            raise

        try:
            json_objects = self._storage.list_names(
                prefix=self.dag_config_prefix, suffix='.json')
        except Exception as e:
            logger.error(f"Error listing DAG configurations under "
                         f"'{self.dag_config_prefix}': {str(e)}")
            logger.error(traceback.format_exc())
            raise

        # Storage listings are recursive (object stores have no real
        # directories). Match the legacy os.listdir behaviour by keeping
        # only DIRECT children of the prefix - sub-prefixes such as
        # 'config/dags/examples/' hold samples that must not be auto-loaded.
        base = f"{self.dag_config_prefix}/" if self.dag_config_prefix else ""
        json_objects = [name for name in json_objects
                        if '/' not in name[len(base):]]

        if not json_objects:
            logger.warning(
                f"No JSON configuration files found under storage prefix "
                f"'{self.dag_config_prefix}' "
                f"(provider: {self._storage.name})")
            logger.info("You can add DAG configurations via the web UI or "
                        "place JSON files under the configured prefix")
            return

        logger.info(f"Found {len(json_objects)} DAG configuration file(s) "
                    f"under '{self.dag_config_prefix}' "
                    f"(provider: {self._storage.name})")

        # v1.5.2: Use lazy initialization when worker pool is enabled
        # This prevents creating duplicate Kafka consumers in main process
        use_lazy_init = self._worker_pool is not None
        if use_lazy_init:
            logger.info("Worker pool enabled - DAGs will be loaded with lazy initialization")

        for object_path in json_objects:
            try:
                logger.info(f"Loading DAG from storage object: {object_path}")
                config = self._read_dag_config(object_path)
                # v2.1.0: remember the source object name so the intraday
                # schedule-refresh loop can re-read this DAG's config.
                config['config_filename'] = object_path[len(base):]
                dag = ComputeGraph(config, lazy_init=use_lazy_init)
                with self._lock:
                    self.dags[dag.name] = dag
                logger.info(f"Successfully loaded DAG: {dag.name}")
            except Exception as e:
                logger.error(f"Error loading DAG from {object_path}: {str(e)}")
                logger.error(traceback.format_exc())
                raise

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
        """Map a DAG config filename to its logical storage path."""
        return f"{self.dag_config_prefix}/{filename}" if self.dag_config_prefix else filename

    def _list_storage_dag_objects(self):
        """Return the direct-child DAG JSON object paths under the prefix.

        Mirrors the filtering in :meth:`_load_dags` so sub-prefixes such as
        ``config/dags/examples/`` (samples) are never treated as live DAGs.
        """
        self._storage.ensure_prefix(self.dag_config_prefix)
        names = self._storage.list_names(
            prefix=self.dag_config_prefix, suffix='.json')
        base = f"{self.dag_config_prefix}/" if self.dag_config_prefix else ""
        return [n for n in names if '/' not in n[len(base):]], base

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
                   "skipped_in_memory": [], "errors": []}

        try:
            object_paths, base = self._list_storage_dag_objects()
        except Exception as e:  # noqa: BLE001 - surface, never crash caller
            logger.error("reload_from_storage: cannot list storage prefix "
                         "'%s': %s", self.dag_config_prefix, e)
            logger.error(traceback.format_exc())
            summary["errors"].append({"object": self.dag_config_prefix,
                                      "error": str(e)})
            return summary

        # Filenames (relative to the prefix) currently present in storage.
        storage_filenames = {p[len(base):] for p in object_paths}

        # Names of DAGs already loaded, keyed by their source filename.
        with self._lock:
            loaded_by_filename = {
                dag.config.get('config_filename'): name
                for name, dag in self.dags.items()
                if dag.config.get('config_filename')}
            loaded_names = set(self.dags.keys())

        use_lazy_init = self._worker_pool is not None

        # ---- Additions: storage objects not yet represented in memory ----
        for object_path in object_paths:
            filename = object_path[len(base):]
            if filename in loaded_by_filename:
                continue  # already loaded from this file
            try:
                config = self._read_dag_config(object_path)
                config['config_filename'] = filename
                dag_name = config.get('name')
                if not dag_name:
                    raise ValueError("DAG config has no 'name'")
                with self._lock:
                    if dag_name in self.dags:
                        # Same name already loaded from a different source;
                        # don't silently clobber it - flag and skip.
                        raise ValueError(
                            f"a DAG named '{dag_name}' is already loaded "
                            f"from a different source")
                    dag = ComputeGraph(config, lazy_init=use_lazy_init)
                    self.dags[dag_name] = dag
                summary["added"].append(dag_name)
                logger.info("reload_from_storage: added DAG '%s' from '%s'",
                            dag_name, object_path)
            except Exception as e:  # noqa: BLE001
                logger.error("reload_from_storage: failed to add '%s': %s",
                             object_path, e)
                logger.error(traceback.format_exc())
                summary["errors"].append({"object": object_path,
                                          "error": str(e)})

        # ---- Removals: storage-backed DAGs whose file disappeared ----
        if remove_missing:
            for filename, dag_name in loaded_by_filename.items():
                if filename in storage_filenames:
                    continue  # still present
                if dag_name not in loaded_names:
                    continue
                try:
                    # Reuse delete() so worker-pool unload + stop are handled.
                    self.delete(dag_name)
                    summary["removed"].append(dag_name)
                    logger.info("reload_from_storage: removed DAG '%s' "
                                "(source file '%s' is gone)",
                                dag_name, filename)
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
