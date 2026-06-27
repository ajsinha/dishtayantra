"""
DAG Designer Routes (FastAPI, v2.0.0)
=====================================

Visual DAG designer pages plus the validation and component-catalogue JSON
APIs.  v2.0.0 adds the cloud object-store pub/sub entries (``s3://``,
``azureblob://``, ``gcs://``) to the component catalogue.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.dag.designer_catalogue import build_component_catalogue

from web.fastapi_compat import (
    AuthGuards,
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


class DAGDesignerRoutes:
    """Handles DAG Designer-related routes."""

    def __init__(self, app: FastAPI, dag_server, user_registry,
                 guards: AuthGuards):
        self.app = app
        self.dag_server = dag_server
        self.user_registry = user_registry
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/dag-designer', self.dag_designer, methods=['GET'],
            name='dag_designer', include_in_schema=False)
        add('/dag-designer/load/{dag_name}', self.dag_designer_load,
            methods=['GET'], name='dag_designer_load',
            include_in_schema=False)
        add('/api/dag-designer/validate', self.validate_dag,
            methods=['POST'], name='dag_designer_validate')
        add('/api/dag-designer/components', self.get_available_components,
            methods=['GET'], name='dag_designer_components')
        # v3.1.0: deploy a designed DAG straight to the server.
        add('/api/dag-designer/folders', self.get_dag_folders,
            methods=['GET'], name='dag_designer_folders')
        add('/api/dag-designer/save', self.save_dag,
            methods=['POST'], name='dag_designer_save')

    # ------------------------------------------------------------------ #
    # Pages
    # ------------------------------------------------------------------ #

    def dag_designer(self, request: Request):
        """Main DAG Designer view."""
        username = self.guards.login_required(request)
        try:
            dags = self.dag_server.list_dags()
            dag_names = [dag['name'] for dag in dags]
            return render(request, 'dag/designer.html',
                          existing_dags=dag_names,
                          is_admin=self.user_registry.has_role(username,
                                                               'admin'))
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error loading DAG Designer', e)
            return render(request, 'dag/designer.html', existing_dags=[],
                          is_admin=False)

    def dag_designer_load(self, request: Request, dag_name: str):
        """Load an existing DAG into the designer."""
        username = self.guards.login_required(request)
        try:
            if dag_name not in self.dag_server.dags:
                return redirect_to(request, 'dag_designer',
                                   flash_message=f'DAG "{dag_name}" not '
                                                 f'found',
                                   flash_category='error')
            dag = self.dag_server.dags[dag_name]
            dag_config = dag.config

            dags = self.dag_server.list_dags()
            dag_names = [d['name'] for d in dags]
            return render(request, 'dag/designer.html',
                          existing_dags=dag_names,
                          loaded_dag=json.dumps(dag_config, indent=2),
                          loaded_dag_name=dag_name,
                          is_admin=self.user_registry.has_role(username,
                                                               'admin'))
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading DAG', e)
            return redirect_to(request, 'dag_designer')

    # ------------------------------------------------------------------ #
    # JSON APIs
    # ------------------------------------------------------------------ #

    async def validate_dag(self, request: Request):
        """Validate a DAG configuration (API endpoint)."""
        self.guards.login_required(request)
        try:
            dag_config = await request.json()
            errors, warnings = self._validate_config(dag_config)
            return JSONResponse({'valid': len(errors) == 0,
                                 'errors': errors,
                                 'warnings': warnings})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error validating DAG: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'valid': False,
                                 'errors': [f"Validation error: {str(e)}"],
                                 'warnings': []}, status_code=500)

    def _validate_config(self, dag_config):
        """Structurally validate a DAG config; return (errors, warnings).

        Shared by both the Validate button and Save/Deploy so the two can
        never drift apart. Pure function of the supplied config - it does NOT
        check name collisions against the running set (the caller does that).
        """
        errors = []
        warnings = []

        # Basic validation
        if not dag_config.get('name'):
            errors.append("DAG name is required")
        if not dag_config.get('nodes') or \
                len(dag_config.get('nodes', [])) == 0:
            errors.append("At least one node is required")

        # Collect names defensively: every node/component must carry a 'name'. A missing
        # 'name' (e.g. legacy id-based DAGs that use 'id') is reported as a clear error
        # rather than raising a raw KeyError that reaches the user as "Save failed:
        # 'name'".
        def _safe_names(items, kind):
            collected = set()
            for it in items:
                if not isinstance(it, dict) or not it.get('name'):
                    errors.append(f"A {kind} entry is missing the required 'name' "
                                  f"field (legacy id-based or malformed config)")
                else:
                    collected.add(it['name'])
            return collected

        # Validate edges reference existing nodes
        node_names = _safe_names(dag_config.get('nodes', []), 'node')
        from core.dag.compute_graph_builders import IMPLICIT_TRANSFORMERS
        transformer_names = _safe_names(dag_config.get('transformers', []),
                                        'transformer')
        # passthru / null resolve implicitly even without an explicit definition.
        transformer_names |= set(IMPLICIT_TRANSFORMERS.keys())

        for edge in dag_config.get('edges', []):
            if edge.get('from_node') not in node_names:
                errors.append(f"Edge references non-existent source "
                              f"node: {edge.get('from_node')}")
            if edge.get('to_node') not in node_names:
                errors.append(f"Edge references non-existent target "
                              f"node: {edge.get('to_node')}")
            if edge.get('data_transformer') and \
                    edge.get('data_transformer') not in transformer_names:
                warnings.append(
                    f"Edge from '{edge.get('from_node')}' to "
                    f"'{edge.get('to_node')}' references transformer "
                    f"'{edge.get('data_transformer')}' which may be a "
                    f"custom type")

        # Cycle detection using DFS
        cycles = self._detect_cycles(dag_config.get('nodes', []),
                                     dag_config.get('edges', []))
        for cycle in cycles:
            cycle_path = ' -> '.join(cycle)
            errors.append(f"Cycle detected: {cycle_path}")

        # Validate node references
        subscriber_names = _safe_names(dag_config.get('subscribers', []),
                                       'subscriber')
        publisher_names = _safe_names(dag_config.get('publishers', []),
                                      'publisher')
        calculator_names = _safe_names(dag_config.get('calculators', []),
                                       'calculator')

        # Reference fields must be plain string names (or lists of them). Legacy DAGs
        # sometimes inline a dict here (e.g. "calculator": {"type": "cpp", ...}); treat
        # any non-string as a clear error instead of letting `dict not in set` raise an
        # unhashable-type TypeError.
        def _ref_ok(node_label, field, value):
            if isinstance(value, str):
                return True
            errors.append(f"Node '{node_label}' has a non-string {field} reference "
                          f"(got {type(value).__name__}); expected a component name "
                          f"(legacy inline definition is not supported)")
            return False

        for node in dag_config.get('nodes', []):
            if not isinstance(node, dict):
                continue
            node_label = node.get('name', '<unnamed>')
            sub = node.get('subscriber')
            if sub and _ref_ok(node_label, 'subscriber', sub) and \
                    sub not in subscriber_names:
                errors.append(f"Node '{node_label}' references "
                              f"non-existent subscriber: {sub}")
            pubs = node.get('publishers', [])
            for pub in (pubs if isinstance(pubs, list) else []):
                if _ref_ok(node_label, 'publisher', pub) and \
                        pub not in publisher_names:
                    errors.append(f"Node '{node_label}' references "
                                  f"non-existent publisher: {pub}")
            calc = node.get('calculator')
            if calc and _ref_ok(node_label, 'calculator', calc) and \
                    calc not in calculator_names:
                errors.append(f"Node '{node_label}' references "
                              f"non-existent calculator: {calc}")
            itrans = node.get('input_transformers', [])
            for trans in (itrans if isinstance(itrans, list) else []):
                if _ref_ok(node_label, 'input_transformer', trans) and \
                        trans not in transformer_names:
                    errors.append(f"Node '{node_label}' references "
                                  f"non-existent input transformer: "
                                  f"{trans}")
            otrans = node.get('output_transformers', [])
            for trans in (otrans if isinstance(otrans, list) else []):
                if _ref_ok(node_label, 'output_transformer', trans) and \
                        trans not in transformer_names:
                    errors.append(f"Node '{node_label}' references "
                                  f"non-existent output transformer: "
                                  f"{trans}")

        # Warnings
        if dag_config.get('start_time') and \
                not dag_config.get('duration'):
            warnings.append("start_time is set but duration is not "
                            "specified. Default duration (-5 minutes) "
                            "will be used.")

        if dag_config.get('autoclone'):
            autoclone = dag_config['autoclone']
            if not autoclone.get('ramp_up_time'):
                errors.append("autoclone.ramp_up_time is required when "
                              "autoclone is configured")
            if not autoclone.get('ramp_count'):
                errors.append("autoclone.ramp_count is required when "
                              "autoclone is configured")

        return errors, warnings

    def _detect_cycles(self, nodes, edges):
        """
        Detect cycles in the DAG using depth-first search.

        Returns:
            list[list[str]]: cycles found, each a list of node names ending
            with the repeated start node.
        """
        graph = {}
        node_names = {n['name'] for n in nodes
                      if isinstance(n, dict) and n.get('name')}

        for name in node_names:
            graph[name] = []
        for edge in edges:
            from_node = edge.get('from_node')
            to_node = edge.get('to_node')
            if from_node in graph and to_node in node_names:
                graph[from_node].append(to_node)

        cycles = []
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    cycle_start_idx = path.index(neighbor)
                    return path[cycle_start_idx:] + [neighbor]
            path.pop()
            rec_stack.remove(node)
            return None

        for node_name in node_names:
            if node_name not in visited:
                cycle = dfs(node_name)
                if cycle:
                    cycles.append(cycle)
                    # Reset for finding additional cycles
                    visited = set()
                    rec_stack = set()
                    path = []
                    for n in cycle[:-1]:
                        visited.add(n)
        return cycles

    def get_available_components(self, request: Request):
        """Return the component catalogue used by the designer palette.

        The catalogue itself lives in :mod:`core.dag.designer_catalogue`
        (single source of truth); the Designer palette renders dynamically
        from this same endpoint so the two can never drift.
        """
        self.guards.login_required(request)
        return JSONResponse(build_component_catalogue())

    # ------------------------------------------------------------------ #
    # Save / Deploy to server (v3.1.0)
    # ------------------------------------------------------------------ #

    def get_dag_folders(self, request: Request):
        """Return the configured DAG folders a design can be saved into.

        The primary folder (config/dags) is always first; any additional
        'storage.dags.prefixes' folders follow. The designer uses this to let
        the user pick a destination folder when deploying.
        """
        self.guards.admin_required(request)
        prefixes = getattr(self.dag_server, 'dag_config_prefixes', None)
        if not prefixes:
            prefixes = [getattr(self.dag_server, 'dag_config_prefix',
                                'config/dags')]
        return JSONResponse({
            'folders': list(prefixes),
            'default': prefixes[0],
        })

    async def save_dag(self, request: Request):
        """Validate a designed DAG, persist it to a chosen folder, and reload.

        This closes the loop with the v3.1.0 multi-folder loading, reload
        auto-start, and global-name-uniqueness features: a design goes from
        canvas to a running DAG in one action, with a collision pre-check so
        we never clobber or duplicate an existing DAG name.
        """
        self.guards.admin_required(request)
        try:
            payload = await request.json()
            config = payload.get('dag') or {}
            folder = (payload.get('folder') or '').strip().strip('/')
            overwrite = bool(payload.get('overwrite'))

            dag_name = (config.get('name') or '').strip()
            if not dag_name:
                return JSONResponse(
                    {'success': False,
                     'errors': ['DAG name is required to save.']},
                    status_code=400)

            # Reuse the same structural validation the Validate button uses.
            errors, warnings = self._validate_config(config)
            if errors:
                return JSONResponse(
                    {'success': False, 'errors': errors, 'warnings': warnings},
                    status_code=400)

            # Resolve the destination folder against the configured set.
            prefixes = getattr(self.dag_server, 'dag_config_prefixes',
                               None) or [getattr(self.dag_server,
                                                 'dag_config_prefix',
                                                 'config/dags')]
            if not folder:
                folder = prefixes[0]
            if folder not in prefixes:
                return JSONResponse(
                    {'success': False,
                     'errors': [f"Folder '{folder}' is not a configured DAG "
                                f"folder. Choose one of: "
                                f"{', '.join(prefixes)}"]},
                    status_code=400)

            object_path = f"{folder}/{dag_name}.json"

            # Global-name-uniqueness pre-check (the policy enforced at load):
            # a name already in use by a DIFFERENT file is a hard collision.
            existing = {d['name']: d for d in self.dag_server.list_dags()}
            if dag_name in existing and not overwrite:
                incumbent_path = existing[dag_name].get('config_filename', '')
                return JSONResponse(
                    {'success': False, 'collision': True,
                     'existing_file': incumbent_path,
                     'target_file': object_path,
                     'errors': [
                         f"A DAG named '{dag_name}' already exists"
                         + (f" (from '{incumbent_path}')" if incumbent_path
                            else '')
                         + ". Saving will overwrite it - confirm to proceed."]},
                    status_code=409)

            # Persist via the storage abstraction, then reload so the running
            # set picks it up (reload auto-starts eligible new DAGs).
            storage = self.dag_server._storage
            storage.write_text(object_path, json.dumps(config, indent=2))
            logger.info("DAG Designer deployed '%s' to '%s'",
                        dag_name, object_path)

            reloaded = False
            try:
                self.dag_server.reload_from_storage()
                reloaded = True
            except Exception as e:  # noqa: BLE001 - report, don't hide
                logger.error("Deployed '%s' but reload failed: %s",
                             dag_name, e)
                return JSONResponse(
                    {'success': True, 'reloaded': False,
                     'object_path': object_path,
                     'warnings': warnings + [
                         f"Saved to '{object_path}', but the live reload "
                         f"failed ({e}). The DAG will load on next restart."]})

            return JSONResponse({
                'success': True, 'reloaded': reloaded,
                'object_path': object_path, 'dag_name': dag_name,
                'warnings': warnings,
                'message': f"Deployed '{dag_name}' to '{object_path}'."
                           + (' Reloaded and running.' if reloaded else ''),
            })
        except Exception as e:  # noqa: BLE001 - surface error to the UI
            logger.error("DAG Designer save failed: %s\n%s",
                         e, traceback.format_exc())
            return JSONResponse(
                {'success': False, 'errors': [f"Save failed: {e}"]},
                status_code=500)
