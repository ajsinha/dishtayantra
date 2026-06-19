"""
core/dag/dag_view.py - Build the DAG "Details" view-model from a live ComputeGraph.

This is the single source of truth for the data the details page renders. It runs
in two places so the UI is identical no matter where a DAG executes:

  * the dashboard route, for DAGs running in this (main) process; and
  * the worker process, which builds the same view-model for a DAG running in a
    worker subprocess and ships it back over the status channel.

Everything returned is JSON/pickle-safe so it survives the worker IPC boundary.
The rolling rate-history sparkline is intentionally NOT included here - that is
owned by the main process and appended by the route.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def json_safe(value):
    """Coerce a value to something JSON/pickle-serializable for the payload.

    datetimes -> ISO string; primitives pass through; everything else -> str.
    """
    if value is None:
        return None
    from datetime import datetime, date
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _role(type_name):
    """Node-type -> semantic role used for colouring in the SVG graph."""
    t = (type_name or '').lower()
    if 'subscription' in t or 'metronome' in t:
        return 'source'
    if 'publishersink' in t or 'sink' in t:
        return 'sink'
    if 'subgraph' in t:
        return 'subgraph'
    return 'compute'


def _coerce_errors(raw):
    """Normalise node errors to the {time, error} dicts the template expects."""
    out = []
    for err in (raw or []):
        if isinstance(err, dict):
            out.append({'time': json_safe(err.get('time', 'Unknown')),
                        'error': json_safe(err.get('error', err))})
        else:
            out.append({'time': 'Unknown', 'error': str(err)})
    return out


def build_dag_view(dag):
    """Return {details, node_details, graph_data, dag_stats} for a built DAG.

    Mirrors exactly what the dashboard route used to assemble inline, so a DAG
    running in a worker renders identically to one in the main process. dag_stats
    carries no 'rate_history' (the route appends that from its own deque).
    """
    details = dag.details()

    try:
        sorted_nodes = dag.topological_sort()
    except Exception:  # noqa: BLE001 - never block the page on sort failure
        sorted_nodes = list(getattr(dag, 'nodes', {}).values())

    node_details = []
    graph_nodes = []
    graph_edges = []
    for node in sorted_nodes:
        deps = [edge.from_node.name for edge in node._incoming_edges]
        last_compute = json_safe(getattr(node, '_last_compute', None))
        errors = _coerce_errors(getattr(node, '_errors', []))
        node_details.append({
            'name': node.name,
            'type': node.__class__.__name__,
            'dependencies': deps,
            'config': getattr(node, 'config', {}),
            'last_calculation': last_compute,
            'errors': errors,
        })
        calc = getattr(node, '_calculator', None)
        graph_nodes.append({
            'id': node.name,
            'label': node.name,
            'type': node.__class__.__name__,
            'role': _role(node.__class__.__name__),
            'dependencies': deps,
            'calculator': (getattr(calc, 'name', None)
                           or (calc.__class__.__name__
                               if calc is not None else None)),
            'calculation_count': getattr(calc, '_calculation_count', None)
            if calc else None,
            'last_calculation': last_compute,
            'error_count': len(getattr(node, '_errors', []) or []),
            'messages_in': getattr(node, '_messages_in', 0),
            'messages_out': getattr(node, '_published_count',
                                    getattr(node, '_messages_out', 0)),
            'streaming': getattr(node, '_streaming', False),
            'last_error': (errors[-1] if errors else None),
        })
        for edge in node._incoming_edges:
            edge_obj = {'from': edge.from_node.name, 'to': node.name}
            transformer = getattr(edge, 'data_transformer', None)
            if transformer is not None:
                edge_obj['label'] = getattr(transformer, 'name', None) \
                    or transformer.__class__.__name__
            pname = getattr(edge, 'pname', None)
            if pname:
                edge_obj['pname'] = pname
            graph_edges.append(edge_obj)
    graph_data = {'nodes': graph_nodes, 'edges': graph_edges}

    # Throughput: messages ingested (per subscriber) and published (per sink node),
    # plus per-source / per-sink breakdowns including live queue depth.
    ingested_total = 0
    rate_total = 0.0
    peak_total = 0.0
    ingest_breakdown = []
    # True per-minute ingest counts summed across subscribers (for the histogram).
    # The window length is taken from the meters themselves so it can never drift
    # from RateMeter._minute_window.
    bucket_completed = None
    bucket_partial = 0
    window = 30  # fallback only if no subscriber exposes minute buckets
    for sub_name, sub in (getattr(dag, 'subscribers', {}) or {}).items():
        cnt = int(getattr(sub, '_receive_count', 0) or 0)
        ingested_total += cnt
        meter = getattr(sub, '_rate_meter', None)
        sub_rate = meter.rate_per_minute() if meter else 0.0
        sub_peak = meter.peak_per_minute() if meter else 0.0
        rate_total += sub_rate
        peak_total += sub_peak
        if meter is not None and hasattr(meter, 'minute_buckets'):
            mb = meter.minute_buckets()
            comp = mb.get('completed') or []
            if bucket_completed is None:
                window = mb.get('window_minutes', len(comp)) or len(comp)
                bucket_completed = [0] * len(comp)
            for i in range(min(len(bucket_completed), len(comp))):
                bucket_completed[i] += comp[i]
            bucket_partial += mb.get('current_partial', 0)
        ingest_breakdown.append({
            'name': sub_name,
            'source': getattr(sub, 'source', ''),
            'received': cnt,
            'rate_per_minute': round(sub_rate, 1),
            'frozen': (sub.is_frozen() if hasattr(sub, 'is_frozen') else False),
            'queue_depth': (sub.get_queue_size()
                            if hasattr(sub, 'get_queue_size') else None),
        })
    if bucket_completed is None:
        bucket_completed = [0] * window
    published_total = 0
    publish_breakdown = []
    for node in sorted_nodes:
        pub_cnt = getattr(node, '_published_count', None)
        if pub_cnt is not None:
            published_total += int(pub_cnt)
            publish_breakdown.append({'name': node.name,
                                      'published': int(pub_cnt)})

    dag_stats = {
        'messages_ingested': ingested_total,
        'messages_published': published_total,
        'messages_per_minute': round(rate_total, 1),
        'peak_per_minute': round(peak_total, 1),
        'ingest_breakdown': ingest_breakdown,
        'publish_breakdown': publish_breakdown,
        # True per-minute ingest counts (tumbling 1-min windows), oldest->newest,
        # plus the in-progress partial minute. Used for the honest histogram.
        'ingest_buckets': {
            'completed': bucket_completed,
            'current_partial': bucket_partial,
            'window_minutes': window,
        },
        # Exact count for the most recently *completed* 1-minute window.
        'last_full_minute': bucket_completed[-1] if bucket_completed else 0,
        # v5.15.0: drain/maintenance status (frozen subscribers + what's left
        # queued). Computed here so it flows through the worker snapshot too.
        'drain': (dag.drain_status() if hasattr(dag, 'drain_status') else {
            'frozen_subscribers': [], 'any_frozen': False,
            'subscriber_queue_total': 0, 'publisher_queue_total': 0,
            'wal_pending': 0, 'drained': True}),
    }

    return {
        'details': details,
        'node_details': node_details,
        'graph_data': graph_data,
        'dag_stats': dag_stats,
    }
