"""Tests for per-DAG log attribution (v2.2)."""
from routes.admin_log_routes import extract_dag_name


def test_extract_dag_name_recognizes_convention():
    assert extract_dag_name(
        'DAG scheduled_heartbeat: started') == 'scheduled_heartbeat'
    assert extract_dag_name('DAG heartbeat_monitor: suspended') == \
        'heartbeat_monitor'
    assert extract_dag_name('DAG my-dag.v2: ok') == 'my-dag.v2'


def test_extract_dag_name_ignores_non_dag_lines():
    # Cross-cutting / server lines must NOT be attributed to a DAG.
    assert extract_dag_name('HA: DEMOTED to secondary DAG server') is None
    assert extract_dag_name('DAGs (Total: 2)') is None
    assert extract_dag_name('Loaded DAG configuration from disk') is None
    assert extract_dag_name('  start_time=1100, duration=01h00m') is None
    assert extract_dag_name('') is None
    assert extract_dag_name(None) is None


def test_parse_log_file_filters_by_dag(tmp_path):
    from routes.admin_log_routes import AdminLogRoutes
    log = tmp_path / 'dagserver.log'
    log.write_text(
        '2026-06-13 10:00:00 - INFO - DAG alpha: started\n'
        '2026-06-13 10:00:01 - INFO - DAGComputeServer initialized\n'
        '2026-06-13 10:00:02 - INFO - DAG beta: started\n'
        '2026-06-13 10:00:03 - WARNING - DAG alpha: slow sweep\n')
    inst = AdminLogRoutes.__new__(AdminLogRoutes)
    all_e = inst._parse_log_file(str(log))
    assert len(all_e) == 4
    assert sorted({e['dag'] for e in all_e if e['dag']}) == ['alpha', 'beta']
    alpha = inst._parse_log_file(str(log), dag_filter='alpha')
    assert len(alpha) == 2
    assert all(e['dag'] == 'alpha' for e in alpha)
