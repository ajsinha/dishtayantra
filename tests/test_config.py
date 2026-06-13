"""
Tests for the v2.2 format-agnostic configuration system: YAML and
.properties parity, dotted-key flattening, and the ${VAR:default}
resolution fix.
"""
import os

import pytest

from core.config_parsers import (
    ConfigParseError,
    detect_format,
    find_default_config,
    flatten_yaml,
    parse_config_file,
    parse_properties_text,
    parse_yaml_text,
)
from core.properties_configurator import (
    ConfigurationManager,
    PropertiesConfigurator,
)


# ---------------------------------------------------------------- parsing
def test_detect_format():
    assert detect_format('config/application.yaml') == 'yaml'
    assert detect_format('a.yml') == 'yaml'
    assert detect_format('a.properties') == 'properties'
    assert detect_format('a.conf') == 'properties'  # default


def test_flatten_yaml_nesting():
    flat = flatten_yaml({'db': {'sqlite': {'path': 'x'}},
                         'server': {'port': 5002, 'debug': False}})
    assert flat['db.sqlite.path'] == 'x'
    assert flat['server.port'] == '5002'
    assert flat['server.debug'] == 'false'  # bool -> lowercase


def test_flatten_yaml_lists():
    flat = flatten_yaml({'tags': ['x', 'y', 'z']})
    assert flat['tags'] == 'x,y,z'        # joined -> get_list works
    assert flat['tags.0'] == 'x'          # indexed access
    assert flat['tags.2'] == 'z'


def test_flatten_yaml_list_of_mappings():
    flat = flatten_yaml({'servers': [{'host': 'a'}, {'host': 'b'}]})
    assert flat['servers.0.host'] == 'a'
    assert flat['servers.1.host'] == 'b'
    assert 'servers' not in flat  # no join for non-scalar elements


def test_parse_properties_text():
    flat = parse_properties_text(
        '# comment\na.b=1\n// also comment\n\nc.d = two = three\n')
    assert flat['a.b'] == '1'
    assert flat['c.d'] == 'two = three'  # split on first '=' only


def test_yaml_root_must_be_mapping():
    with pytest.raises(ConfigParseError):
        parse_yaml_text('- just\n- a\n- list\n')


def test_invalid_yaml_raises():
    with pytest.raises(ConfigParseError):
        parse_yaml_text('key: [unclosed\n')


# ---------------------------------------------------------------- parity
def test_yaml_properties_parity():
    """The shipped YAML and .properties files resolve to the same values
    for every shared key (two documented cosmetic diffs excluded)."""
    yk = parse_config_file('config/application.yaml')
    pk = parse_config_file('config/application.properties')
    cosmetic = {'app.trademark.notice', 'server.debug'}
    shared = (set(yk) & set(pk)) - cosmetic
    diffs = {k: (yk[k], pk[k]) for k in shared if yk[k] != pk[k]}
    assert not diffs, f"unexpected value diffs: {diffs}"


def test_find_default_config_prefers_yaml(tmp_path):
    (tmp_path / 'application.properties').write_text('a=1')
    assert find_default_config(str(tmp_path)).endswith('.properties')
    (tmp_path / 'application.yaml').write_text('a: 1')
    assert find_default_config(str(tmp_path)).endswith('.yaml')


def test_find_default_config_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        find_default_config(str(tmp_path))


# ---------------------------------------------------------------- engine
@pytest.fixture()
def fresh_singleton():
    """Reset the configurator singleton around a test, then RESTORE the
    original session-wide singleton so later tests (e.g. the webapp fixture)
    still see the repository configuration. Previously this left _instance as
    None, which silently broke any subsequent test that relied on the shared
    singleton depending on collection order."""
    saved = PropertiesConfigurator._instance
    PropertiesConfigurator._instance = None
    yield
    if PropertiesConfigurator._instance is not None:
        PropertiesConfigurator._instance.stop_reload()
    PropertiesConfigurator._instance = saved


def test_configuration_manager_alias():
    assert ConfigurationManager is PropertiesConfigurator


def test_yaml_loads_and_coerces(tmp_path, fresh_singleton):
    cfg = tmp_path / 'app.yaml'
    cfg.write_text(
        'server:\n  port: 5002\n  debug: false\n'
        'tags:\n  - a\n  - b\n')
    pc = PropertiesConfigurator([str(cfg)])
    assert pc.get('server.port') == '5002'
    assert pc.get_int('server.port') == 5002
    assert pc.get_bool('server.debug') is False
    assert pc.get_list('tags') == ['a', 'b']


def test_var_default_syntax_resolves(tmp_path, fresh_singleton, monkeypatch):
    """v2.2 BUGFIX: ${VAR:default} now applies the default when VAR is
    unset, and the env value when it is set. Previously the whole
    'VAR:default' string was treated as a key and never resolved."""
    monkeypatch.delenv('DY_TEST_SECRET', raising=False)
    cfg = tmp_path / 'app.yaml'
    cfg.write_text('app:\n  secret: "${DY_TEST_SECRET:fallback-value}"\n')
    pc = PropertiesConfigurator([str(cfg)])
    assert pc.get('app.secret') == 'fallback-value'


def test_var_default_env_override(tmp_path, fresh_singleton, monkeypatch):
    monkeypatch.setenv('DY_TEST_SECRET', 'from-environment')
    cfg = tmp_path / 'app.yaml'
    cfg.write_text('app:\n  secret: "${DY_TEST_SECRET:fallback-value}"\n')
    pc = PropertiesConfigurator([str(cfg)])
    assert pc.get('app.secret') == 'from-environment'


def test_nested_variable_resolution(tmp_path, fresh_singleton):
    cfg = tmp_path / 'app.yaml'
    cfg.write_text(
        'base:\n  dir: /opt/app\n'
        'logs:\n  dir: "${base.dir}/logs"\n'
        'archive:\n  dir: "${logs.dir}/archive"\n')
    pc = PropertiesConfigurator([str(cfg)])
    assert pc.get('logs.dir') == '/opt/app/logs'
    assert pc.get('archive.dir') == '/opt/app/logs/archive'


def test_properties_format_still_works(tmp_path, fresh_singleton):
    cfg = tmp_path / 'app.properties'
    cfg.write_text('a.b=hello\nport=8080\n')
    pc = PropertiesConfigurator([str(cfg)])
    assert pc.get('a.b') == 'hello'
    assert pc.get_int('port') == 8080
