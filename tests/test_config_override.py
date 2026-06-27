"""Regression test for the DY_CONFIG_FILE config override (two-node setup).

PropertiesConfigurator is a process singleton, so this is tested in fresh
subprocess interpreters to avoid cross-test contamination. The override must
win regardless of which file the first caller passes (import order can
initialise the singleton before run_server's explicit load), and must be a
no-op when unset.
"""
import os
import subprocess
import sys
import textwrap

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _ensure_configs():
    gen = os.path.join(REPO, "twonode", "generate_configs.py")
    if os.path.exists(gen):
        subprocess.run([sys.executable, gen], cwd=REPO, check=True,
                       capture_output=True, timeout=60)


def _run(config_file):
    code = textwrap.dedent("""
        from core.properties_configurator import PropertiesConfigurator
        # First caller deliberately passes CANONICAL; an override must still win.
        p = PropertiesConfigurator(['config/application.yaml'])
        print(p.get_int('server.port', 5002))
        print(p.get('service.instance_name', '?'))
    """)
    env = dict(os.environ)
    env.pop("DY_CONFIG_FILE", None)
    if config_file is not None:
        env["DY_CONFIG_FILE"] = config_file
    out = subprocess.check_output([sys.executable, "-c", code], cwd=REPO,
                                  env=env, text=True, timeout=60)
    port, name = out.strip().splitlines()[:2]
    return int(port), name


def test_override_wins_over_first_caller():
    _ensure_configs()
    port, name = _run("twonode/configs/nodeB.yaml")
    assert port == 18090
    assert name == "Service-Plane-B"


def test_unset_is_unchanged():
    port, _name = _run(None)
    assert port == 5002   # canonical default, override inactive
