"""Smoke tests for the free-threading readiness spike (roadmap Phase 0)."""

from benchmarks.freethreading_spike import gil_status, run_spike


def test_gil_status_does_not_crash():
    # Returns None on standard builds, or a bool on free-threaded builds.
    val = gil_status()
    assert val is None or isinstance(val, bool)


def test_run_spike_structure():
    report = run_spike(calls=500, thread_counts=[1, 2], quiet=True)

    for key in (
        "python_version",
        "free_threading_capable",
        "scaling",
        "extensions",
        "verdict",
        "max_speedup",
        "peak_parallel_efficiency",
    ):
        assert key in report, f"missing key: {key}"

    assert isinstance(report["free_threading_capable"], bool)
    assert len(report["scaling"]) == 2
    for row in report["scaling"]:
        assert set(row) >= {"threads", "wall_s", "speedup", "efficiency"}
        assert row["wall_s"] >= 0
    # single-thread baseline always has speedup 1.0
    base = next(r for r in report["scaling"] if r["threads"] == 1)
    assert base["speedup"] == 1.0
    assert isinstance(report["verdict"], str) and report["verdict"]


def test_run_spike_can_skip_extensions():
    report = run_spike(calls=200, thread_counts=[1], check_extensions=False, quiet=True)
    assert report["extensions"] == []
    assert report["scaling"][0]["threads"] == 1
