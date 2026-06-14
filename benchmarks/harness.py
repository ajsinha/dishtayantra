"""Measurement framework for DishtaYantra benchmarks.

Provides:
  * percentile()            - dependency-free percentile over a sample
  * LatencyStats            - summary stats (ms) over a list of latencies
  * MemorySampler           - background peak-RSS sampler
  * BenchmarkResult         - structured result with to_dict / to_markdown
  * run_inmemory_dag_benchmark - drive a ComputeGraph over the in-memory
                                 broker, measuring end-to-end latency
                                 (correlated per message by trade_id),
                                 throughput, and peak memory.

This is intentionally engine-agnostic at its core: the latency/throughput/
memory machinery works for any workload. The in-memory DAG runner is the
first reference workload (roadmap Phase 0: "make it CI-runnable").
"""

from __future__ import annotations

import json
import sys
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional


# --------------------------------------------------------------------------- #
# Stats helpers
# --------------------------------------------------------------------------- #
def percentile(sorted_values: List[float], pct: float) -> float:
    """Linear-interpolated percentile over an already-sorted list.

    pct is in [0, 100]. Returns 0.0 for an empty sample.
    """
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


@dataclass
class LatencyStats:
    """Latency summary in milliseconds."""

    count: int = 0
    mean_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    max_ms: float = 0.0

    @classmethod
    def from_seconds(cls, latencies_s: List[float]) -> "LatencyStats":
        if not latencies_s:
            return cls()
        ms = sorted(v * 1000.0 for v in latencies_s)
        return cls(
            count=len(ms),
            mean_ms=sum(ms) / len(ms),
            p50_ms=percentile(ms, 50),
            p95_ms=percentile(ms, 95),
            p99_ms=percentile(ms, 99),
            max_ms=ms[-1],
        )


def gil_status() -> Optional[bool]:
    """True/False if GIL enabled, or None if it can't be determined."""
    fn = getattr(sys, "_is_gil_enabled", None)
    if fn is None:
        return None
    try:
        return bool(fn())
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Memory sampling
# --------------------------------------------------------------------------- #
class MemorySampler:
    """Background sampler that tracks peak RSS (MB) while running."""

    def __init__(self, interval_s: float = 0.05):
        self.interval_s = interval_s
        self.peak_rss_mb = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._rss_fn = self._pick_rss_fn()

    @staticmethod
    def _pick_rss_fn() -> Optional[Callable[[], float]]:
        try:
            import psutil  # type: ignore

            proc = psutil.Process()
            return lambda: proc.memory_info().rss / (1024 * 1024)
        except Exception:
            try:
                import resource

                # ru_maxrss is KB on Linux, bytes on macOS.
                factor = 1024.0 if sys.platform != "darwin" else 1024.0 * 1024.0
                return lambda: resource.getrusage(
                    resource.RUSAGE_SELF
                ).ru_maxrss / factor * (1.0 / 1024.0) * 1024.0
            except Exception:
                return None

    def _run(self) -> None:
        if self._rss_fn is None:
            return
        while not self._stop.is_set():
            try:
                self.peak_rss_mb = max(self.peak_rss_mb, self._rss_fn())
            except Exception:
                pass
            self._stop.wait(self.interval_s)

    def __enter__(self) -> "MemorySampler":
        if self._rss_fn is not None:
            try:
                self.peak_rss_mb = self._rss_fn()
            except Exception:
                self.peak_rss_mb = 0.0
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)


# --------------------------------------------------------------------------- #
# Result
# --------------------------------------------------------------------------- #
@dataclass
class BenchmarkResult:
    name: str
    n_messages: int
    delivered: int
    duration_s: float
    throughput_msg_s: float
    latency: LatencyStats = field(default_factory=LatencyStats)
    peak_rss_mb: float = 0.0
    python_version: str = field(default_factory=lambda: sys.version.split()[0])
    gil_enabled: Optional[bool] = field(default_factory=gil_status)
    timestamp: str = field(
        default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S")
    )
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    def summary(self) -> str:
        gil = (
            "n/a"
            if self.gil_enabled is None
            else ("on" if self.gil_enabled else "OFF (free-threaded)")
        )
        lat = self.latency
        return (
            f"[{self.name}] {self.delivered}/{self.n_messages} msgs in "
            f"{self.duration_s:.2f}s -> {self.throughput_msg_s:,.0f} msg/s | "
            f"latency ms p50={lat.p50_ms:.2f} p95={lat.p95_ms:.2f} "
            f"p99={lat.p99_ms:.2f} max={lat.max_ms:.2f} | "
            f"peak RSS {self.peak_rss_mb:.0f} MB | py{self.python_version} "
            f"gil={gil}"
        )

    def to_markdown(self) -> str:
        lat = self.latency
        gil = (
            "n/a"
            if self.gil_enabled is None
            else ("on" if self.gil_enabled else "OFF (free-threaded)")
        )
        return (
            f"### {self.name}\n\n"
            f"| metric | value |\n|---|---|\n"
            f"| messages delivered | {self.delivered}/{self.n_messages} |\n"
            f"| duration (s) | {self.duration_s:.3f} |\n"
            f"| throughput (msg/s) | {self.throughput_msg_s:,.0f} |\n"
            f"| latency p50 (ms) | {lat.p50_ms:.3f} |\n"
            f"| latency p95 (ms) | {lat.p95_ms:.3f} |\n"
            f"| latency p99 (ms) | {lat.p99_ms:.3f} |\n"
            f"| latency max (ms) | {lat.max_ms:.3f} |\n"
            f"| peak RSS (MB) | {self.peak_rss_mb:.0f} |\n"
            f"| python | {self.python_version} (gil {gil}) |\n"
            f"| timestamp | {self.timestamp} |\n"
        )


# --------------------------------------------------------------------------- #
# In-memory DAG benchmark
# --------------------------------------------------------------------------- #
def _extract(msg: Any, key: str) -> Any:
    """Pull a key out of a queue message that may be a dict or JSON string."""
    if isinstance(msg, dict):
        return msg.get(key)
    if isinstance(msg, (bytes, bytearray)):
        try:
            msg = msg.decode("utf-8")
        except Exception:
            return None
    if isinstance(msg, str):
        try:
            return json.loads(msg).get(key)
        except Exception:
            return None
    return None


def run_inmemory_dag_benchmark(
    name: str,
    config: Dict[str, Any],
    input_queue: str,
    output_queue: str,
    n_messages: int,
    gen_fn: Callable[[int], Dict[str, Any]],
    *,
    correlation_key: str = "trade_id",
    warmup_messages: int = 50,
    pace_hz: Optional[float] = None,
    drain_timeout_s: float = 30.0,
    startup_s: float = 0.5,
) -> BenchmarkResult:
    """Drive a ComputeGraph over the in-memory broker and measure it.

    The publisher runs on a background thread while the main thread drains the
    output queue concurrently, so latency reflects pipeline transit time
    (correlated per message by ``correlation_key``) rather than pure backlog
    draining. Throughput is delivered / wall-clock.
    """
    # Imported lazily so importing the package never drags in the engine.
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub

    graph = ComputeGraph(config)
    graph.start()
    time.sleep(startup_s)

    ps = InMemoryPubSub()

    # Warmup (not measured) to let JIT / caches / threads settle.
    for i in range(warmup_messages):
        msg = gen_fn(-(i + 1))  # negative ids so they don't collide
        ps.publish_to_queue(input_queue, json.dumps(msg))
    # Drain warmup output.
    warm_deadline = time.time() + 5.0
    drained = 0
    while drained < warmup_messages and time.time() < warm_deadline:
        if ps.consume_from_queue(output_queue) is not None:
            drained += 1
        else:
            time.sleep(0.005)

    publish_ts: Dict[Any, float] = {}
    publish_lock = threading.Lock()
    interval = (1.0 / pace_hz) if pace_hz else 0.0

    def _publish() -> None:
        for i in range(n_messages):
            msg = gen_fn(i)
            cid = msg.get(correlation_key)
            with publish_lock:
                publish_ts[cid] = time.perf_counter()
            ps.publish_to_queue(input_queue, json.dumps(msg))
            if interval:
                time.sleep(interval)

    latencies: List[float] = []
    delivered = 0

    with MemorySampler() as mem:
        t0 = time.perf_counter()
        pub_thread = threading.Thread(target=_publish, daemon=True)
        pub_thread.start()

        deadline = time.time() + drain_timeout_s
        while delivered < n_messages and time.time() < deadline:
            msg = ps.consume_from_queue(output_queue)
            if msg is None:
                time.sleep(0.002)
                continue
            now = time.perf_counter()
            delivered += 1
            cid = _extract(msg, correlation_key)
            with publish_lock:
                ts = publish_ts.get(cid)
            if ts is not None:
                latencies.append(now - ts)
        duration = time.perf_counter() - t0
        pub_thread.join(timeout=2.0)

    try:
        graph.stop()
    except Exception:
        pass

    throughput = delivered / duration if duration > 0 else 0.0
    notes = ""
    if delivered < n_messages:
        notes = (
            f"drain timed out: only {delivered}/{n_messages} delivered "
            f"within {drain_timeout_s}s"
        )
    if not latencies:
        notes = (notes + "; " if notes else "") + (
            f"no per-message latency captured (correlation_key "
            f"'{correlation_key}' not preserved through the DAG)"
        )

    return BenchmarkResult(
        name=name,
        n_messages=n_messages,
        delivered=delivered,
        duration_s=duration,
        throughput_msg_s=throughput,
        latency=LatencyStats.from_seconds(latencies),
        peak_rss_mb=mem.peak_rss_mb,
        notes=notes,
    )
