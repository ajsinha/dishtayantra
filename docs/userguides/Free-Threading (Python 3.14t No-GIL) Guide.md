# Free-Threading (Python 3.14t No-GIL) — Enablement and Testing Guide

## What this guide covers

Free-threading was introduced experimentally in Python 3.13 (PEP 703) and became
an officially **supported** build in Python 3.14 (PEP 779). A free-threaded build
runs with the Global Interpreter Lock (GIL) disabled, so Python threads execute on
multiple CPU cores at the same time. DishtaYantra targets the **Python 3.14
free-threaded build (`python3.14t`)**; the commands below use it. This guide
explains, specifically for DishtaYantra:

1. What free-threading changes for this system (and what it does not).
2. How to install a free-threaded interpreter and enable the no-GIL mode.
3. A dependency-by-dependency compatibility picture.
4. A complete, ordered testing plan - from a one-command check to stress tests.
5. Known risk areas in this codebase and how to exercise them.

> **Status:** free-threading support here is **experimental / opt-in**. The
> shipped configuration runs on a normal GIL build. Do not enable it in
> production until you have completed the testing plan below.

---

## 1. What free-threading changes for DishtaYantra

DishtaYantra is heavily threaded: each running DAG has a compute thread,
metronome nodes run timer threads, pub/sub subscribers run listener threads, the
schedule monitor, AutoClone manager, worker health monitor, and HA manager all
run background threads. Under the normal GIL build, only one of these executes
Python bytecode at any instant. Under free-threading, they can run **genuinely in
parallel** on separate cores.

**Where this helps:** CPU-bound work inside a single process - several DAGs (or
several threads of one DAG) computing at once without the worker-pool's separate
processes. It can also reduce latency jitter from threads contending for the GIL.

**What it does NOT change:**

- **The worker pool** (Tutorial 7) already achieves parallelism with separate
  **processes**, each with its own interpreter. Free-threading is orthogonal to
  it; you can use either or both. Worker processes are unaffected by the parent's
  GIL setting.
- **The JVM gateway pool** (Tutorial 8) runs work inside the JVM; Python's GIL was
  never the bottleneck there.
- **Correctness requirements.** The GIL used to make some sloppy patterns
  *accidentally* safe. Free-threading removes that safety net, so any latent data
  race is now a real race. DishtaYantra was written with explicit locking (its
  singletons use double-checked locking with `threading.Lock()`, and the
  in-memory queues build on the independently-locked `queue.Queue`), which is
  exactly what you want here - but third-party code is the variable.

---

## 2. Installing a free-threaded interpreter

Free-threading is a **separate build** of CPython. Installing the normal 3.14
build does **not** give you the no-GIL capability; you need the "t" (threaded)
variant, `python3.14t`.

### Option A - python.org installer
On the 3.14 installer, enable the **"free-threaded Python"** option. This
produces an interpreter typically invoked as `python3.14t`.

### Option B - pyenv
```bash
pyenv install 3.14t-dev        # or the current 3.14 free-threaded build name
pyenv shell 3.14t-dev
```

### Option C - uv / build flag
If building from source, configure with `--disable-gil`. With tooling like `uv`,
request the free-threaded variant of the interpreter explicitly.

### Confirm you actually have it
```bash
python3.14t -c "import sys; print(sys._is_gil_enabled())"
```
- If this prints `False`, the GIL is **off** (free-threading active).
- If it prints `True`, the interpreter *can* free-thread but the GIL is currently
  on (often because an imported C extension re-enabled it - see below).
- If it raises `AttributeError`, this is a **standard** build with no
  free-threading capability; install the "t" variant.

### Turning the GIL on/off at runtime
On a free-threaded build the GIL is off by default but can be forced on:
```bash
PYTHON_GIL=1 python3.14t ...     # force the GIL ON
python3.14t -X gil=0 ...         # explicitly request it OFF
```
Use `PYTHON_GIL=1` as an A/B baseline when benchmarking.

---

## 3. Dependency compatibility

The risk in free-threading is almost entirely **C/C++/Rust extension modules**.
A native extension must be explicitly built and tagged free-threading-ready
(wheels tagged `cp314t`); if it is not, importing it **silently re-enables the
GIL**, quietly defeating the whole exercise. Pure-Python packages are low risk.

DishtaYantra's dependencies, grouped by risk:

| Dependency | Kind | Risk | Notes |
|------------|------|------|-------|
| fastapi, jinja2, itsdangerous, python-multipart | pure-Python | Low | Fine. |
| kafka-python | pure-Python | Low | Fine. |
| stomp.py, py4j, kazoo | pure-Python | Low | py4j spawns a callback-server thread - load-test it. |
| prometheus-client, markdown, Pygments, PyYAML, requests | pure-Python | Low | PyYAML's optional C loader is the exception. |
| uvicorn[standard] | mixed | Medium | Pulls optional C speedups (httptools, uvloop, websockets). Test, or run with the pure-Python loop. |
| SQLAlchemy | mixed | Medium | Pure-Python core; the DB driver underneath is the real question. |
| msgpack | C + pure-Python | Low/Med | Has a pure-Python fallback if the C build is unavailable. |
| **confluent-kafka** | C (librdkafka) | **High** | Needs a free-threaded wheel; verify on PyPI. |
| **psycopg2-binary** | C | **High** | Postgres driver; verify wheel. |
| **aerospike** | C | **High** | Verify wheel. |
| **redis** | pure-Python, optional `hiredis` C parser | Medium | The `redis` package itself is pure-Python; `hiredis`, if installed, is C. |
| **pandas** | C/Cython | **High** | Verify wheel. |
| **pyarrow** | C++ | **High** | Verify wheel. |
| **polars** | Rust | Medium/High | Rust extensions have generally moved faster on free-threading; still verify. |
| **lmdb** | C | **High** | Used for cross-worker data exchange under concurrency - the one to scrutinize most. |
| **psutil** | C | Medium | Worker metrics; verify wheel. |
| Optional cloud (boto3, azure-*, google-cloud-storage) | mostly pure-Python | Low/Med | Only relevant if you use those providers. |

> **Do not assume.** Wheel availability and free-threading readiness change
> release to release. The specific versions pinned in `requirements.txt` may or
> may not yet ship `cp314t` wheels. **Verify each one against PyPI** rather than
> trusting this table - which is exactly what the check script in the next
> section does for you.

---

## 4. The testing plan

Work through these **in order**. Do not skip ahead; each stage gates the next.

### Stage 0 - Automated compatibility check (5 minutes)

A script ships at `scripts/check_free_threading.py`. It imports every dependency
one at a time and reports which ones re-enable the GIL.

```bash
python3.14t scripts/check_free_threading.py
```

Read the verdict:
- **PASS** - the GIL stayed disabled after importing everything available.
  Proceed to Stage 1.
- **FAIL / RE-ENABLED GIL** - one or more native deps lack a free-threaded wheel.
  The summary lists them. Until those are upgraded (or removed if you do not use
  them), free-threading is effectively off. Decide per dependency: upgrade, drop,
  or wait.

> The script also runs on a normal interpreter; it will tell you it cannot detect
> GIL state but will still flag import failures.

### Stage 1 - Install into a free-threaded environment

```bash
python3.14t -m venv .venv-ft
source .venv-ft/bin/activate
pip install -r requirements.txt
```
Watch for any package that **builds from source** instead of installing a wheel -
that often means no `cp314t` wheel exists yet, and the result may re-enable the
GIL or fail at runtime. Re-run Stage 0 inside this venv.

### Stage 2 - Unit + integration test suite (single-threaded correctness)

Run the full suite on the free-threaded interpreter:
```bash
SECRET_KEY=test python3.14t -m pytest tests/ -q
```
Then run it **with the GIL forced on** as a baseline and compare:
```bash
SECRET_KEY=test PYTHON_GIL=1 python3.14t -m pytest tests/ -q
```
Both should show the same pass count. A test that passes with the GIL on but
fails with it off is pointing at a real race - investigate before going further.

Also watch the process for a `RuntimeWarning` that the GIL was re-enabled by an
extension; pytest will surface it.

### Stage 3 - Concurrency / race hunting

The suite checks logic but not necessarily heavy concurrency. Exercise the
threaded subsystems deliberately, ideally with many DAGs running at once:

- **In-memory pub/sub** - the most-shared mutable singleton. Run the
  coordination example (Tutorial 6) with the producer and consumer started
  together, and several copies of each, for an extended period. Verify no lost or
  duplicated messages and no exceptions in the logs.
- **Concurrent DAG start/stop** - start and stop many DAGs rapidly from the
  dashboard or API while others run, to stress the server's shared registries.
- **Schedule + AutoClone churn** - enable AutoClone on a DAG with a short
  ramp window so clones are created and torn down repeatedly while other DAGs run.
- **Run under a thread sanitizer mindset** - if you can, run a long soak
  (hours) under load and watch for sporadic, non-reproducible errors, which are
  the signature of a data race.

Repeat the key scenarios with `PYTHON_GIL=1` to confirm they only misbehave (if
at all) with the GIL off.

### Stage 4 - Subsystem-specific tests for the high-risk natives

Only test what you actually use:

- **LMDB cross-worker path** - if you use the worker pool with `lmdb://`
  queues (Tutorial 7), run that pipeline under load. LMDB's binding semantics
  under no-GIL deserve direct exercise. *(Note: the worker pool's children are
  separate processes, but the parent and any in-process LMDB access still run on
  the free-threaded interpreter.)*
- **Database layer** - log in, create users/roles/API keys, and hammer
  authenticated endpoints concurrently to exercise SQLAlchemy + the DB driver.
- **Kafka / Redis / Aerospike / SQL** - for each broker you actually use, run
  a DAG that publishes and subscribes under sustained throughput.
- **JVM gateway pool** - if you use Java calculators (Tutorial 8), run the
  JVM pipeline under concurrency; py4j's callback threads now run truly in
  parallel.
- **Prometheus + psutil metrics** - leave metrics collection on during all of
  the above; `psutil` is a C extension sampled from a background thread.

### Stage 5 - Performance comparison (is it worth it?)

Free-threading has per-operation overhead; the win comes only from real
parallelism. Benchmark a representative, CPU-bound, multi-DAG workload three
ways and compare throughput and tail latency:

1. Normal GIL build.
2. Free-threaded build with `PYTHON_GIL=1` (GIL on).
3. Free-threaded build with the GIL off.

If (3) is not meaningfully faster than (1) for your workload, the worker pool
(separate processes) may be the better route to parallelism for you.

---

## 5. Known risk areas in this codebase

These are the specific places to watch; the codebase handles them with explicit
locks today, but they are where a regression would show up first:

- **Process-wide singletons** - `InMemoryPubSub`, the storage provider,
  `UserRegistry`, the JVM/Rust managers, and the LMDB manager are shared across
  all threads. They use double-checked locking on construction; the thing to
  verify is that *mutation* of their internal state under concurrent access stays
  correct without the GIL.
- **In-memory queues** - built on `queue.Queue` (independently locked), but the
  surrounding bookkeeping (stats counters, queue registries) is the area to
  stress.
- **Shared counters and stats** - throughout pub/sub and node execution there
  are counters (`packaged_count`, calculation counts, queue depths). Plain `int`
  increments are not atomic without the GIL; under heavy parallelism these can
  drift. They are diagnostics, not control flow, so drift is cosmetic - but worth
  knowing when a count looks slightly off.
- **Third-party C extensions** - covered above; the single biggest risk.

---

## 6. Decision checklist

Enable free-threading in production only when **all** of these hold:

- [ ] `scripts/check_free_threading.py` reports **PASS** with every dependency
      you use installed.
- [ ] The full test suite passes on the free-threaded build, matching the
      GIL-on baseline pass count.
- [ ] No `RuntimeWarning` about the GIL being re-enabled appears at startup.
- [ ] A multi-hour soak test under realistic load shows no sporadic errors.
- [ ] Each high-risk native dependency you use (LMDB, your DB driver, your
      broker client, pandas/pyarrow/polars) has been exercised under concurrency.
- [ ] Benchmarks show a real, worthwhile speed-up over the GIL build for your
      workload.

If any box is unchecked, stay on the standard build (and consider the worker
pool for parallelism instead). Free-threading is a powerful option, but it earns
its place only after this verification - not on optimism.

---

## Summary

DishtaYantra's own code is written for thread-safety (explicit locks, guarded
singletons, queue-based pub/sub), which positions it well for free-threading.
The real determinant is the **native dependency wheels**: until each C/Rust
extension you use ships a free-threaded (`cp314t`) build, importing it re-enables
the GIL and silently defeats the benefit. Use `scripts/check_free_threading.py`
to get a concrete go/no-go list, then work through the staged testing plan before
trusting it in production.
