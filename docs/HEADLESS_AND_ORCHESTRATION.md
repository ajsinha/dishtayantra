# Headless Execution & Orchestration

Two related operational modes that let DishtaYantra run batch/ETL work without the
web UI, and let a long-running instance orchestrate ephemeral ones.

Runnable: `python -m core.dag.headless_runner --help` and
`python -m perftest.run_orchestration_example`.

---

## 1. Headless run-once (the worker)

The compute engine has never required the web UI — the UI is a monitor/designer,
and the engine (`ComputeGraph` / `DAGComputeServer`) has its own lifecycle. The
headless runner packages "start a DAG, process a bounded feed to completion, then
exit" into a first-class CLI:

```bash
python -m core.dag.headless_runner --config job.json \
    [--replay feed.jsonl] [--expect N] [--summary out.json] [--job-id ID]
```

What it does: starts the DAG (no FastAPI), optionally replays a bounded feed onto
the input queue, waits for **completion**, drains, stops cleanly, writes a summary
JSON, and exits `0` (success) or `1` (incomplete/error) so a scheduler can react.

**Completion detection** is explicit, not a timer:
- *Count-based* — complete when the sink's published count reaches the expected
  count (from `--expect`, or the number of replayed records).
- *Quiescence* — when no node is dirty and the input queues are empty for several
  consecutive polls (used when the expected count is unknown).

**Zero-loss on shutdown:** the runner waits for queues to drain and nodes to go
idle before stopping, honouring the engine's no-message-loss invariant.

A bounded batch file is just a finite stream (see `docs/BATCH_FILE_PROCESSING.md`),
so this is the natural way to run an EOD job: replay the feed, process, exit.

## 2. Orchestration — control plane + headless workers

The richer pattern: a long-running **control-plane** instance (with the UI, for
observability) reacts to events and, based on logic, launches ephemeral
**headless workers** to do heavy run-to-completion ETL. This separates an always-on,
lightweight decision plane from isolated, disposable compute — a crashing or
memory-hungry job can't take down the control plane, and finished workers simply
exit and free their resources.

A `JobDispatchCalculator` node turns qualifying events into worker processes. It is
the deliberate side-effecting node in an otherwise functional graph, so it is
built to be:

- **Idempotent** — exactly one launch per unique job key, even on recompute or a
  replayed event (a dedup set). The equality gate suppresses recompute on
  unchanged input; the dedup set guarantees exactly-once regardless.
- **Asynchronous** — it launches and returns immediately; a daemon thread waits on
  each child. It never blocks the reactive compute loop.
- **Bounded** — a `max_concurrent` cap with a pending queue (a real worker pool),
  so a burst of events cannot fork-bomb the host.
- **Observable** — on a child's exit it records the child's summary and (optionally)
  publishes a completion event to a queue the parent subscribes to, so the parent
  reacts to `job-done`/`job-failed` like any other event. It also exposes
  `shutdown()` to terminate live children so the control plane never orphans them.

Example DAG calculator entry:

```json
{
  "name": "dispatch",
  "type": "core.dag.job_dispatch.JobDispatchCalculator",
  "config": {"max_concurrent": 4, "summary_dir": "/var/dy/jobs", "done_queue": "job_done"}
}
```

Each trigger event carries `job_id` (dedup key), `config` (the worker DAG), and
optionally `replay`/`expect`. The dispatcher launches
`python -m core.dag.headless_runner --config <config> --job-id <id> --summary ...`,
and on exit reads the worker's summary.

**The reactive loop** closes when the worker's completion is published to a queue
the parent's DAG subscribes to. In production this is a shared broker
(Kafka/MQ/Redis) so parent and worker (separate processes) communicate; the
in-memory `done_queue` demonstrates the same contract in one process (the
cross-process artifact is the worker's summary file, which the parent reads on
child exit).

## 3. Choosing the shape

- **External scheduler launches a run-once worker.** Your enterprise scheduler
  (Autosys/Control-M/Airflow) watches for the feed, then runs the headless worker;
  it processes and exits. Leanest, most standard, nothing polls while idle.
- **In-DAG sensing + dispatch.** A control-plane DAG senses readiness (a metronome
  polling a manifest/`.done`/control total) and dispatches workers itself.
  Self-contained; good when no external scheduler manages arrival.
- **Process isolation vs in-process parallelism.** Reach for separate headless
  processes when you want isolation, ephemerality, and run-to-completion. If you
  only want throughput, the in-process worker pool / `DAGAutoCloneMixin` is the
  lighter answer.

Prefer an **explicit completeness signal** (manifest, trailer record, control
count) over time-based heuristics — that is where batch pipelines silently
process half a file.

## 4. Backward compatibility

All additive: `core/dag/headless_runner.py` and `core/dag/job_dispatch.py` are new
modules, `JobDispatchCalculator` is a new calculator selected only by a DAG that
references it, and the web app / `DAGComputeServer` paths are untouched. Existing
DAGs and the UI behave exactly as before.

## 5. Files

- `core/dag/headless_runner.py` — the run-once worker + CLI.
- `core/dag/job_dispatch.py` — the idempotent, async, bounded dispatcher.
- `perftest/run_orchestration_example.py` — end-to-end demo: a control-plane DAG
  dispatches headless workers (exactly-once, capped) that each process an EOD feed
  and exit, and the parent reacts to their completion.

Related: `docs/BATCH_FILE_PROCESSING.md`, `docs/ARCHITECTURE.md`, `docs/ROADMAP.md`.
