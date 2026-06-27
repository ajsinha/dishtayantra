# Flow-log Replay & Log-based Recovery (design)

Status: **design + first slice landed** (foundation module `core/replay.py`). Engine
integration is scoped below as subsequent slices.

## 1. Why

DishtaYantra already records a compact **change-log** of node fires (Flow
Time-Travel): because the equality gate only fires on real output changes, the log
is a faithful, deduplicated record of *what each node produced and when*. Two
high-leverage capabilities fall out of that log almost for free, and both reinforce
the existing architecture instead of fighting it:

1. **Replay** — re-run a recorded window through the engine. Three uses:
   - *Time-travel debugging*: reproduce a past incident deterministically.
   - *Regression testing*: replay real historical inputs through a **new** calculator
     version and diff the outputs against what was recorded ("record in prod, replay
     in CI").
   - *What-if*: run an alternate DAG/config against real past inputs.
2. **Log-based recovery** — on startup, seed each node's last-known output from the
   log so the graph resumes warm instead of cold, *without* standing up the heavy
   checkpoint/disaggregated-state machinery (Roadmap B2). A frugal on-ramp to
   recovery that needs no extra infrastructure.

Neither requires a cluster, a broker, or an external store — they use the flow DB
that is already on by default.

## 2. The engine seam (grounded in the code)

A node computes in `Node.compute()` (`core/dag/graph_elements.py`):
`consolidate_edge_inputs()` gathers the outputs of its incoming edges → input
transformers → **input equality gate** → `calculator.calculate(input)` → output
transformers → **output equality gate** → on change, mark children dirty and
`FLOW_RECORDER.record_node_fire(self)`. Dataflow propagates **node outputs** along
edges; a node's input is just the merge of its upstreams' outputs.

That gives replay a clean injection point: **set a source/boundary node's output to a
recorded value, mark its children dirty, and let `notify_work()` drive the normal
compute loop.** Downstream nodes recompute through their real calculators and their
real equality gates — so replay exercises actual logic, not a recording.

The recorded row (`config/schema/flow_events_*.sql`) carries everything needed:
`dag_id, node_id, seq, ts_ms, inputs_json, output_json, targets_json, compute_us,
instance, host, port`. The store already exposes the reads we need:
`query(dag_id, t0, t1, nodes, …)`, `iter_export(...)` (streaming, memory-bounded),
and `state_at(dag_id, ts)` (latest output per node ≤ ts — exactly the recovery seed).

## 3. Feature A — Replay (offline / sandbox first)

**Default mode is offline and side-effect-free.** Build a *fresh* `ComputeGraph`
from the target config in a sandbox, with egress/publish nodes replaced by **sinks**
(no external I/O). Drive it by replaying the recorded **boundary** events (source /
connector node outputs) in `(ts_ms, seq)` order; let the engine recompute the
interior. Two run modes:

- **as-fast-as-possible** (CI / regression): ignore wall-clock gaps, just preserve
  order.
- **paced**: honour inter-event `ts_ms` deltas (optionally scaled) for realistic
  time-travel debugging.

**Outputs of a replay run:** the recomputed per-node output timeline, plus an
optional **diff** against the recorded outputs (the regression signal). A node whose
recomputed output diverges from the recorded one is the headline result.

## 4. Feature B — Log-based recovery (seed, don't re-fire)

On startup, for each managed DAG, call `state_at(dag_id, now)` to get the last
recorded output per node, and **seed** `node._output` (and `_input`) from it, marking
nodes clean. The graph then resumes from last-known state; new inputs after boot
drive normal compute. We **seed, we do not re-fire** — re-firing could duplicate
side effects. Config-gated and off by default (`flow_recorder.recover_on_start`).

## 5. Consistency & correctness questions (the hard parts — surface, don't hide)

- **Determinism.** Faithful replay requires calculators to be pure functions of their
  input. Calculators that read wall-clock, randomness, or external services will not
  reproduce. We document this and provide a `replay_clock` injection point so
  time-dependent calculators can be made deterministic; non-deterministic ones are
  flagged, not silently trusted.
- **Side effects in replay.** Egress/publish must be stubbed in offline mode (sinks),
  or replay would re-publish historical data to live destinations. This is a
  **safety invariant**, not an option.
- **Payload fidelity vs. the capture cap.** The recorder caps payloads at
  `max_payload_bytes` (default 2048) and truncates larger values. A truncated payload
  **cannot be faithfully replayed.** Therefore replay-critical DAGs need a
  *full-fidelity capture* mode (raise/disable the cap, or store full payloads). Replay
  must detect `__truncated__` markers and refuse (or warn) rather than produce a
  wrong result. This is the single most important honest caveat.
- **Boundary identification.** Replay needs to know which nodes are *sources*
  (injected) vs *interior* (recomputed). Derived from the DAG topology (nodes with no
  incoming edges, or explicitly tagged connector nodes).
- **Multi-instance logs.** With a shared flow DB, events carry `(instance, host,
  port)`; replay/recovery filter by instance so one node doesn't replay another's
  stream unless explicitly asked.
- **Nodes that never fired.** No recorded output → seed nothing (same as cold). Safe.
- **Recovery vs. in-flight.** Recovery seeds committed *outputs*; it does not recover
  in-flight queue contents. That is B2's job. We are explicit that this is
  *warm-start*, not exactly-once recovery.

## 6. Slices (incremental, each shippable)

1. **Foundation (landed): `core/replay.py`** — read-only over the flow store. A
   `ReplaySource` that streams a recorded window as ordered events, and a
   `reconstruct_output_timeline()` / `recovery_seed()` that turns the log into the
   per-node latest-output map. Fully offline, fully tested. No engine changes.
2. **Offline replay runner** — build a sandbox `ComputeGraph` from config, stub
   egress, inject boundary events from slice 1, capture recomputed outputs.
3. **Regression diff + CLI/API** — `dyflow replay <dag> <window> [--against config]`
   producing a per-node diff; `POST /api/flow/{dag}/replay`.
4. **Log-based recovery at boot** — config-gated `recover_on_start` seeding via slice
   1's `recovery_seed()`.
5. **Full-fidelity capture mode** — per-DAG opt-in to uncapped payloads for
   replay-critical pipelines, with size guards.

## 7. Frugality / non-goals

- No new infrastructure: replay and recovery use the existing flow DB.
- Not a substitute for B2 (durable keyed state, exactly-once, fast rescale). This is
  the *cheap* recovery on-ramp and a *unique* debugging/testing tool, not the
  cluster-grade guarantee.
- Offline-by-default; any mode that could cause external side effects is opt-in and
  loudly gated.
