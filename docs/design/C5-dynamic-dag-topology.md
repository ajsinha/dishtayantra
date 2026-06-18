# C5 — Dynamic / Elastic DAG Topology — Design

Status: **proposed (design).** Roadmap item C5 (Phase 3). This document sketches
how a running DishtaYantra DAG can **grow and shrink at runtime** — add, modify,
or remove nodes / calculators / edges — and, on top of that, how a **templatized
section** of a DAG can **expand in reaction to the data that arrives** (e.g. a new
symbol, tenant, or session key spins up its own sub-pipeline, and idle ones are
torn down). It is grounded in the current engine and is strictly additive: a DAG
that never uses these features behaves exactly as it does today.

---

## 1. The governing constraint (grounded in the code)

Everything here is shaped by one fact about the current engine. In
`core/dag/compute_graph.py`, the compute loop computes its execution order **once**:

```
def do_compute(self):
    sorted_nodes = self.topological_sort()        # line 431 — ONCE, above the loop
    while not self._stop_event.is_set():
        ...
        for node in sorted_nodes: node.pre_compute()
        for node in sorted_nodes:
            if node.isdirty(): node.compute()
        ...
```

Two properties follow, and both are assets, not obstacles:

1. **One compute thread per DAG.** All node execution for a DAG is serialized onto
   its compute thread. Nothing else mutates node state during a sweep.
2. **The order is frozen for the life of the loop.** `sorted_nodes` is a local
   captured before the first sweep; the loop never re-sorts.

So the design problem is **not** "how do I lock the graph for concurrent edits."
It is: *how do I change structure such that no sweep ever observes a half-applied
change, and the frozen order is refreshed exactly when (and only when) structure
changes.* The single-threaded compute loop makes the safe answer simple.

The other non-negotiable is the **equality gate**: a node recomputes only when its
input actually changes (`isdirty()`). Any structural change must leave that
invariant intact — new nodes must start dirty, rebinds must force one recompute,
removals must not strand dirty dependents.

---

## 2. Core mechanism: structural mutations as queued commands

Other threads (subscriber listeners noticing a new key, an admin API call, a
metronome) **never** touch `self.nodes` or the edge set directly. They submit a
**mutation command** to a per-DAG mailbox and call the existing `notify_work()`:

- `AddNode`, `RemoveNode`
- `AddEdge`, `RemoveEdge`
- `RebindCalculator` (swap the calculator instance/config on an existing node)
- `InstantiateTemplate(key)`, `RetireTemplate(key)` (§4)

The compute loop drains this mailbox at the **one quiescent point** — at the top
of the loop, right after `self._work_available.clear()` and **before**
`pre_compute`. A mutation is therefore just another kind of "work" that wakes the
loop; it slots into the event-driven model already in place.

Each command is applied **transactionally**:

1. **Validate** the command in isolation (referenced nodes exist, calculator
   resolves, config parses — fail loud, never default silently).
2. **Stage** the change against a copy of the adjacency structure.
3. **Check invariants on the staged topology** — crucially, run the existing
   cycle detection in `GraphAlgorithmsMixin` (`compute_graph_support.py`) so an
   expansion can never introduce a cycle.
4. **Commit** atomically (swap in the new structure) and **bump the topology
   generation** (§3); or **reject** the whole command with a clear error and leave
   the running graph untouched.

Because commit happens on the compute thread between sweeps, the next sweep always
runs against a fully-consistent topology.

---

## 3. Generation-stamped topology (refresh the order only on change)

Replace "sort once, never again" with "sort once **per generation**." Hold a
`topology_generation` counter and cache `sorted_nodes` keyed by it. The sweep loop
checks the generation at the top; if a committed mutation bumped it, it recomputes
`sorted_nodes` (and any cached per-node dirty wiring), otherwise it reuses the
cache. Steady state keeps today's zero-overhead behaviour; the O(V+E) sort is paid
only when structure actually changed, which is rare relative to sweeps.

`subgraph.py` already follows this shape — it computes `_topological_order` once
(line 201) and reuses it; the generalization is to *invalidate on mutation* rather
than *never*. Incremental re-ordering of only the affected region is a later
optimization; full re-sort on change is the correct, simple v1.

---

## 4. Templatized, data-driven expansion

This is the headline capability: a section of the DAG that **materializes per data
key** and disappears when no longer needed.

### 4.1 Stable outer boundary (the key to not churning the whole graph)

A template section exposes **fixed ports**: a **dispatcher** node at the entrance
and a **collector** node at the exit. The surrounding static graph wires only to
the dispatcher and the collector and never changes. All expansion/contraction
happens **inside** that envelope:

```
        static upstream ──▶ [DISPATCHER] ──▶ (instance for key A) ──┐
                                          ├─▶ (instance for key B) ──┤─▶ [COLLECTOR] ──▶ static downstream
                                          └─▶ (instance for key …) ──┘
```

Consequences: the *outer* topological order is unaffected by instance lifecycle,
so the generation bump and re-sort (§3) are confined to the bounded interior.
Downstream wiring is permanent, so consumers never see the structure move.

### 4.2 Template = parameterized subgraph spec

A `DagTemplate` is a DAG-config fragment (same JSON idiom as existing DAG configs,
so it is serializable and renderable in the DAG Designer) with placeholders bound
at instantiation:

- node / calculator / edge definitions using `{key}` (and other params),
- a **trigger**: how to detect that expansion is needed and how to derive the key
  from an incoming datum (e.g. `key = record["symbol"]`),
- a **lifecycle policy** (§4.4).

### 4.3 ExpansionManager + dispatcher

The **dispatcher** extracts the key from each datum and asks an **ExpansionManager**
for the instance:

- **new key** → manager emits an `InstantiateTemplate(key)` mutation command
  (§2), which builds the per-key instance behind the boundary and registers
  `key → instance` (and its input port). Until commit, the datum can be buffered or
  routed on the next sweep.
- **known key** → route directly to the existing instance's input port.

The manager owns the `key → instance` registry and is the single place lifecycle
is enforced.

### 4.4 Lifecycle: grow *and* shrink

Shrink is a first-class concern, not an afterthought:

- **Idle TTL** — retire an instance whose input has been quiet for *T*.
- **LRU / capacity cap** — hold at most *N* live instances; evict least-recently-used.
- **Explicit** — admin/API retire, or a control message on the stream.

Retirement issues a `RetireTemplate(key)` command that **drains** the instance
(let in-flight data finish), removes its nodes/edges, releases its pub/sub queues,
and bumps the generation. This is finer-grained AutoClone: AutoClone ramps whole
DAGs up/down; this ramps a *section* per key.

---

## 5. Two embedding strategies

The interior instances can live two ways; the boundary contract (§4.1) is the same
for both, so a template can switch strategy without changing the outer graph.

**A. In-graph instances** — instances are real nodes/edges inside the parent DAG,
executed by the parent's single compute thread. *Pros:* tight, strong within-sweep
consistency, one scheduler. *Cons:* every instantiate/retire bumps the parent
generation and re-sorts the (bounded) interior; many keys = a large single graph.

**B. Pub/sub-mediated subgraphs (recommended first)** — the dispatcher publishes to
a **per-key topic**; each instance is its own small `ComputeGraph` (its own compute
thread) subscribed to that topic, and publishes to a shared collector topic. The
in-memory broker already supports this dynamically (`create_queue`,
`subscribe_to_topic`, `publish_to_topic`). *Pros:* the **parent's `sorted_nodes` is
never touched** — expansion is "create a topic + start a subgraph," which sidesteps
the topo/equality invariants of the parent entirely; natural isolation per key;
trivially parallel under the future free-threaded build (A3). *Cons:* more threads;
cross-instance consistency is eventual rather than per-sweep; teardown must drain
the topic.

Recommendation: ship strategy **B** first (lowest risk to the core engine), then
offer **A** for cases that need tight, single-sweep coupling.

---

## 6. Modifying (not just adding): hot calculator rebind

`RebindCalculator` swaps a node's calculator instance/config between sweeps. It must:
validate and construct the new calculator **before** the swap (reject on failure,
leave the old one running); preserve or explicitly reset any calculator-held state;
and **mark the node dirty** so the equality gate forces exactly one recompute with
the new logic. No partial state, no silent fallback.

---

## 7. Equality gate & invariants — preserved, not bypassed

- **New nodes start dirty** and are wired into dependency propagation before the
  first sweep that sees them, so they compute once and then obey the gate.
- **Rebinds force one recompute** (§6).
- **Removals** detach cleanly: dependents are re-evaluated against the new input
  set; no dangling dirty references remain.
- **Cycle freedom** is checked on the staged topology before commit (§2).
- **Fail loud:** a malformed template or command is rejected with a clear error;
  the running graph is never left half-modified. Consistent with the project rule
  of no silent config defaulting and no swallowed exceptions.

---

## 8. HA & determinism (the standby must converge)

DishtaYantra runs primary/standby HA, so dynamic structure must replicate. Two
complementary guarantees:

1. **Deterministic-from-data expansion.** If a template's structure is a pure
   function of the observed key stream (same keys seen → same instances), a standby
   that consumes the same stream reconstructs the same topology naturally.
2. **Mutation event log.** Record every committed structural command as an ordered,
   replayable event. The standby applies the log to converge; the log also gives
   auditability and time-travel debugging ("how did this graph come to look like
   this?"). This dovetails with B2 (durable state) when that lands.

Strategy **B** (per-key subgraphs behind topics) makes (1) especially clean: the
structure is just "one subgraph per distinct key seen."

---

## 9. Backward compatibility

Entirely additive and opt-in:

- A DAG with no templates and no mutation commands runs the existing code path; the
  generation counter simply never increments, so `sorted_nodes` is computed once
  exactly as today.
- The mutation mailbox is empty for static DAGs — no per-sweep cost beyond an
  is-empty check.
- Templates are a new optional section type in the DAG config; existing configs are
  unchanged and keep loading.

---

## 10. Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Re-sort churn under high key cardinality | Stable boundary (§4.1) confines re-sort to the interior; prefer strategy B (no parent re-sort); LRU cap on live instances. |
| Thread explosion (strategy B) | Capacity cap + idle TTL; pool/coalesce low-traffic keys into a shared instance. |
| Race between data arrival and instantiation commit | Buffer or replay the triggering datum; instantiation is idempotent (re-issuing for an existing key is a no-op). |
| Cycle introduced by a template | Cycle check on staged topology before commit (§2). |
| Standby divergence | Deterministic-from-data design + replayable mutation log (§8). |
| Stateful calculator rebind losing state | Explicit state migrate/reset contract on `RebindCalculator` (§6). |

---

## 11. Phased delivery (maps to roadmap C5)

1. **Mutation command bus + between-sweeps apply + generation-stamped re-sort.**
   General, safe runtime add/remove/modify with transactional validation and cycle
   checks. (Foundational; unblocks everything else.)
2. **Stable template boundary** (dispatcher + collector ports; fixed outer wiring).
3. **Data-driven keyed expansion** via ExpansionManager + lifecycle (idle TTL, LRU
   cap, explicit retire) using **strategy B** (per-key subgraphs behind topics).
4. **Hot calculator rebind** and in-graph instances (**strategy A**) for
   tight-coupling cases.
5. **HA convergence:** mutation event log + standby replay; Designer support for
   collapsed "expandable" template sections showing live instance counts.

---

## 12. Open questions

- Per-key **ordering / fairness** when many instances share the collector — round
  robin, priority, or watermark-aligned (ties into B1 event-time)?
- **State scope** for instances: purely per-key, or shared read-only reference data
  injected at instantiation?
- **Observability**: how live instance counts, per-key throughput, and
  instantiation/retire events surface in metrics and the dashboard (extends the
  backpressure/observability work).
- Where to draw the line between this and **AutoClone** — unify them under one
  "elastic section" abstraction, or keep DAG-level vs section-level distinct?
