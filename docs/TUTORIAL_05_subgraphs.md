# Two Subgraphs & a Full Risk Platform

Build one DAG that exercises nearly the whole platform: three input
feeds merged with a fan-in, **two subgraphs** (validation and
risk analytics), three parallel analytics branches that fan back in, and
four heterogeneous sinks — all gated by a market-hours schedule and
controllable through a subgraph supervisor.

The earlier tutorials built up from a single node to a layered enterprise
pipeline. This one focuses on the platform's most powerful structural feature:
**subgraphs** — reusable, independently controllable sub-pipelines
that can be "lit up" and "lit down" at runtime while the parent DAG keeps
running. We will assemble a realistic trading *risk & surveillance*
platform that uses two of them.

> **What's a subgraph, really?** It's a whole assembly line that you
> wrap into a single box and then drop into a bigger line as if it were one
> station. Two reasons this is powerful: **reuse** — build a
> "risk analytics" box once and use it in many pipelines instead of copy-pasting
> the same ten stations; and **independent control** — you can
> switch one box off and on (the tutorial calls it "lit down / lit up") *without
> stopping the rest of the line*, like taking one machine offline for
> maintenance while the factory keeps running. If you've ever wished you could
> pause just one part of a system, that's what this gives you.

> **Ready-made:** the complete DAG ships at
>
> **config/example/dags/complex\_risk\_platform\_showcase.json**
>
> ,
> and its file-based subgraph at
>
> **config/subgraphs/risk\_analytics\_pipeline.json**
>
> .
> Copy the DAG into `config/dags/` (the subgraph file is already in place),
> restart or hit **Reload DAGs**, and open it in the
> **Graph View** to follow along. This tutorial explains how it is
> built, piece by piece.

## What we are building

```
                       ┌─ var_branch ───┐
trade_in ─┐                            │                │
market_in ─┼─► ingest_merge ─► [validation_subgraph] ─► [risk_subgraph] ─┼─ greeks_branch ─┼─► analytics_merge ─┬─► limit_node ─┬─► risk_out
refdata_in ┘     (fan-in)        (subgraph #1)            (subgraph #2)   └─ liquidity_br. ─┘     (fan-in)        │               └─► alert_node ─► alert_out
                                                                                                                 ├─► surveillance_node ─► surveillance_out
                                                                                                                 └─► audit_node ─► audit_out
```

That is 18 top-level nodes plus 8 more inside the two subgraphs — 26 in
all — with four input subscribers, four output publishers, and seventeen
calculators across seven calculator types. Let us build it in stages.

---

## New here? The idea in plain English

Two new ideas here, both about *structure*. The first is a **subgraph**: a small, self-contained assembly line that you can drop into a bigger one as if it were a single station. Just as a factory might have a pre-built sub-assembly (say, "engine assembly") that slots into the main line, a subgraph lets you package a few related steps once and reuse them — keeping a big pipeline tidy and its pieces reusable.

The second is a **fan-in**: several conveyor belts feeding *into* one station, so it can combine inputs from multiple sources (here, three separate feeds merged into one stream before processing). The opposite, one station feeding several belts out, is a "fan-out".

**In one sentence:** we merge three feeds into one (fan-in), then run the data through two reusable mini-pipelines (subgraphs) to build a full risk platform — without one giant tangled diagram.

---

## Step 1 — Three feeds, merged with a fan-in

A risk engine needs trades, live market data, and reference data. We declare
three subscribers and a fourth `fanin://` subscriber that merges them
into a single stream, then attach an `ApplyDefaultsCalculator` so every
record gets sane defaults.

```
"subscribers": [
  { "name": "trade_feed",          "config": { "source": "mem://queue/trades" } },
  { "name": "market_data_feed",    "config": { "source": "mem://queue/market_data" } },
  { "name": "reference_data_feed", "config": { "source": "mem://queue/reference_data" } },
  { "name": "merged_inputs",
    "config": { "source": "fanin://trade_feed,market_data_feed,reference_data_feed" } }
]
```

The three source nodes (`trade_in`, `market_in`,
`refdata_in`) each feed `ingest_merge`, which subscribes to the
merged stream. In the Graph View you will see three edges converge on one node
— the classic fan-in shape.

> **Why a fan-in node?** Each feed can arrive at its own rate from
> its own broker. The `fanin://` source interleaves them so downstream
> logic sees one ordered stream and does not need to know how many inputs there
> were.

## Step 2 — Subgraph #1 — inline validation & enrichment

Our first subgraph is defined **inline** (embedded directly in the
node). It validates, enriches, scrubs internal-only fields, and normalizes field
names — four internal nodes that "borrow" calculators from the parent.

```
{
  "name": "validation_subgraph",
  "type": "SubgraphNode",
  "config": {
    "entry_connection": "ingest_merge",
    "exit_connections": ["risk_subgraph"],
    "execution_mode": "synchronous",
    "control_source": "mem://queue/subgraph_control"
  },
  "subgraph": {
    "name": "validation_enrichment_pipeline",
    "entry_node": "sg_validate",
    "exit_node": "sg_normalize",
    "dark_output_mode": "passthrough",
    "error_policy": "propagate",
    "nodes": [
      { "name": "sg_validate",  "borrows": "validate_trade",  "role": "entry" },
      { "name": "sg_enrich",    "borrows": "enrich_market" },
      { "name": "sg_scrub",     "borrows": "drop_internal" },
      { "name": "sg_normalize", "borrows": "normalize_names", "role": "exit" }
    ],
    "edges": [
      { "from": "sg_validate", "to": "sg_enrich" },
      { "from": "sg_enrich",   "to": "sg_scrub" },
      { "from": "sg_scrub",    "to": "sg_normalize" }
    ]
  }
}
```

Key ideas:

|  |  |
| --- | --- |
| `entry_connection` / `exit_connections` | How the subgraph wires into the parent: input comes from `ingest_merge`; output flows to `risk_subgraph`. |
| `entry_node` / `exit_node` | The first and last *internal* nodes — the subgraph's own boundary. |
| `borrows` | Each internal node reuses a calculator defined once in the parent's `calculators` list — no duplication. |
| `dark_output_mode: passthrough` | When this subgraph is "lit down", it passes input straight through untouched (validation is optional under load). |

## Step 3 — Subgraph #2 — the risk analytics engine (file-based)

The second subgraph is the heavy compute core: scenario generation, pricing,
netting, and PFE exposure metrics. Unlike subgraph #1, this one is
**loaded from an external file** via `subgraph_file` —
the node in the parent DAG just points at it:

```
{
  "name": "risk_subgraph",
  "type": "SubgraphNode",
  "config": {
    "subgraph_file": "risk_analytics_pipeline.json",
    "entry_connection": "validation_subgraph",
    "exit_connections": ["var_branch", "greeks_branch", "liquidity_branch"],
    "execution_mode": "synchronous",
    "control_source": "mem://queue/subgraph_control"
  }
}
```

The referenced file lives at

**config/subgraphs/risk\_analytics\_pipeline.json**

(the loader searches `config/subgraphs/`, `subgraphs/`, then the
working directory, so a bare filename is enough). Its contents are a standalone
subgraph definition:

```
{
  "name": "risk_analytics_pipeline",
  "type": "subgraph",
  "version": "1.0.0",
  "entry_node": "sg_scenario",
  "exit_node": "sg_pfe",
  "dark_output_mode": "cached",
  "error_policy": "use_cached",
  "nodes": [
    { "name": "sg_scenario", "borrows": "scenario_gen",  "role": "entry" },
    { "name": "sg_price",    "borrows": "price_trade" },
    { "name": "sg_netting",  "borrows": "apply_netting" },
    { "name": "sg_pfe",      "borrows": "pfe_metrics",   "role": "exit" }
  ],
  "edges": [
    { "from": "sg_scenario", "to": "sg_price" },
    { "from": "sg_price",    "to": "sg_netting" },
    { "from": "sg_netting",  "to": "sg_pfe" }
  ]
}
```

> **Inline vs file-based.** Subgraph #1 is *embedded*
> (defined directly in the parent under a `"subgraph"` key) — handy
> when it is used once. Subgraph #2 is *file-based* (`subgraph_file`)
> — the same definition can then be reused by several parent DAGs and versioned
> on its own. The borrowed calculators (`scenario_gen`, etc.) are still
> defined once in the **parent** DAG's `calculators` list.

> **Two subgraphs, two dark-output strategies.** Validation uses
> `passthrough` (skip cleanly when down). Risk uses `cached`
> with `error_policy: use_cached` — if the expensive engine is lit
> down or errors, downstream consumers keep receiving the *last good*
> exposure numbers instead of nothing. This is exactly how you would protect a
> real risk feed during a market data outage.

## Step 4 — Parallel analytics branches, then fan back in

The risk subgraph fans out to three independent branches — Value-at-Risk,
option Greeks, and a liquidity score (the only non-null calculator, a
`RandomCalculator`, so you can watch numbers move). All three converge on
`analytics_merge`.

```
{ "from_node": "risk_subgraph", "to_node": "var_branch" },
{ "from_node": "risk_subgraph", "to_node": "greeks_branch" },
{ "from_node": "risk_subgraph", "to_node": "liquidity_branch" },

{ "from_node": "var_branch",       "to_node": "analytics_merge" },
{ "from_node": "greeks_branch",    "to_node": "analytics_merge" },
{ "from_node": "liquidity_branch", "to_node": "analytics_merge" }
```

This fan-out / fan-in around a subgraph is the platform's idiom for "run several
analyses on the same enriched data, then combine."

## Step 5 — Four sinks: results, alerts, surveillance, audit

After the merge, the data branches to four destinations. Note the pattern: a
`CalculationNode` does the work, and a separate
`PublisherSinkNode` publishes it — calculation nodes compute,
sink nodes publish.

```
{ "from_node": "analytics_merge", "to_node": "limit_node" },
{ "from_node": "analytics_merge", "to_node": "surveillance_node" },
{ "from_node": "analytics_merge", "to_node": "audit_node" },

{ "from_node": "limit_node",        "to_node": "alert_node" },
{ "from_node": "alert_node",        "to_node": "alert_out" },
{ "from_node": "surveillance_node", "to_node": "surveillance_out" },
{ "from_node": "audit_node",        "to_node": "audit_out" },
{ "from_node": "limit_node",        "to_node": "risk_out" }
```

The four publishers show off heterogeneous outputs: three in-memory topics/queues
(`risk_results`, `surveillance`, `alerts`) and one
**file sink** writing a JSON-lines audit log to disk.

> **Warning —**
>
> **Common gotcha.** A plain `CalculationNode` cannot
> publish — only sink nodes can. If you attach `publishers` to a
> calculation node the DAG will fail to build. Always end a publishing branch
> with a `PublisherSinkNode` (or `SinkNode`).

## Step 6 — Schedule & supervisor

The whole DAG runs only during US market hours, and both subgraphs share a
control stream so a supervisor can light them up or down without touching the
parent.

```
"schedule": {
  "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
  "holiday_calendars": ["USA"],
  "timezone": "America/New_York"
},
"subgraph_supervisor": {
  "enabled": true,
  "control_source": "mem://queue/subgraph_control"
}
```

Because the schedule timezone is `America/New_York`, the
`0930-1600` window means Eastern market hours no matter what timezone the
server runs in. On the dashboard, outside that window the DAG shows
**Outside Schedule** with the exact reason.

## Run it

1. Copy `config/example/dags/complex_risk_platform_showcase.json`
   into `config/dags/`.
2. Click **Reload DAGs** on the dashboard (or restart the server).
3. Open the DAG's **Details** page and explore the
   **Graph View**: scroll to zoom, drag to pan, and
   **click any node** to see its live state. The two subgraph nodes
   appear in purple.
4. Scroll to the **Subgraphs** panel to see each subgraph's state,
   execution count, latency, and internal nodes — and to light them up or
   down.

> **What this one DAG demonstrates:** multi-source ingestion,
> fan-in and fan-out, two independently-controlled subgraphs with different
> dark-output strategies, calculator borrowing, parallel compute branches,
> heterogeneous sinks (memory + file), market-aware scheduling, and supervisor
> control — most of the platform, in a single file.

> **Warning —**
>
> **Performance note.** A single DAG runs on one compute thread, so
> even with subgraphs the sweep is sequential. For CPU-heavy risk runs, assign the
> DAG to the [worker pool](/help/worker-pool), or use
> [AutoClone](/help/autoclone) to scale copies during
> peak hours.

## You have completed the tutorials

For depth on the features used here, see
[Subgraphs](/help/subgraph),
[Scheduling](/help/scheduling),
[AutoClone](/help/autoclone), and the per-broker setup
guides under [User Guides](/help/userguides).
