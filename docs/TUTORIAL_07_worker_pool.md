# Running DAGs Across Multiple Workers

By default every DAG runs inside the one main server process. The
**worker pool** spreads your DAGs across several separate
processes so they run on different CPU cores at the same time — real
parallelism for heavy workloads. This tutorial explains the idea in plain
terms, gives you ready-made files, and walks through the UI screen by screen.

## The idea, in plain language

Think of the server as an office. Out of the box there is **one
worker** (the main process) doing every task on the to-do list one at a
time. If one task is slow, everything behind it waits.

The **worker pool** hires more staff — several
*worker processes*, each running on its own CPU core. Now several DAGs
genuinely run at the same moment. You can also decide *who does what*:
let the system assign work automatically, or pin a specific DAG to a specific
worker, or give a demanding DAG a worker all to itself.

> **Why separate processes and not just threads?** Python runs
> threads in one process under a single lock, so CPU-heavy work does not truly
> run in parallel with threads. Separate *processes* each get their own
> interpreter and core, which is what gives you real parallel speed-up.

> **Warning —**
>
> **One consequence to remember.** Separate processes do not share
> memory, so DAGs on *different workers* cannot talk over an in-memory
> queue (`mem://`). To pass data between workers, use an
> `lmdb://` queue (a fast memory-mapped file the pool sets up for you)
> or a real broker like Kafka/RabbitMQ. DAGs on the *same* worker can still
> use `mem://`.

---

## Step 1 — Turn the worker pool on

The pool is configured by one file:

**config/worker\_config.json**

.
It ships **disabled** so the platform runs single-process out of the
box. A ready-to-use example is provided at

**config/example/worker\_config.example.json**

.

1. Back up the existing file:
   `cp config/worker_config.json config/worker_config.json.bak`
2. Copy the example over it:
   `cp config/example/worker_config.example.json config/worker_config.json`
3. Open it and confirm the top two settings:

```
"worker_pool": {
  "enabled": true,        // turn the pool ON
  "num_workers": 4,       // how many worker processes (or "auto" = CPU count)
  "min_workers": 2,
  "max_workers": 32,
  ...
}
```

That is the only required change. `num_workers` is the number of staff
you are hiring — start with the number of physical CPU cores on the machine.

> **Warning —**
>
> **The pool starts at server startup.** Because worker processes are
> spawned when the server boots, you must **restart the server** after
> enabling the pool — the "Reload DAGs" button does not start the pool. Also make
> sure the `lmdb` package is installed (`pip install -r requirements.txt`),
> since cross-worker queues use it.

## Step 2 — Choose how each DAG is placed

You do not have to decide placement for every DAG. Most should simply let the
pool choose. But when you need control, add a `worker_affinity` block to a
DAG's JSON. There are three styles — pick one:

| Placement style | What it does | When to use it |
| --- | --- | --- |
| **Automatic** *(no affinity block)* | The pool places the DAG using the default strategy (`weight_based`): it estimates each DAG's CPU cost and spreads load evenly. | The common case. Let the system balance things for you. |
| **Pinned** `"pinned_worker": 0` | Always runs on that exact worker number. | Predictable placement, or a library that must stay on one process. |
| **Exclusive** `"exclusive": true` | Gets a dedicated worker with no other DAGs sharing it. | A hot, heavy, latency-sensitive DAG that should not compete for CPU. |
| **Preferred** `"preferred_workers": [1,2]` | Tries those workers first, but may use others if needed. | A soft hint rather than a hard rule. |

Two more optional fields help automatic placement make good decisions:

- `"priority": 8` — higher-priority DAGs (1–10) are placed first.
- `"dag_cpu_weight": 0.8` — how expensive you expect this DAG to be;
  heavier DAGs are spread apart.

## Step 3 — The ready-made example: three DAGs, three styles

Three example DAGs ship under

**config/example/dags/worker\_pool/**

, designed to be run
together so you can watch them land on different workers:

```
  wp_market_ingest   (PINNED to worker 0)   ──lmdb://queue/market_ticks──►  wp_risk_engine
  wp_risk_engine     (EXCLUSIVE worker)      ──lmdb://queue/risk_results──►  wp_reporting
  wp_reporting       (AUTOMATIC placement)   ──► file:///tmp/log/dagserver/wp_report.jsonl
```

They form a small cross-worker pipeline: a pinned ingestion DAG feeds an
exclusive heavy risk DAG, which feeds an automatically-placed reporting DAG. Note
the `lmdb://` queues — that is what lets data cross from one worker
process to another.

The pinned DAG, for example, looks like this:

```
{
  "name": "wp_market_ingest",
  "priority": 8,
  "dag_cpu_weight": 0.2,
  "worker_affinity": {
    "pinned_worker": 0,
    "exclusive": false,
    "preferred_workers": []
  },
  "publishers": [
    { "name": "ticks_out", "config": { "destination": "lmdb://queue/market_ticks" } }
  ],
  ...
}
```

**To install them:** copy the three files into `config/dags/`
and restart (the pool reads DAGs at startup):

```
cp config/example/dags/worker_pool/*.json config/dags/
```

---

## Step 4 — The UI, screen by screen

Once the server is running with the pool enabled and the three DAGs installed,
here is exactly what you will see and where.

> ### A. The Dashboard — the "Execution" column
>
> **In the UI:** Open the **Dashboard**. Each DAG row has an **Execution**
> column. In single-process mode it shows *Main Process*. With the pool on and a
> DAG running, it shows a blue badge like
> — telling you which worker process that DAG landed on. Start all three example DAGs
> and you will see them on different worker numbers.

> ### B. Admin → Worker Pool (`/admin/workers`)
>
> **In the UI:** This is the control room. Find it under the **Admin** menu. It shows:
>
> - **Summary cards** at the top: how many *Workers* are
>   running and how many *DAGs* are assigned.
> - **Pool Controls**: buttons to manage the pool at runtime.
> - **Worker Processes grid**: one tile per worker, each showing its
>   health, CPU/memory use, and how many DAGs it is running. This is where you can
>   literally see the load spread across workers.
> - **DAG Assignments table**: every DAG, which worker it is on, its
>   status, and per-DAG actions. After starting the examples you will see
>   `wp_market_ingest` on Worker 0 (pinned), `wp_risk_engine`
>   alone on its exclusive worker, and `wp_reporting` wherever the pool
>   placed it.

> ### C. A DAG's Details page — execution info
>
> **In the UI:** Open any DAG's **Details** page. Near the top, the
> **Execution** banner now shows whether the DAG is running in a
> *worker* process (with its worker number and a health badge) or in the
> *main process*. This is the quickest way to confirm a single DAG's placement.

> ### D. Watching the hand-off in the logs
>
> **In the UI:** Use each DAG's **View Logs** button (filtered to that DAG) to watch the
> cross-worker pipeline flow: ingest emitting ticks, the risk engine consuming and
> publishing results, and reporting writing its file. Open
> `/tmp/log/dagserver/wp_report.jsonl` to see the end output accumulate.

---

## Step 5 — How automatic placement and rebalancing work

For DAGs without an affinity block, the `affinity` section of
`config/worker_config.json` decides placement:

```
"affinity": {
  "default_strategy": "weight_based",   // round_robin | weight_based | least_loaded | random
  "rebalance_enabled": true,
  "rebalance_threshold": 0.3,           // rebalance if load is >30% out of balance
  "rebalance_interval_seconds": 60,
  "allow_dag_pinning": true,
  "allow_exclusive_workers": true
}
```

|  |  |
| --- | --- |
| `weight_based` | Packs DAGs by estimated CPU cost so no worker is overloaded. **Recommended default.** |
| `least_loaded` | Each new DAG goes to the emptiest worker right now. |
| `round_robin` | Cycle through workers one by one — simple and even by count. |
| `random` | Scatter randomly. Useful for testing. |

If `rebalance_enabled` is on, the pool periodically checks whether load
has drifted out of balance and moves *automatically-placed* DAGs to even
things out. **Pinned and exclusive DAGs are never moved** — your
explicit choices are always respected.

## Step 6 — If a worker crashes

The pool monitors worker health. With `auto_restart_on_crash: true`
(the default), a crashed worker is restarted up to `max_restart_attempts`
times, and the DAGs it was running are re-dispatched to a healthy worker. You will
see the worker tile on `/admin/workers` drop and come back.

> **Tip —**
>
> **Pairs well with AutoClone.** Combine the worker pool with
> [AutoClone](/help/autoclone) to scale a hot DAG into
> several copies during peak hours, spread across your workers — horizontal
> scaling (more copies) on top of parallelism (more cores).

## Troubleshooting

|  |  |
| --- | --- |
| **Execution column still says "Main Process"** | The pool did not start. Check `enabled: true` in `config/worker_config.json` and that you *restarted* the server (not just reloaded DAGs). The startup banner prints a Worker Pool line. |
| **Server fails to start after enabling** | Install the LMDB package: `pip install -r requirements.txt` (cross-worker queues need it). |
| **Two DAGs on different workers can't exchange data** | They are using `mem://`, which does not cross processes. Switch those queues to `lmdb://` (or a real broker). |
| **A pinned DAG isn't on its worker** | Check `allow_dag_pinning: true` in the affinity section, and that `pinned_worker` is a valid worker number (0 to num\_workers−1). |

> **What this tutorial covered:** what the worker pool is and why it
> gives real parallelism, how to enable it in one file, the four placement styles
> (automatic, pinned, exclusive, preferred), a ready-made three-DAG cross-worker
> pipeline, a screen-by-screen UI walkthrough (Dashboard execution column,
> `/admin/workers` control room, Details execution banner, per-DAG logs),
> automatic rebalancing, and crash recovery.

## You have completed the tutorials

For more depth see the
[Worker Pool and DAG Affinity Guide](/help/worker-pool) under
User Guides, plus [AutoClone](/help/autoclone) and
[Scheduling](/help/scheduling).
