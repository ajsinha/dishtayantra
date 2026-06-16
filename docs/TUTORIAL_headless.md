# Tutorial: Headless Execution & Orchestration (a gentle, complete walk-through)

This tutorial teaches two related skills:

1. **Headless mode** — running a DishtaYantra pipeline *without* the web interface,
   so it processes a batch of work, finishes, and exits (perfect for a scheduled
   overnight job).
2. **Orchestration** — having one long-running DishtaYantra act as a *manager* that
   launches many headless workers in response to events, and reacts when they
   finish.

It assumes no prior DishtaYantra knowledge. If you have read
`docs/TUTORIAL_eod_batch.md`, you already know the core ideas (DAG, node, edge,
calculator, reactive); we recap them briefly so this tutorial stands on its own.
The reference-style summary is in `docs/HEADLESS_AND_ORCHESTRATION.md`.

---

## Part 0 — The ideas, in plain English

**Quick recap of the building blocks.** A DishtaYantra *pipeline* (a **DAG**) is a
flowchart of processing steps. Each step is a **node**; the arrows between them are
**edges**; the logic inside a step is a **calculator**. The engine is **reactive**:
a step runs when data arrives for it. That is all you need to carry forward.

**What does "headless" mean?** "Head" here is the web interface — the dashboard you
look at in a browser. Running *headless* means running the engine with **no
dashboard**: it just does its job and stops. Think of the difference between a
kitchen appliance with a screen and buttons versus the same motor wired straight
into a timer. The web UI is wonderful for designing and watching pipelines, but a
scheduled 2 a.m. batch job does not need a screen — it needs to start, finish, and
report whether it worked.

**Why would I want that?** Because most real batch work is launched by a
**scheduler** — enterprise tools like Autosys, Control-M, or Airflow, or even plain
`cron`. The scheduler says "it is 2 a.m. and the trade file has arrived; run the
EOD job", waits for it to finish, checks whether it **succeeded or failed**, and
then decides what to run next. For that, your job needs to (a) run to completion on
its own and (b) exit with a clear success/failure signal. That is exactly what
headless mode provides.

**What is "orchestration"?** Now imagine you do not have just one job, but many —
one per region, or one per incoming file, arriving throughout the day. You want
something that watches for these and launches the right job each time. That
"something" is an **orchestrator**, or **control plane**. The pattern is a manager
and a team of workers:

- The **control plane** is a long-running DishtaYantra (with the dashboard, so you
  can watch it). Its job is to *decide* and *delegate*, not to do heavy lifting. It
  reacts to events and launches workers.
- A **worker** is a short-lived, headless DishtaYantra that does one heavy job and
  then exits.

The analogy: a **dispatcher at a depot**. The dispatcher (control plane) does not
drive the trucks. When an order comes in, they assign it to a driver (launch a
worker), keep track of who is out, make sure not too many trucks leave at once, and
note when each driver returns. That is precisely the shape we will build.

**Why split it this way?** Two big reasons:

- **Isolation / safety.** If one heavy job crashes or eats all the memory, it does
  so in its *own* separate process. The control plane — and every other job —
  keeps running. One bad truck does not burn down the depot.
- **Cleanliness.** A worker starts, does its job, and exits, releasing all its
  resources. Nothing slowly leaks or piles up over days.

---

## Part 1 — Running a pipeline headless

Take any pipeline that reads from a queue — for example the `eod_enrichment`
pipeline from the EOD tutorial — and run it headless with one command:

```bash
python -m core.dag.headless_runner \
    --config eod_enrichment.json \
    --replay trades.jsonl \
    --expect 20000 \
    --summary eod_summary.json \
    --job-id eod-2026-06-15
```

Here is what each option means, in plain terms:

- **`--config`** — the pipeline to run (the JSON file describing the DAG).
- **`--replay`** — a file of input records to feed in, one line at a time, as if
  they were arriving live. (In production, where data arrives over a real message
  broker like Kafka, you would leave this out — the data simply shows up on its
  own.)
- **`--expect 20000`** — "you should produce 20,000 results." This is how the
  runner knows when the job is **done**: once 20,000 results have come out the
  other end, the work is complete. (If you do not know the count, you can omit this
  and the runner detects completion a different way — see "How does it know it is
  finished?" below.)
- **`--summary`** — a file to write a short report into.
- **`--job-id`** — a label for this run, echoed into the report so you can match it
  up later.

When it finishes it writes (and prints) a small report like this:

```json
{"job_id": "eod-2026-06-15", "dag": "eod_enrichment", "fed": 20000,
 "published": 20001, "expected": 20000, "completed_by": "count",
 "elapsed_s": 1.03, "status": "ok"}
```

Reading the report: it fed in 20,000 records, produced 20,001 results
(`published`), finished because it reached the expected **count**, took about a
second, and the overall `status` is `ok`. The process then exits with code **0**
for success, or **1** if it could not finish — so a scheduler can simply check the
exit code and branch on it.

> *Why 20,001 and not 20,000?* When the engine starts, it sends a single empty
> "hello, I'm alive" message through the pipeline before any real data. That counts
> as one extra output. The count-based completion (`>= expected`) handles this
> automatically — it is harmless and worth knowing about so the off-by-one does not
> surprise you.

**How does it know it is finished?** Two ways:

- **By count** (what we used): "done when N results have come out." Best when you
  know how many records the file has.
- **By quiescence** (the fallback): "done when the pipeline has gone *quiet* — no
  step has anything left to do and the input queue is empty — and stays quiet for a
  few checks in a row." Best when you do not know the count up front. "Quiescence"
  just means stillness: the machine has stopped humming.

Either way, the runner **drains** the pipeline before stopping — it waits for
every in-flight record to finish rather than cutting off mid-stream — so no data is
lost on shutdown.

---

## Part 2 — From one job to many: the manager and workers

Now the orchestration pattern. We will build a control-plane pipeline whose shape
is:

```
job_requests --> [dispatch] --> log         (when a request arrives, launch a worker)
job_done     --> [react]    --> ...          (when a worker finishes, react to it)
```

In words: requests for work arrive on one input (`job_requests`). A special
**dispatch** step launches a headless worker for each request. When a worker
finishes, it reports back on a second input (`job_done`), and a **react** step lets
the manager respond (log it, alert on failure, kick off the next stage, whatever
you need).

---

## Part 3 — The dispatcher (the manager's launching desk)

The step that launches workers is a special calculator called
`JobDispatchCalculator`. It is unusual because most calculators are *pure* — they
just transform data and have no side effects. This one deliberately has a side
effect: it **starts other programs**. Because launching processes is powerful and
a little dangerous, it is built with four safety properties. You configure it like
any other calculator:

```json
{"name": "dispatch",
 "type": "core.dag.job_dispatch.JobDispatchCalculator",
 "config": {"max_concurrent": 4, "summary_dir": "/var/dy/jobs", "done_queue": "job_done"}}
```

Each incoming request is a small dictionary carrying a `job_id` (a unique name for
the job), a `config` (which pipeline the worker should run), and optionally
`replay`/`expect` (the input file and how many results to expect). The dispatcher
then runs, for each request:

```
python -m core.dag.headless_runner --config <config> --job-id <job_id> --summary ...
```

The four safety properties, with the depot analogy:

- **Idempotent** — *exactly one truck per order, even if the order is read twice.*
  If the same `job_id` arrives again (a duplicate event, or the engine re-examining
  the same input), the dispatcher recognises it and does **not** launch a second
  worker. You never accidentally run the same job twice.
- **Asynchronous** — *the dispatcher does not ride along in the truck.* It launches
  the worker and immediately goes back to waiting for the next request. It never
  freezes while a job runs; a background helper quietly watches each worker for
  completion.
- **Bounded** — *only so many trucks on the road at once.* `max_concurrent: 4`
  means at most four workers run simultaneously; extra requests wait politely in a
  queue and start as soon as a slot frees up. This stops a sudden flood of requests
  from launching a thousand processes and crushing the machine.
- **Observable** — *the dispatcher logs every return.* When a worker finishes, the
  dispatcher reads that worker's summary file, records the result, and (if you set
  `done_queue`) publishes a "job done" event back into the pipeline. It can also
  cleanly **shut down** any still-running workers, so none are ever orphaned.

---

## Part 4 — Closing the loop (reacting when a worker finishes)

Setting `done_queue` is what makes the system *reactive end to end*. When a worker
finishes, its result is fed back in as a `job_done` event, and the control plane
reacts to it exactly like any other input — log success, raise an alert on failure,
or trigger the next job in a chain.

A note on how the worker tells the manager it is done, since they are separate
programs: in production they communicate through a shared message broker (Kafka,
RabbitMQ, Redis) — the worker publishes "done" and the manager is subscribed. In
the in-process demo below, the worker writes its summary to a **file**, and the
manager reads that file when the worker exits, then publishes the "done" event
internally. Same idea, simpler plumbing for a single-machine demo.

---

## Part 5 — Run the whole thing

The shipped demo wires all of the above together — a control plane that dispatches
headless EOD workers and reacts to their completion:

```bash
python -m perftest.run_orchestration_example --jobs 3
```

It sends three job requests **plus one duplicate** (to prove the idempotency
property). Watch what happens:

- three separate worker **processes** start, each runs an EOD enrichment job and
  exits;
- the **duplicate** request is recognised and **skipped** — no fourth worker;
- the control plane receives a **"job done"** event for each of the three completed
  jobs and reacts to it.

That is the entire pattern in motion: decide, delegate, bound, and react.

---

## Part 6 — Which shape should I use?

There are three sensible arrangements; pick by your situation.

- **Let your scheduler launch a run-once worker.** If you already run Autosys,
  Control-M, Airflow, or cron, the simplest design is: the scheduler watches for the
  input file, then runs `headless_runner`; the job processes and exits. Nothing
  polls or sits idle in between. This is the leanest and most standard option, and
  often all you need.
- **Let a control-plane DAG sense and dispatch.** If there is no external scheduler
  to lean on, a long-running control plane can watch for readiness itself (for
  example, a timer step that checks every minute for a "the file is ready" marker)
  and dispatch workers. Self-contained, at the cost of running all the time.
- **Separate processes vs. in-process parallelism.** Use separate headless
  *processes* (this whole tutorial) when you want **isolation and clean exit** for
  heavy jobs. If you only need more throughput on one machine and do not need
  isolation, DishtaYantra also has lighter in-process parallelism (a worker pool and
  "autoclone"); see the Help Center → *Auto-Cloning* and → *Worker Pool*.

One principle worth tattooing on the wall: **prefer an explicit "I'm complete"
signal over guessing by time.** A manifest file, a trailer record that says "1,000
rows follow", or a known control count is far safer than "assume it's done if
nothing arrived for five minutes." Time-based guesses are exactly how batch
pipelines silently process half a file and no one notices until the numbers are
wrong.

---

## Where to go next

- **Reference + the dispatcher's full guarantees:** `docs/HEADLESS_AND_ORCHESTRATION.md`
- **The EOD job these workers actually run:** `docs/TUTORIAL_eod_batch.md`
- **In-process scaling instead of separate processes:** Help Center → *Auto-Cloning*, → *Worker Pool*
