# Java Calculators on the JVM Gateway Pool

Run calculation steps in **Java on a real JVM** while the DAG
orchestration stays in Python. A pool of Py4J gateways — the
**JVM worker pool** — gives you several concurrent lanes into
the JVM so many Java calls run at once. This tutorial explains the idea simply,
lists the prerequisites honestly, ships a ready-made example, and walks the UI.

## The idea, in plain language

Most calculators on this platform are Python. But sometimes the maths you need
already exists in **Java** — a pricing library, a risk model, a
validated in-house JAR. Instead of rewriting it, the platform launches a
**Java Virtual Machine** alongside the server and runs those calculators
*inside* it. Your DAG looks normal; specific nodes just happen to execute in Java.

The bridge between Python and Java is **Py4J**. Rather than one
single connection (which would be a bottleneck), the platform opens a
**pool of gateway connections** — each on its own port — so
multiple Java calculator calls can be in flight at the same time. That pool of lanes
into the JVM is what we mean by the **JVM worker pool**.

```
  Python (DAG orchestration)                    JVM (Java calculators)
  ┌───────────────────────┐    Py4J gateway pool   ┌──────────────────────┐
  │ trade_in  ─► price ─►  │  ═══ port 25333 ════►  │  TradePricingCalc    │
  │            risk  ─►    │  ═══ port 25334 ════►  │  RiskCalculator      │
  │            sinks       │  ═══ port 25335 ════►  │  DataValidationCalc  │
  └───────────────────────┘  ═══ port 25336 ════►  └──────────────────────┘
                              (pool_size = 4 lanes)
```

> **How is this different from the Tutorial 7 worker pool?** The
> [worker pool](/help/worker-pool) spreads whole
> *Python DAGs* across CPU cores. The JVM gateway pool runs individual
> *Java calculator calls* inside a JVM. They are complementary: you can run a
> DAG on a Python worker *and* have its calculators call into the JVM pool.

## Prerequisites (read this first)

Unlike the pure-Python tutorials, this one needs three things in place before the
example will load — Java calculators require a live JVM:

1. **Install Py4J:** `pip install py4j` (it is in
   `requirements.txt`, so `pip install -r requirements.txt` covers it).
2. **Build the Java JAR:** run `java/build.sh` from the project
   root. This compiles the example calculators into

   **java/lib/dishtayantra-calculators.jar**

   and fetches
   `py4j.jar`. You need a JDK installed.
3. **Enable the JVM manager:** turn it on in

   **config/jvm\_config.json**

   (an example is provided —
   see Step 1).

> **Warning —**
>
> **Without these, the example DAG will not load** — a Java
> calculator cannot be created if there is no JVM to host it. That is expected. The
> rest of the platform runs fine without Java; this is an opt-in capability.

---

## Step 1 — Turn the JVM manager on

The JVM is configured by

**config/jvm\_config.json**

, which
ships **disabled**. A ready-to-use example is at

**config/example/jvm\_config.example.json**

.

1. Back up: `cp config/jvm_config.json config/jvm_config.json.bak`
2. Copy the example over it:
   `cp config/example/jvm_config.example.json config/jvm_config.json`
3. Confirm the manager and the primary gateway pool:

```
"jvm_manager": {
  "enabled": true,            // turn the JVM manager ON
  "auto_start_on_load": true  // launch the JVM when the server boots
},
"gateways": [
  {
    "name": "primary",
    "enabled": true,
    "base_port": 25333,
    "pool_size": 4,           // THE JVM WORKER POOL: 4 concurrent lanes (ports 25333-25336)
    "jvm": {
      "heap_size_mb": 512,
      "max_heap_size_mb": 2048,
      "classpath": ["java/lib/*"]
    }
  }
]
```

`pool_size` is the number of gateway lanes into that JVM — raise it if
many DAGs call Java calculators at once. Then **restart the server** (the
JVM starts at boot).

## Step 2 — The predefined Java calculators

The example JAR ships several ready calculators, listed in the
`calculators.definitions` section of the config. Each maps a friendly name to
a Java class:

| Name | What it does |
| --- | --- |
| `DataValidationCalculator` | Validates records against rules |
| `TradePricingCalculator` | Pricing with commission & fees |
| `RiskCalculator` | Value-at-Risk (VaR) metrics |
| `MathCalculator` | Arithmetic on numeric fields |
| `AggregationCalculator` | sum / avg / min / max |
| `PortfolioCalculator` | NAV and P&L |
| `StringTransformCalculator` | upper / lower / trim |
| `PassthruCalculator` | Returns input unchanged (testing) |

## Step 3 — The ready-made example DAG

The example ships at

**config/example/dags/jvm/jvm\_trade\_risk\_pipeline.json**

: a
validate → price → risk pipeline where **every calculation runs in
Java**, with Python handling the sinks. Each Java calculator declares its class and
which gateway pool to use:

```
"calculators": [
  {
    "name": "price",
    "type": "core.calculator.java_calculator.JavaCalculator",
    "config": {
      "java_class": "com.dishtayantra.calculators.examples.ExampleCalculators$TradePricingCalculator",
      "gateway_config": { "gateway": "primary" },
      "commission_rate": 0.001,
      "fee_per_trade": 1.50
    }
  }
]
```

|  |  |
| --- | --- |
| `type` | The full path `core.calculator.java_calculator.JavaCalculator` — this is what routes the node into the JVM. |
| `java_class` | The fully-qualified Java class to run (must be on the JVM's classpath). |
| `gateway_config.gateway` | Which pool to use — `"primary"`, or `"secondary"` for an isolated second JVM (see Step 5). |
| other keys | Passed straight through to the Java calculator as its configuration. |

**Install it** (after the prerequisites are met):

```
cp config/example/dags/jvm/jvm_trade_risk_pipeline.json config/dags/
```

Then reload DAGs and start it. Publish a trade onto `mem://queue/jvm_trades`
and watch the priced/risk output appear, with risk lines written to
`/tmp/log/dagserver/jvm_risk.jsonl`.

---

## Step 4 — The UI, screen by screen

> ### A. JVM Management (`/jvm`)
>
> **In the UI:** Find **JVM Management** under the menu. It shows summary tiles
> (*Gateways Configured*, *Java Calculators*), then a **Py4J
> Gateways** section with one card per gateway. Each card shows the gateway's
> connection status and gives **Stop / Restart / Reconnect** buttons and a
> *Details* link. There is also a **Reload Config** button to
> re-read `jvm_config.json` without a full restart.

> ### B. Gateway details (`/jvm/gateway/primary`)
>
> **In the UI:** Click a gateway's *Details*. You will see whether the JVM was started, the
> connection status (a green *Connected* / red *Disconnected* badge), the
> host and **base port**, the configured **pool size**, and the
> JVM/connection settings. This is where you confirm your `pool_size` lanes are
> live.

> ### C. Java calculators (`/jvm/calculators`)
>
> **In the UI:** The **Java Calculators** table lists every predefined calculator with its
> gateway. Click one to open its detail page, which shows the Java class, default
> configuration, and a copy-paste **DAG configuration example** for using it
> — handy when wiring a new DAG.

> ### D. Logs
>
> **In the UI:** Java/Py4J activity is logged to `logs/jvm_gateway.log` (configurable). Use a
> DAG's **View Logs** to follow the Python side of the pipeline as Java calls
> return.

---

## Step 5 — A second JVM pool for isolation

The example config defines a **secondary** gateway (disabled by default). Enable
it to run a separate JVM — for example, to keep heavy risk jobs off the JVM that serves
fast pricing calls, or to give a team its own heap. A calculator picks its pool with
`gateway_config`:

```
"gateway_config": { "gateway": "secondary" }
```

This is the JVM analogue of pinning a DAG to a worker: route specific Java work to a
specific JVM.

## Troubleshooting

|  |  |
| --- | --- |
| **DAG won't load: "Py4J is not installed"** | `pip install py4j` (or `pip install -r requirements.txt`) and restart. |
| **Gateway shows Disconnected** | The JVM didn't start. Check a JDK is installed, that `java/build.sh` produced the JAR, and the classpath in `jvm_config.json` points at `java/lib/*`. |
| **"java\_class must be specified"** | Every JVM calculator needs a `java_class` in its config. |
| **Class not found on the JVM** | The class isn't on the classpath. Rebuild with `java/build.sh` or add your JAR to the gateway's `classpath`. |
| **Calls are slow under load** | Raise `pool_size` (more lanes) or enable the secondary pool to spread Java work. |

> **Tip —**
>
> **Combine the two pools.** Put a Java-calculator DAG on a Python
> [worker](/help/worker-pool) and have it call the JVM gateway
> pool: the DAG runs in parallel with other DAGs across cores, while its Java steps run in
> the JVM. Process-level parallelism plus JVM concurrency.

> **What this tutorial covered:** what the JVM gateway pool is and how it
> differs from the Python worker pool, the honest prerequisites (py4j, the built JAR, the
> enabled manager), how to size the pool with `pool_size`, the predefined Java
> calculators, a ready-made all-Java pipeline, a screen-by-screen UI walkthrough
> (`/jvm`, gateway details, calculators), and a second JVM pool for isolation.

## You have completed the tutorials

For more depth see the Multi-Language
Calculator Integration Guide under User Guides, and the
[Worker Pool](/help/worker-pool) tutorial it pairs with.
