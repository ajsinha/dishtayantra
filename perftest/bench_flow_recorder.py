"""Flow recorder verification v2: hot-path cost, no-loss load, burst protection."""
import sys, time, tempfile
sys.path.insert(0, "/home/claude/dishtayantra-main")
from core.flow_recorder import FlowRecorder
from core.flow_store import SqliteFlowStore

TRADE = {"trade_id": "T-1000042", "symbol": "AAPL", "side": "BUY",
         "quantity": 500, "price": 187.45, "currency": "USD"}

def producer_rate(rec, n):
    out = {"notional_usd": 93725.0, "risk_band": "LOW"}
    t0 = time.perf_counter()
    for i in range(n):
        rec.record("trade-etl", "risk_calc", inputs=TRADE, output=out, targets=["pub"])
    dt = time.perf_counter() - t0
    return n/dt, dt*1e9/n

class SlowStore:
    """A deliberately slow store to force queue overload."""
    def __init__(self): self.n=0
    def write_batch(self, rows): self.n += len(rows); time.sleep(0.01)  # 10ms/batch
    def query(self,*a,**k): return []
    def purge_older_than_ms(self,c): return 0
    def distinct_dags(self): return []
    def state_at(self,*a,**k): return {}
    def iter_export(self,*a,**k): return iter(())

print("=== A. Hot-path cost: disabled vs enabled (serialization now off-thread) ===\n")
rec = FlowRecorder(); rec.configure(store=SqliteFlowStore(tempfile.mktemp(suffix=".db")),
                                    maxsize=2_000_000, batch_max=2000)
rec.disable()
r_off, ns_off = producer_rate(rec, 300_000)
print(f"  DISABLED : {r_off:13,.0f} fires/sec  ({ns_off:6.0f} ns/fire)")
rec.enable()
r_on, ns_on = producer_rate(rec, 300_000)
print(f"  ENABLED  : {r_on:13,.0f} fires/sec  ({ns_on:6.0f} ns/fire)")
print(f"  added cost on the compute thread: {ns_on-ns_off:.0f} ns/fire "
      f"(~{(ns_on-ns_off)/1000:.1f} us)")
time.sleep(1.0); rec.stop(flush=True)
s = rec.stats()
print(f"  persisted: fired={s['fired']:,} written={s['written']:,} dropped={s['dropped']:,}")
# sampling dials the cost down (record 1 in 10)
rec_s = FlowRecorder(); rec_s.configure(store=SqliteFlowStore(tempfile.mktemp(suffix=".db")),
                                        maxsize=2_000_000, batch_max=2000, sample_rate=0.1)
rec_s.enable()
r_s, ns_s = producer_rate(rec_s, 300_000)
print(f"  ENABLED @ sample_rate=0.1 : {r_s:13,.0f} fires/sec  ({ns_s:6.0f} ns/fire)")
time.sleep(0.5); rec_s.stop(flush=True)
print()

print("=== B. Sustainable load: nothing dropped when the store keeps up ===\n")
rec2 = FlowRecorder(); rec2.configure(store=SqliteFlowStore(tempfile.mktemp(suffix=".db")),
                                      maxsize=500_000, batch_max=2000)
rec2.enable()
for i in range(50_000):
    rec2.record("trade-etl","risk_calc",inputs=TRADE,output={"i":i},targets=["pub"])
time.sleep(1.5); rec2.stop(flush=True)
b = rec2.stats()
print(f"  fired={b['fired']:,}  written={b['written']:,}  dropped={b['dropped']:,}  "
      f"errors={b['errors']}  -> {'NO LOSS' if b['dropped']==0 and b['written']==b['fired'] else 'see nums'}\n")

print("=== C. Burst overload: a stuck/slow store must NOT stall compute ===\n")
slow = SlowStore()
rec3 = FlowRecorder(); rec3.configure(store=slow, maxsize=5_000, batch_max=500)
rec3.enable()
r_burst, ns_burst = producer_rate(rec3, 300_000)   # far faster than 10ms/batch store
g = rec3.stats()
print(f"  producer stayed at {r_burst:13,.0f} fires/sec ({ns_burst:.0f} ns/fire) "
      f"despite a 10ms/batch store")
print(f"  fired={g['fired']:,} dropped={g['dropped']:,} errors={g['errors']} "
      f"queued={g['queued']:,}")
print(f"  -> store capacity ~50k/s, but producer ran {r_burst/50000:.0f}x faster: "
      f"compute thread never blocked; overflow shed as counted drops "
      f"({'PASS' if r_burst>150_000 and g['dropped']>0 and g['errors']==0 else 'check'})")
rec3.stop(flush=False)

print("\n=== D. Correctness: range query + state reconstruction ===\n")
st = SqliteFlowStore(tempfile.mktemp(suffix=".db"))
rec4 = FlowRecorder(); rec4.configure(store=st, batch_max=500); rec4.enable()
for i in range(2000):
    rec4.record("trade-etl", f"node{i%3}", inputs=TRADE,
                output={"i": i, "notional_usd": 93725.0+i}, targets=["pub"])
time.sleep(0.6); rec4.stop(flush=True)
allev = st.query("trade-etl", limit=10_000)
mid = allev[len(allev)//2]["ts_ms"]
rng = st.query("trade-etl", t0_ms=allev[0]["ts_ms"], t1_ms=mid, limit=10_000)
stt = st.state_at("trade-etl", mid)
print(f"  total={len(allev):,}  range[..mid]={len(rng):,}  "
      f"state_at nodes={sorted(stt['nodes'].keys())}")
print(f"  sample structured output: {allev[0]['output']}")
print(f"  export rows (generator): {sum(1 for _ in st.iter_export('trade-etl'))}")
