import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import multiprocessing, time, subprocess, os

def cpu_pct(pid, dt=1.0):
    def ticks(p):
        with open(f"/proc/{p}/stat") as f: parts=f.read().split()
        return int(parts[13])+int(parts[14])   # utime+stime
    try:
        a=ticks(pid); time.sleep(dt); b=ticks(pid)
        hz=os.sysconf("SC_CLK_TCK")
        return round((b-a)/hz/dt*100,1)
    except Exception as e:
        return f"err:{e}"

def main():
    from core.workers import WorkerPoolManager
    cfg = {"worker_pool":{"enabled":True,"num_workers":1,"worker_startup_timeout_seconds":8,
            "status_report_interval_seconds":2,"health_check_interval_seconds":5,
            "auto_restart_on_crash":True,"max_restart_attempts":3},
           "communication":{"use_lmdb_for_cross_worker":False},
           "affinity":{"default_strategy":"round_robin"}}
    print("starting pool...",flush=True)
    pool=WorkerPoolManager(config=cfg)
    t0=time.time(); pool.start(); print(f"start() returned in {time.time()-t0:.1f}s",flush=True)
    for i in range(3):
        wid,wi=next(iter(pool.workers.items()))
        pid=wi.process.pid; alive=wi.process.is_alive()
        n=len([l for l in subprocess.run(["pgrep","-f","repro_pool"],capture_output=True,text=True).stdout.splitlines() if l])
        c=cpu_pct(pid) if alive else "-"
        print(f"[t+{i+1}] worker pid={pid} alive={alive} cpu%={c} | total repro procs={n}",flush=True)
    print("stopping...",flush=True); pool.stop(timeout=5); print("stopped",flush=True)

if __name__=="__main__":
    try: multiprocessing.set_start_method("spawn",force=True)
    except RuntimeError: pass
    main()
