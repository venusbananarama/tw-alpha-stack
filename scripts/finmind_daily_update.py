
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# FinMind daily update with Workers/QPS
import argparse, os, sys, pandas as pd, requests, time, threading
from datetime import datetime, timedelta
try:
    import yaml
except: print("[ERROR] need pyyaml"); sys.exit(1)
RAW_ROOT=os.path.join("data","finmind","raw"); REPORT_ROOT=os.path.join("data","finmind","reports")
class RateLimiter:
    def __init__(self,qps): self.interval=1.0/max(qps,0.01); self._next=0; self._lock=threading.Lock()
    def acquire(self):
        with self._lock:
            now=time.time()
            if now>=self._next: self._next=now+self.interval; return
            sleep_for=self._next-now; self._next+=self.interval
        time.sleep(sleep_for)
def make_session():
    s=requests.Session(); a=requests.adapters.HTTPAdapter(pool_connections=32,pool_maxsize=32)
    s.mount("https://",a); s.mount("http://",a); return s
def fm_fetch(sess,params,token,limiter):
    limiter.acquire(); 
    r=sess.get("https://api.finmindtrade.com/api/v4/data",headers={"Authorization": f"Bearer {token}"},params=params,timeout=60)
    j=r.json(); 
    if r.status_code!=200 or j.get("status") not in (200,"200"): raise RuntimeError(f"HTTP {r.status_code}: {j}")
    return pd.DataFrame(j.get("data",[]))
def ensure_parent(p): os.makedirs(os.path.dirname(p),exist_ok=True)
def write_parquet(df,p):
    if df.empty: print(f"[INFO] Empty frame, skip write: {p}"); return
    ensure_parent(p); df.to_parquet(p,index=False); print(f"[INFO] Wrote: {p} rows={len(df)}")
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--last-n-days",type=int,default=5)
    ap.add_argument("--datasets-yaml",default="configs/datasets.yaml"); ap.add_argument("--groups",default="prices,chip,derivatives,macro_others")
    ap.add_argument("--workers",type=int,default=6); ap.add_argument("--qps",type=float,default=1.6); args=ap.parse_args()
    token=os.environ.get("FINMIND_TOKEN"); 
    if not token: print("[ERROR] FINMIND_TOKEN empty"); sys.exit(2)
    end=datetime.utcnow().strftime("%Y-%m-%d"); start=(datetime.utcnow()-timedelta(days=args.last_n_days-1)).strftime("%Y-%m-%d")
    cfg=yaml.safe_load(open(args.datasets_yaml,"r",encoding="utf-8")); groups=[g.strip() for g in args.groups.split(",")]
    sess=make_session(); limiter=RateLimiter(args.qps)
    for g in groups:
        items=cfg.get("groups",{}).get(g,[{"dataset":g}])
        for item in items:
            ds=item["dataset"]; print(f"== Daily dataset {ds} ==")
            frames=[]
            for d in pd.bdate_range(start,end):
                try:
                    df=fm_fetch(sess,{"dataset":ds,"start_date":d.strftime("%Y-%m-%d")},token,limiter)
                    if not df.empty: frames.append(df)
                except Exception as ex: print(f"[WARN] {ds} day {d} {ex}")
            df=pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()
            out=os.path.join(RAW_ROOT,ds,f"{ds}__{start}_to_{end}.parquet"); write_parquet(df,out)
    verify=os.path.join(REPORT_ROOT,"daily_verify.csv")
    rows=[]
    for g in groups:
        items=cfg.get("groups",{}).get(g,[{"dataset":g}])
        for item in items:
            ds=item["dataset"]; dirp=os.path.join(RAW_ROOT,ds)
            if os.path.exists(dirp):
                for fn in sorted(os.listdir(dirp))[-3:]:
                    fp=os.path.join(dirp,fn); rows.append({"dataset":ds,"file":fn,"size":os.path.getsize(fp)})
    pd.DataFrame(rows).to_csv(verify,index=False); print(f"[INFO] Wrote verify: {verify}")
if __name__=="__main__": main()
