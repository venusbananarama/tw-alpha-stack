import argparse, json, os, datetime
p=argparse.ArgumentParser(); p.add_argument("--date",required=True); p.add_argument("--wf-windows",nargs="*",default=["6","12","24"]); p.add_argument("--output",default="./reports")
a=p.parse_args(); os.makedirs(a.output,exist_ok=True); os.makedirs("./metrics",exist_ok=True)
rec={"ts":datetime.datetime.now().isoformat(),"run_id":f"p2eval-{a.date}","run_type":"factor_eval","step":"eval","windows":[int(x) for x in a.wf_windows],"status":"ok"}
open("./metrics/ingest_ledger.jsonl","a",encoding="utf-8").write(json.dumps(rec)+"\n"); print("[seed] eval ok")
