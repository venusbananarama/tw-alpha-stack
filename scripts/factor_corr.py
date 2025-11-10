import argparse, json, os, datetime
p=argparse.ArgumentParser(); p.add_argument("--date",required=True); p.add_argument("--output",default="./reports")
a=p.parse_args(); os.makedirs(a.output,exist_ok=True)
rec={"ts":datetime.datetime.now().isoformat(),"run_id":f"p2corr-{a.date}","run_type":"factor_corr","step":"corr","status":"ok"}
open("./metrics/ingest_ledger.jsonl","a",encoding="utf-8").write(json.dumps(rec)+"\n"); open(os.path.join(a.output,"factor_corr.json"),"w").write('{"note":"seed"}'); print("[seed] corr ok")
