from pathlib import Path
import pandas as pd, pyarrow.dataset as ds, subprocess, sys, os, yaml
ROOT = Path(r"C:\AI\tw-alpha-stack")
UNI  = ROOT / "configs/universe.yaml"
BL   = ROOT / "configs/blacklist_empty.yaml"
BL_SYMS=set()
if BL.exists():
  try: BL_SYMS=set(map(str,yaml.safe_load(open(BL,"r",encoding="utf-8")).get("blacklist",{}).get("empty_prices",[])))
  except Exception: BL_SYMS=set()

def max_date(rel):
  d=ds.dataset(str(ROOT/f"datahub/silver/alpha/{rel}"), format="parquet")
  return pd.to_datetime(d.to_table(columns=["date"]).to_pandas()["date"]).max().date()

def run(ds_name, rel_name):
  start=str(pd.Timestamp(max_date(rel_name))+pd.Timedelta(days=1)).split(" ")[0]
  end  =pd.Timestamp.today().strftime("%Y-%m-%d")
  if start>end: print(f"[SKIP] {ds_name} up-to-date"); return 0
  cmd=[str(ROOT/".venv/Scripts/python.exe"),"-X","utf8",str(ROOT/"scripts/finmind_backfill.py"),
       "--start",start,"--end",end,"--datasets",ds_name,"--workers","4","--qps","1.0",
       "--datahub-root",str(ROOT/"datahub"),"--universe",str(UNI)]
  if BL_SYMS: cmd+=["--skip-symbols", ",".join(sorted(BL_SYMS))]
  env=os.environ.copy(); env["PYTHONUTF8"]="1"; env["PYTHONIOENCODING"]="utf-8"
  print("RUN:"," ".join(cmd),flush=True)
  return subprocess.call(cmd, env=env)

rc=0
rc|=run("TaiwanStockPrice","prices")
rc|=run("TaiwanStockDividend","dividend")
rc|=run("TaiwanStockPER","per")
rc|=run("TaiwanStockInstitutionalInvestorsBuySell","chip")
sys.exit(rc)
