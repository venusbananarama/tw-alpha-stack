# coding: utf-8
import sys, os, subprocess, time
from datetime import datetime, timedelta

argv = sys.argv[1:]
script_dir = os.path.dirname(os.path.abspath(__file__))
orig = os.path.join(script_dir, "fm_dateid_fetch.py")

def _debug(msg):
    if os.environ.get("WRAP_DEBUG") == "1":
        sys.stderr.write(msg + "\n")

def get_arg(name, default=None):
    try:
        i = argv.index(name)
    except ValueError:
        return default
    return argv[i+1] if i+1 < len(argv) else default

def set_or_add_arg(lst, flag, value):
    a = list(lst)
    if flag in a:
        j = a.index(flag)
        if j+1 < len(a):
            a[j+1] = value
        else:
            a += [flag, value]
    else:
        a += [flag, value]
    return a

def strip_wrapper_flags(lst):
    # wrapper 自用旗標不下傳到底層
    return [x for x in lst if x != "--strict"]

# ---- 參數正規化：支援 --datasets/--ids 並轉為 --dataset/--symbols/--id-key ----
datasets = []
ds = get_arg("--datasets") or get_arg("--dataset")
if ds:
    datasets = [x.strip() for x in ds.split(",") if x.strip()]
ids = get_arg("--ids") or get_arg("--symbols")
target_ds = datasets[0] if datasets else None

def infer_id_key(dsname):
    if dsname in {"TaiwanStockKBar", "TaiwanStockPrice"}:
        return "stock_id"
    return "data_id"

strict = ("--strict" in argv)

def normalize_args(args):
    a = list(args)
    if "--datasets" in a and "--dataset" not in a and target_ds:
        a = set_or_add_arg(a, "--dataset", target_ds)
        i = a.index("--datasets"); del a[i:i+2]
    if "--ids" in a and "--symbols" not in a and ids:
        a = set_or_add_arg(a, "--symbols", ids)
        i = a.index("--ids"); del a[i:i+2]
    if "--id-key" not in a and target_ds:
        a = set_or_add_arg(a, "--id-key", infer_id_key(target_ds))
    return a

def add_parvc_limits(a):
    # ParValueChange 專屬：限速 + 重試參數
    a = set_or_add_arg(a, "--max-retries", "3")
    a = set_or_add_arg(a, "--rpm", "4")
    return a

def call_orig(args, rewrite_parvc_warn=False):
    na = strip_wrapper_flags(normalize_args(args))
    _debug("DEBUG cmd: " + " ".join([orig] + na))
    # 以 capture_output 執行，完畢後再重寫輸出
    r = subprocess.run([sys.executable, orig] + na, capture_output=True, text=True)
    out, err = r.stdout, r.stderr
    if rewrite_parvc_warn:
        out = out.replace("FAIL TaiwanStockParValueChange", "[WARN] TaiwanStockParValueChange")
        err = err.replace("FAIL TaiwanStockParValueChange", "[WARN] TaiwanStockParValueChange")
    if out:
        sys.stdout.write(out); sys.stdout.flush()
    if err:
        sys.stderr.write(err); sys.stderr.flush()
    return r.returncode

def call_with_backoff(a, rounds=3, sleeps=(0,10,25), rewrite_warn=False):
    rc = 1
    for k in range(rounds):
        if k < len(sleeps) and sleeps[k] > 0:
            time.sleep(sleeps[k])
        rc = call_orig(a, rewrite_parvc_warn=rewrite_warn)
        if rc == 0:
            return 0
    return rc

# ---- 白名單：做 range→daily 的 fallback ----
FALLBACK_DS = {"TaiwanStockParValueChange", "TaiwanStockShareholding"}

if target_ds not in FALLBACK_DS:
    sys.exit(call_orig(argv, rewrite_parvc_warn=False))

dsname = target_ds or "Dataset"

# 1) 先跑整段（ParValueChange 帶限速與重試；寬鬆時做文字重寫）
args_range = list(argv)
rewrite_warn = (dsname == "TaiwanStockParValueChange" and not strict)
if dsname == "TaiwanStockParValueChange":
    args_range = add_parvc_limits(args_range)
rc = call_with_backoff(args_range, rounds=3, sleeps=(0,10,25), rewrite_warn=rewrite_warn)
if rc == 0:
    sys.exit(0)

# 2) range 失敗 → daily fallback
start = get_arg("--start")
end   = get_arg("--end")
if not start or not end:
    sys.exit(rc)

fmt = "%Y-%m-%d"
try:
    s = datetime.strptime(start, fmt)
    e = datetime.strptime(end, fmt)
except Exception:
    sys.exit(rc)

delta_days = max((e - s).days, 1)
any_failed = False

for i in range(delta_days):
    d1 = s + timedelta(days=i)
    d2 = d1 + timedelta(days=1)
    a = list(argv)
    a = set_or_add_arg(a, "--start", d1.strftime(fmt))
    a = set_or_add_arg(a, "--end",   d2.strftime(fmt))
    if dsname == "TaiwanStockParValueChange":
        a = add_parvc_limits(a)
    rc1 = call_with_backoff(a, rounds=3, sleeps=(0,10,25), rewrite_warn=rewrite_warn)
    if rc1 != 0:
        any_failed = True
        if strict:
            sys.exit(rc1)
        else:
            sys.stderr.write(f"[WARN] {dsname} daily fetch failed for {d1.strftime(fmt)} (exit={rc1})\n")

# 3) 回報碼：寬鬆→永遠 0；嚴格→有失敗則 1
sys.exit(0 if not strict else (0 if not any_failed else 1))
