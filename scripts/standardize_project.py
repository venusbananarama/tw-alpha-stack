#!/usr/bin/env python3
"""
standardize_project.py
- Create scripts/ if missing
- Move helper tools into scripts/ (non-destructive; keeps backups)
- Drop a compatibility wrapper grid_run.py at root
- Generate README_project.md (if missing)
- Seed missing helper tools (grid/analyze/check/verify/requirements/run_grid.ps1)
"""
from __future__ import annotations
import shutil, sys, os, json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"

WRAPPER_GRID = """# Wrapper: forward to scripts/grid_run.py using current interpreter
import runpy, os, sys
here = os.path.dirname(os.path.abspath(__file__))
target = os.path.join(here, "scripts", "grid_run.py")
if not os.path.exists(target):
    raise SystemExit("scripts/grid_run.py not found. Place it there and re-run.")
runpy.run_path(target, run_name="__main__")
"""

GRID_RUN = """import argparse, json, sys, subprocess
from pathlib import Path
import pandas as pd

def run_backtest(factors, outdir, config, factor):
    out_path = Path(outdir) / f"{factor}"
    out_path.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "backtest.longonly_topN",
           "--factors", str(factors), "--out-dir", str(out_path),
           "--config", str(config), "--factor", factor]
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)
    perf_file = out_path / "performance.json"
    if perf_file.exists():
        return json.loads(perf_file.read_text(encoding="utf-8"))

def main():
    import itertools
    ap = argparse.ArgumentParser(description="Grid backtest runner (uses current Python)")
    ap.add_argument("--factors", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--factorset", nargs="+", default=["composite_score", "mom_252_21", "vol_20"])
    ap.add_argument("--topn", nargs="+", type=int, default=[20,50,100])   # kept for compatibility
    ap.add_argument("--rebalance", nargs="+", default=["M","W"])          # kept for compatibility
    a = ap.parse_args()

    rows = []
    for fac in a.factorset:
        try:
            perf = run_backtest(a.factors, a.outdir, a.config, fac)
            if perf:
                perf["factor"] = fac
                rows.append(perf)
        except Exception as e:
            print("[ERROR]", fac, e)

    if rows:
        df = pd.DataFrame(rows)
        outcsv = Path(a.outdir) / "summary.csv"
        df.to_csv(outcsv, index=False)
        print("[OK] Summary saved to", outcsv)
        try:
            print(df.sort_values("Sharpe", ascending=False).head())
        except Exception:
            pass

if __name__ == "__main__":
    main()
"""

ANALYZE_SUMMARY = """import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ap = argparse.ArgumentParser()
ap.add_argument("--summary", required=True)
ap.add_argument("--outdir", default=None)
a = ap.parse_args()

df = pd.read_csv(a.summary)
outdir = Path(a.outdir) if a.outdir else Path(a.summary).parent
outdir.mkdir(parents=True, exist_ok=True)

plt.figure()
for fac, g in df.groupby("factor"):
    plt.scatter(g["CAGR"], g["Sharpe"], label=str(fac))
plt.xlabel("CAGR"); plt.ylabel("Sharpe"); plt.title("Sharpe vs CAGR"); plt.legend()
plt.tight_layout(); plt.savefig(outdir / "scatter_sharpe_cagr.png", dpi=160); plt.close()

df.sort_values("Sharpe", ascending=False).to_csv(outdir / "summary_sorted_by_sharpe.csv", index=False)
print("[OK] Charts saved to", outdir)
"""

PROJECT_CHECK = """import argparse, re, shutil
from pathlib import Path
import pandas as pd

RULES = {
    "DELETE (cache)": lambda p: ("__pycache__" in str(p)) or p.suffix in (".pyc", ".pyo"),
    "ARCHIVE (backup)": lambda p: re.search(r"\.bak_\d{8}_\d{6}\.py$", p.name) is not None,
}

def classify(path: Path) -> str:
    sp = str(path).replace('\\','/')
    if sp.startswith('datahub/'): return "KEEP (data)"
    if sp.startswith('scripts/'): return "KEEP (scripts)"
    return "KEEP"

def main():
    ap = argparse.ArgumentParser(description="Project cleanup advisor")
    ap.add_argument("--root", default=".")
    ap.add_argument("--archive-into", default=None)
    ap.add_argument("--delete", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    root = Path(a.root).resolve()
    rows = []
    for p in root.rglob("*"):
        if p.is_dir(): continue
        rel = p.relative_to(root)
        label = "KEEP"
        for k, pred in RULES.items():
            if pred(rel): label = k; break
        rows.append((str(rel).replace('\\','/'), label))

    import pandas as pd
    df = pd.DataFrame(rows, columns=["relpath","disposition"]).sort_values(["disposition","relpath"])
    out = root / "cleanup_report.csv"
    df.to_csv(out, index=False)
    print("[OK] Report saved ->", out)

    arch = Path(a.archive_into) if a.archive_into else None
    if arch and not a.dry_run:
        arch.mkdir(parents=True, exist_ok=True)
        for rel, disp in df.values:
            if disp.startswith("ARCHIVE"):
                src = root / rel; dst = arch / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                try: shutil.move(str(src), str(dst)); print("[MOVED]", rel)
                except Exception as e: print("[SKIP]", rel, e)

    if a.delete and not a.dry_run:
        for rel, disp in df.values:
            if disp.startswith("DELETE"):
                src = root / rel
                try:
                    if src.is_dir(): shutil.rmtree(src, ignore_errors=True)
                    elif src.exists(): src.unlink()
                    print("[DELETED]", rel)
                except Exception as e:
                    print("[SKIP]", rel, e)

if __name__ == "__main__":
    main()
"""

VERIFY_ENV = """import sys, importlib
mods = ["pandas","numpy","yaml","pyarrow","matplotlib","openpyxl"]
print("Python:", sys.version)
print("Executable:", sys.executable)
for m in mods:
    try:
        mod = importlib.import_module(m)
        ver = getattr(mod, "__version__", "?")
        print(f"[OK] {m} {ver}")
    except Exception as e:
        print(f"[MISS] {m} -> {e}")
"""

REQUIREMENTS = "pandas>=2.2\nnumpy>=2.0\npyyaml>=6.0\npyarrow>=17.0\nmatplotlib>=3.8\nopenpyxl>=3.1\n"

RUN_GRID_PS1 = r"""param(
  [Parameter(Mandatory=$true)][string]$factors,
  [Parameter(Mandatory=$true)][string]$config,
  [Parameter(Mandatory=$true)][string]$outdir,
  [string[]]$factorset = @("composite_score","mom_252_21","vol_20"),
  [int[]]$topn = @(20,50,100),
  [string[]]$rebalance = @("M","W")
)
$py = Join-Path (Get-Location) ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }
$argv = @("scripts/grid_run.py","--factors",$factors,"--config",$config,"--outdir",$outdir,"--factorset") + $factorset + @("--topn") + $topn + @("--rebalance") + $rebalance
& $py $argv
"""

README = f"# FATAI / TW Alpha Stack — 標準化完成\n產生時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

def write_if_missing(path: Path, text: str):
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print("[SEEDED]", path.relative_to(ROOT))

def main():
    SCRIPTS.mkdir(exist_ok=True)

    # Move known tools into scripts/ (if they live in root)
    to_move = ["grid_run.py", "analyze_summary.py", "project_check.py"]
    for name in to_move:
        src = ROOT / name
        if src.exists():
            dst = SCRIPTS / name
            if not dst.exists():
                import shutil
                shutil.move(str(src), str(dst))
                print("[MOVED]", name, "-> scripts/")

    # Ensure wrapper at root
    write_if_missing(ROOT / "grid_run.py", WRAPPER_GRID)

    # Seed helper tools if missing
    write_if_missing(SCRIPTS / "grid_run.py", GRID_RUN)
    write_if_missing(SCRIPTS / "analyze_summary.py", ANALYZE_SUMMARY)
    write_if_missing(SCRIPTS / "project_check.py", PROJECT_CHECK)
    write_if_missing(ROOT / "verify_env.py", VERIFY_ENV)
    write_if_missing(ROOT / "requirements.txt", REQUIREMENTS)
    write_if_missing(ROOT / "run_grid.ps1", RUN_GRID_PS1)
    write_if_missing(ROOT / "README_project.md", README)

    print("[OK] Standardization complete. Tools are under scripts/.")

if __name__ == "__main__":
    main()
