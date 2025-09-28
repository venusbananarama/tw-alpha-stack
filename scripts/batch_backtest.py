# -*- coding: utf-8 -*-
from __future__ import annotations

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse, yaml, itertools, subprocess, json
from datetime import datetime
from typing import List

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))

def safe_name(val: str) -> str:
    return str(val).replace("=", "_").replace(".", "p").replace(" ", "")

def worker_run(run:dict, args, base_cfg:dict, augmented_path:str, root_out:str):
    cols = run["factors"]
    method = run.get("combine", "zscore_mean")
    score_col = "__".join(cols) + f"__{method}"
    safe_overrides = [f"{k}_{str(v).replace('.', 'p')}" for k,v in (run.get("overrides") or {}).items()]
    run_name = "__".join([score_col] + safe_overrides)
    out_dir = os.path.join(root_out, run_name)
    os.makedirs(out_dir, exist_ok=True)

    nav_path = os.path.join(out_dir, "nav.csv")
    if args.make_report and os.path.exists(nav_path):
        cmd = [sys.executable, os.path.join(REPO_ROOT, "scripts", "make_report.py"),
               "--nav-csv", nav_path,
               "--date-col", "date",
               "--value-col", "nav",
               "--out-dir", out_dir,
               "--freq", args.freq,
               "--title", run_name,
               "--name", run_name]
        with open(os.path.join(out_dir, "_report_error.txt"), "w", encoding="utf-8") as f:
            try:
                subprocess.run(cmd, cwd=REPO_ROOT, stdout=f, stderr=f, text=True, check=True)
            except subprocess.CalledProcessError as e:
                f.write("\n[Exception] make_report failed with return code %s\n" % e.returncode)
            except Exception as e:
                f.write("\n[Exception] " + str(e) + "\n")