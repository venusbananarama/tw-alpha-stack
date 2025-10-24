from __future__ import annotations
import json, sys, argparse
from pathlib import Path
from datetime import datetime

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", dest="run_target", default="runs/wf_configs")
    ap.add_argument("--file", dest="run_file", default=None)
    ap.add_argument("--export", default="reports")
    args = ap.parse_args()

    root = Path(".").resolve()
    export_dir = (root / args.export); export_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = export_dir / f"wf_gate_summary_{ts}.json"

    # 最小可用摘要（避免舊檔被沿用）。保守標 FAIL，等正式 runner 修好再覆蓋。
    summary = {
        "capacity_ok": False,
        "t": 0.0,
        "psr": 0.0,
        "execution_replay_mae_bps": 999.0,
        "max_drawdown_pct": 99.0,
        "run_id": f"stub_{ts}",
        "dsr_after_costs": -1.0,
        "sharpe": 0.0,
        "wf_pass_rate": 0.0,
        "overall": "FAIL",
        "mode": "runner-safe-fallback",
        "note": "Original wf_runner.py failed (indent/syntax). Stub summary exported to unblock Gate.",
    }
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SAFE] Exported {out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
