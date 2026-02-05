from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "scripts" / "wf_runner_core.py"
SAFE = ROOT / "scripts" / "wf_runner_safe.py"


def _run_safe() -> None:
    if not SAFE.exists():
        sys.stderr.write("[bridge] wf_runner_safe.py not found.\n")
        raise SystemExit(2)
    py = os.environ.get("PY") or str((ROOT / ".venv" / "Scripts" / "python.exe"))
    if not Path(py).exists():
        py = sys.executable
    cmd = [py, str(SAFE), *sys.argv[1:]]
    raise SystemExit(subprocess.call(cmd))


def _run_legacy() -> None:
    if CORE.exists():
        try:
            src = CORE.read_text(encoding="utf-8")
            code = compile(src, str(CORE), "exec")
            g = {"__name__": "__main__", "__file__": str(CORE)}
            exec(code, g, None)
            return
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[bridge] falling back to safe runner: {type(exc).__name__}: {exc}\n")
            _run_safe()
    else:
        _run_safe()


def _is_p4_mode(argv: List[str]) -> bool:
    flags = {"--as-of", "--out-dir", "--p4"}
    return any(a in flags for a in argv)


def _try_read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _count_csv_rows(path: Path) -> int:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return 0
    if not lines:
        return 0
    return max(0, len([ln for ln in lines[1:] if ln.strip()]))


def _extract_pass_ratio(obj: Any) -> Tuple[float | None, int | None, int | None, str]:
    if isinstance(obj, dict):
        for key in ("pass_ratio", "wf_pass_ratio", "pass_rate", "wf_pass_rate"):
            if key in obj:
                try:
                    return float(obj[key]), None, None, f"json:{key}"
                except Exception:
                    pass
        if "runs" in obj and isinstance(obj["runs"], list):
            runs = obj["runs"]
            ok = 0
            total = 0
            for r in runs:
                if not isinstance(r, dict):
                    continue
                gate = r.get("gate") or {}
                if isinstance(gate, dict) and "ok" in gate:
                    total += 1
                    ok += 1 if bool(gate.get("ok")) else 0
            if total:
                return ok / total, ok, total, "json:runs"
    if isinstance(obj, list):
        ok = 0
        total = 0
        for r in obj:
            if isinstance(r, dict) and "pass" in r:
                total += 1
                ok += 1 if bool(r["pass"]) else 0
        if total:
            return ok / total, ok, total, "json:list"
    return None, None, None, "unknown"


def _load_pass_ratio(input_path: Path | None) -> Tuple[float, int, int, str, List[int]]:
    windows = [6, 12, 24]
    if input_path and input_path.exists():
        if input_path.suffix.lower() in (".json", ".jsonl"):
            obj = _try_read_json(input_path)
            ratio, ok, total, src = _extract_pass_ratio(obj)
            if isinstance(obj, dict) and isinstance(obj.get("windows"), list):
                try:
                    windows = [int(w) for w in obj["windows"]]
                except Exception:
                    pass
            if ratio is not None:
                return float(ratio), int(ok or 0), int(total or 0), src, windows
        if input_path.suffix.lower() == ".csv":
            df = pd.read_csv(input_path)
            if "pass" in df.columns:
                ok = int(df["pass"].sum())
                total = int(len(df))
                ratio = ok / total if total else 0.0
                return ratio, ok, total, "csv:pass", windows

    wf_summary = ROOT / "reports" / "wf_summary.json"
    if wf_summary.exists():
        obj = _try_read_json(wf_summary)
        ratio, ok, total, src = _extract_pass_ratio(obj)
        if isinstance(obj, dict) and isinstance(obj.get("windows"), list):
            try:
                windows = [int(w) for w in obj["windows"]]
            except Exception:
                pass
        if ratio is not None:
            return float(ratio), int(ok or 0), int(total or 0), src, windows

    runner_results = ROOT / "reports" / "_runner_results.json"
    if runner_results.exists():
        obj = _try_read_json(runner_results)
        ratio, ok, total, src = _extract_pass_ratio(obj)
        if ratio is not None:
            return float(ratio), int(ok or 0), int(total or 0), src, windows

    pass_csv = ROOT / "reports" / "pass_results.csv"
    fail_csv = ROOT / "reports" / "fail_results.csv"
    if pass_csv.exists() or fail_csv.exists():
        ok = _count_csv_rows(pass_csv)
        fail = _count_csv_rows(fail_csv)
        total = ok + fail
        ratio = ok / total if total else 0.0
        return ratio, ok, total, "csv:pass_fail", windows

    return 0.0, 0, 0, "default", windows


def _write_jsonl_atomic(records: List[Dict[str, Any]], path: Path) -> None:
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")
    tmp.replace(path)


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def _run_p4(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir) if args.out_dir else (ROOT / "reports" / "p4" / args.as_of)
    out_dir.mkdir(parents=True, exist_ok=True)

    ratio, ok, total, src, windows = _load_pass_ratio(Path(args.input) if args.input else None)
    pass_ratio = float(ratio)
    pass_ok = pass_ratio >= float(args.pass_threshold)

    rows = []
    for w in windows:
        rows.append(
            {
                "as_of": args.as_of,
                "window": int(w),
                "pass": bool(pass_ok),
                "pass_ratio": pass_ratio,
                "n_pass": int(ok),
                "n_total": int(total),
                "source": src,
            }
        )
    df = pd.DataFrame(rows)
    _write_parquet_atomic(df, out_dir / "wf_summary.parquet")

    records = []
    for row in rows:
        records.append(
            {
                "as_of": row["as_of"],
                "window": row["window"],
                "pass": row["pass"],
                "reason": "pass_ratio",
                "overall_pass_ratio": pass_ratio,
            }
        )
    records.append(
        {
            "as_of": args.as_of,
            "window": "ALL",
            "pass": bool(pass_ok),
            "reason": "overall",
            "overall_pass_ratio": pass_ratio,
        }
    )
    _write_jsonl_atomic(records, out_dir / "wf_gate.jsonl")
    return 0


def main() -> int:
    if not _is_p4_mode(sys.argv[1:]):
        _run_legacy()
        return 0

    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--config", default=None)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--input", default=None, help="optional wf summary json/csv")
    ap.add_argument("--pass-threshold", type=float, default=0.70)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    try:
        return _run_p4(args)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"WF_P4_ERROR: {type(exc).__name__}: {exc}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


