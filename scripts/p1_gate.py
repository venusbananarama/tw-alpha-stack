from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alpha_core.phase2.corelib.io import atomic_write_json  # noqa: E402
from alpha_core.phase1 import paths as p1_paths  # noqa: E402


GATE_SCHEMA_VERSION = "gate_summary.v1"
GATE_SPEC_VERSION = "gate_rules.v2.0"

DEFAULT_STAGE1_DATASETS = [
    "prices",
    "chip",
    "per",
    "dividend",
    "shareholding",
    "inst_total",
    "gov_bank",
    "prices_daily",
]


def _now_iso() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def _w_fri(d: dt.date) -> dt.date:
    back = (d.weekday() - 4) % 7
    return d - dt.timedelta(days=back)


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, obj: Any) -> None:
    atomic_write_json(path, obj, ensure_ascii=False, indent=2)


def _write_p1_gate_summary(path: Path, gate: Dict[str, Any]) -> None:
    payload = dict(gate)
    overall = dict(payload.get("overall") or {})
    overall["stage"] = "p1"
    payload["overall"] = overall
    _write_json(path, payload)


def _run(cmd: List[str], *, cwd: Path) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    return p.returncode, p.stdout or "", p.stderr or ""


def run_preflight(
    repo: Path,
    reports: Path,
    rules: Path,
    expect_date: Optional[dt.date] = None,
) -> Path:
    py = sys.executable
    cmd = [
        py,
        str(repo / "scripts" / "p1_preflight_check.py"),
        "--rules",
        str(rules),
        "--export",
        str(reports),
        "--root",
        str(repo),
    ]
    if expect_date is not None:
        cmd.extend(["--expect-date", expect_date.isoformat()])
    rc, out, err = _run(cmd, cwd=repo)
    if rc != 0:
        raise SystemExit(f"[preflight] failed rc={rc}\nSTDOUT:\n{out}\nSTDERR:\n{err}")

    out_path = reports / "preflight_report.json"
    if not out_path.is_file():
        raise SystemExit(f"[preflight] missing output: {out_path}")
    return out_path


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.is_file():
        return []
    out: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append({k: (v or "").strip() for k, v in row.items()})
    return out


def _boolish(v: Any) -> bool:
    if v is None:
        return False
    s = str(v).strip()
    return s.lower() in ("1", "true", "t", "yes", "y")


def _dig_table_path(reports: Path, as_of: dt.date) -> Path:
    return reports / "dig" / f"factor_dig_table.{as_of.isoformat()}.csv"


def _derive_pass_fail_from_dig_table(reports: Path, as_of: dt.date) -> Tuple[List[str], List[str], List[int]]:
    dig_path = _dig_table_path(reports, as_of)
    rows = _read_csv_rows(dig_path)
    passed: List[str] = []
    failed: List[str] = []
    wins: List[int] = []

    for r in rows:
        fid = r.get("factor_id", "")
        if not fid:
            continue
        ok = _boolish(r.get("ok_rank_ic")) and _boolish(r.get("ok_coverage"))
        if ok:
            passed.append(fid)
        else:
            failed.append(fid)

        w = r.get("window_m", "") or r.get("window", "")
        if w:
            try:
                wins.append(int(str(w)))
            except Exception:
                pass

    passed = sorted(set(passed))
    failed = sorted(set(failed))
    windows = sorted({w for w in wins if w > 0})
    return passed, failed, windows


def compose_wf_summary(reports: Path, as_of: dt.date) -> Dict[str, Any]:
    wf_path = reports / "wf_summary.json"
    wf = _read_json(wf_path) or {}

    pass_rows = _read_csv_rows(reports / "pass_results.csv")
    fail_rows = _read_csv_rows(reports / "fail_results.csv")

    if pass_rows or fail_rows:
        passed = sorted({r.get("factor_id", "") for r in pass_rows if r.get("factor_id")})
        failed = sorted({r.get("factor_id", "") for r in fail_rows if r.get("factor_id")})

        wins: List[int] = []
        for r in pass_rows + fail_rows:
            w = r.get("window", "") or r.get("window_m", "")
            if w:
                try:
                    wins.append(int(str(w)))
                except Exception:
                    pass
        windows = sorted({w for w in wins if w > 0})
        source = "compose_from_pass_fail_csv"
    else:
        passed, failed, windows = _derive_pass_fail_from_dig_table(reports, as_of)
        source = "compose_from_dig_table"

    total = len(passed) + len(failed)
    pass_rate = (len(passed) / total) if total > 0 else 0.0

    overall = wf.get("overall") or {}
    wf_node = overall.get("wf") or {}
    wf_node["pass_rate"] = pass_rate
    if windows:
        wf_node["windows"] = windows
    wf_node["generated"] = _now_iso()
    wf_node["source"] = source

    overall["wf"] = wf_node
    wf["overall"] = overall

    fbs = wf.get("factors_by_status") or {}
    fbs["passed"] = passed
    fbs["failed"] = failed
    wf["factors_by_status"] = fbs

    _write_json(wf_path, wf)
    return wf


def _stage1_checks(pre: Dict[str, Any], datasets: List[str]) -> Tuple[bool, List[Dict[str, Any]]]:
    checks: List[Dict[str, Any]] = []
    freshness = pre.get("freshness") or {}

    all_ok = True
    for ds in datasets:
        r = freshness.get(ds) or {}
        ok = bool(r.get("ok"))
        if r:
            detail = json.dumps(r, ensure_ascii=False, sort_keys=True)
        else:
            detail = "missing in preflight_report.freshness"

        checks.append(
            {
                "name": f"preflight_freshness:{ds}",
                "pass": ok,
                "value": ok,
                "detail": detail,
            }
        )
        if not ok:
            all_ok = False

    return all_ok, checks


def build_gate_summary(
    *,
    as_of: dt.date,
    stage: str,
    mode: str,
    preflight_path: Path,
    wf_path: Optional[Path],
    stage1_pass: bool,
    checks: List[Dict[str, Any]],
    reason: str,
) -> Dict[str, Any]:
    return {
        "schema": GATE_SCHEMA_VERSION,
        "spec": GATE_SPEC_VERSION,
        "overall": {
            "stage": stage,
            "mode": mode,
            "gate": "PASS" if stage1_pass else "FAIL",
            "pass": stage1_pass,
            "reason": reason,
        },
        "checks": checks,
        "run": {
            "as_of": as_of.isoformat(),
            "generated": _now_iso(),
            "preflight_report": str(preflight_path),
            "wf_summary": str(wf_path) if wf_path else None,
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="p1_gate")
    ap.add_argument("--root", default=".", help="repo root")
    ap.add_argument("--reports", default="reports", help="reports dir (relative to root)")
    ap.add_argument("--rules", default="rules.yaml", help="rules.yaml (relative to root)")
    ap.add_argument("--date", default="", help="YYYY-MM-DD; default=latest W-FRI")
    ap.add_argument("--stage", choices=["p1", "all"], default="p1", help="p1=stop after preflight; all=compose+gate")
    ap.add_argument("--mode", choices=["safe", "formal"], default="safe", help="safe=compose; formal=read wf_summary")
    ap.add_argument("--datasets", default=",".join(DEFAULT_STAGE1_DATASETS), help="Stage-1 datasets CSV")
    ap.add_argument("--show-only", action="store_true", help="print gate_summary.json and exit")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    repo = Path(args.root).resolve()
    reports = (repo / args.reports).resolve()
    rules = (repo / args.rules).resolve()

    expect_date_override = _parse_date(args.date) if args.date else None
    as_of = expect_date_override if expect_date_override else _w_fri(dt.date.today())
    datasets = [x.strip() for x in (args.datasets or "").split(",") if x.strip()]

    gate_path = reports / "gate_summary.json"
    p1_gate_path = p1_paths.p1_gate_summary_path(repo)

    if args.show_only:
        g = _read_json(gate_path)
        if not g:
            raise SystemExit(f"[show-only] missing/unreadable: {gate_path}")
        print(json.dumps(g.get("overall", {}), ensure_ascii=False, indent=2))
        return 0

    preflight_path = run_preflight(repo, reports, rules, expect_date_override)
    pre = _read_json(preflight_path) or {}
    if expect_date_override is not None:
        pre_expect = str((pre.get("meta") or {}).get("expect_date") or "")
        if pre_expect != expect_date_override.isoformat():
            raise SystemExit(
                f"[preflight] expect_date mismatch: want={expect_date_override.isoformat()} got={pre_expect}"
            )

    ok1, c1 = _stage1_checks(pre, datasets)

    wf_path: Optional[Path] = None
    if args.stage == "p1":
        gate = build_gate_summary(
            as_of=as_of,
            stage="p1",
            mode="preflight_only",
            preflight_path=preflight_path,
            wf_path=None,
            stage1_pass=ok1,
            checks=c1,
            reason="ok" if ok1 else "preflight_fail",
        )
        _write_json(gate_path, gate)
        _write_p1_gate_summary(p1_gate_path, gate)
        if not args.quiet:
            print(f"[P1] gate={gate['overall']['gate']} wrote={gate_path}")
        return 0 if ok1 else 2

    if args.mode == "safe":
        compose_wf_summary(reports, as_of)
        wf_path = reports / "wf_summary.json"
    else:
        wf_path = reports / "wf_summary.json"
        if not wf_path.is_file():
            raise SystemExit("[formal] wf_summary.json missing")

    gate = build_gate_summary(
        as_of=as_of,
        stage="all",
        mode=args.mode,
        preflight_path=preflight_path,
        wf_path=wf_path,
        stage1_pass=ok1,
        checks=c1,
        reason="ok" if ok1 else "preflight_fail",
    )
    _write_json(gate_path, gate)
    _write_p1_gate_summary(p1_gate_path, gate)
    if not args.quiet:
        print(f"[ALL] gate={gate['overall']['gate']} wrote={gate_path}")
    return 0 if ok1 else 2


if __name__ == "__main__":
    raise SystemExit(main())


