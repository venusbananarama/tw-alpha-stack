from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def _extract_attempt_row(payload: Mapping[str, Any]) -> Dict[str, Any]:
    variant = payload.get("variant") if isinstance(payload.get("variant"), Mapping) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}

    return {
        "seq": payload.get("seq"),
        "attempt_id": payload.get("attempt_id"),
        "factor_id": variant.get("factor_id"),
        "variant_id": variant.get("variant_id"),
        "bottleneck_window": payload.get("bottleneck_window"),
        "passed": payload.get("passed"),
        "early_stopped": payload.get("early_stopped"),
        "rank_ic_min": metrics.get("rank_ic_min"),
        "coverage_min": metrics.get("coverage_min"),
        "elapsed_sec": payload.get("elapsed_sec"),
        "error": payload.get("error"),
    }


def _collect_attempt_rows(run_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    attempt_root = run_dir / "attempt_logs"
    if not attempt_root.is_dir():
        return rows

    for path in sorted(attempt_root.glob("*/attempt_summary.json")):
        payload = _load_json(path)
        if not payload:
            continue
        rows.append(_extract_attempt_row(payload))

    def _sort_key(row: Mapping[str, Any]) -> tuple[int, int, str]:
        seq = row.get("seq")
        try:
            seq_i = int(seq)
            return (0, seq_i, str(row.get("attempt_id") or ""))
        except Exception:
            return (1, 0, str(row.get("attempt_id") or ""))

    rows = sorted(rows, key=_sort_key)
    return rows


def _print_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    if not rows:
        print("(no attempts)")
        return

    widths: Dict[str, int] = {}
    for col in columns:
        widths[col] = len(col)

    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(_to_text(row.get(col))))

    def _render(values: Mapping[str, Any]) -> str:
        parts = []
        for col in columns:
            val = _to_text(values.get(col))
            parts.append(val.ljust(widths[col]))
        return " | ".join(parts)

    header = _render({col: col for col in columns})
    line = "-+-".join("-" * widths[col] for col in columns)
    print(header)
    print(line)
    for row in rows:
        print(_render(row))


def _extract_final_summary(final_payload: Mapping[str, Any]) -> Dict[str, Any]:
    summary = final_payload.get("summary") if isinstance(final_payload.get("summary"), Mapping) else {}
    repair_result = summary.get("repair_result") if isinstance(summary.get("repair_result"), Mapping) else {}

    if repair_result:
        return {
            "passed": repair_result.get("passed"),
            "selected_variant_id": repair_result.get("selected_variant_id"),
            "decision_reason": repair_result.get("decision_reason"),
            "attempted": repair_result.get("attempted"),
        }

    return {
        "passed": summary.get("passed"),
        "selected_variant_id": summary.get("selected_variant_id"),
        "decision_reason": summary.get("decision_reason") or summary.get("decision"),
        "attempted": summary.get("attempted"),
    }


def _extract_holdout_summary(holdout_payload: Mapping[str, Any]) -> Dict[str, Any]:
    selected = holdout_payload.get("selected") if isinstance(holdout_payload.get("selected"), Mapping) else {}
    metrics = selected.get("metrics") if isinstance(selected.get("metrics"), Mapping) else {}
    top_metrics = holdout_payload.get("metrics") if isinstance(holdout_payload.get("metrics"), Mapping) else {}
    return {
        "status": holdout_payload.get("status"),
        "reason": holdout_payload.get("reason"),
        "passed": holdout_payload.get("passed"),
        "selected_variant_id": holdout_payload.get("selected_variant_id"),
        "rank_ic_min": metrics.get("rank_ic_min", top_metrics.get("rank_ic_min")),
        "coverage_min": metrics.get("coverage_min", top_metrics.get("coverage_min")),
    }


def build_report(run_dir: Path) -> Dict[str, Any]:
    run_dir = run_dir.resolve()
    attempts = _collect_attempt_rows(run_dir)
    final_payload = _load_json(run_dir / "final_result.json")
    holdout_payload = _load_json(run_dir / "holdout_check.json")
    final_summary = _extract_final_summary(final_payload)
    return {
        "run_dir": str(run_dir),
        "attempts": attempts,
        "final": final_summary,
        "holdout": _extract_holdout_summary(holdout_payload),
    }


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m alpha_core.phase2.repair")
    parser.add_argument("--run-dir", required=True, help="path to reports/p2_runs/<as_of>/<run_id>")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        print(f"[repair-report] error: run_dir not found: {run_dir}")
        return 2

    report = build_report(run_dir)

    print(f"[repair-report] run_dir={report['run_dir']}")
    print("[repair-report] attempts")
    _print_table(
        report["attempts"],
        columns=[
            "seq",
            "attempt_id",
            "factor_id",
            "variant_id",
            "bottleneck_window",
            "passed",
            "early_stopped",
            "rank_ic_min",
            "coverage_min",
            "elapsed_sec",
            "error",
        ],
    )

    final = report["final"]
    print("[repair-report] final")
    print(f"passed={_to_text(final.get('passed'))}")
    print(f"selected_variant_id={_to_text(final.get('selected_variant_id'))}")
    print(f"decision_reason={_to_text(final.get('decision_reason'))}")
    print(f"attempted={_to_text(final.get('attempted'))}")
    holdout = report.get("holdout", {})
    print("[repair-report] holdout")
    print(f"holdout_status={_to_text(holdout.get('status'))}")
    print(f"holdout_reason={_to_text(holdout.get('reason'))}")
    print(f"holdout_passed={_to_text(holdout.get('passed'))}")
    print(f"holdout_selected_variant_id={_to_text(holdout.get('selected_variant_id'))}")
    print(f"holdout_rank_ic_min={_to_text(holdout.get('rank_ic_min'))}")
    print(f"holdout_coverage_min={_to_text(holdout.get('coverage_min'))}")
    return 0


__all__ = ["main", "build_report"]
