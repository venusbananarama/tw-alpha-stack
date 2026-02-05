from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _datahub_root(repo_root: Path) -> Path:
    return repo_root / "datahub"


def _quarantine_root(datahub_root: Path) -> Path:
    return datahub_root / "silver" / "alpha" / "_quarantine"


def _is_yyyymm_dir(name: str) -> bool:
    return name.startswith("yyyymm=") and len(name) == len("yyyymm=YYYYMM")


def _collect_actions(
    datahub_root: Path,
    datasets: List[str],
) -> Dict[str, List[Dict[str, str]]]:
    actions: Dict[str, List[Dict[str, str]]] = {
        "moves": [],
        "warnings": [],
        "notes": [],
    }

    silver_root = datahub_root / "silver" / "alpha"
    price_legacy = silver_root / "price"
    price_canonical = silver_root / "prices"

    if price_legacy.is_dir():
        for item in price_legacy.rglob("*"):
            if item.is_dir():
                continue
            rel = item.relative_to(price_legacy)
            dest = price_canonical / rel
            actions["moves"].append(
                {
                    "reason": "price_to_prices",
                    "src": str(item),
                    "dst": str(dest),
                }
            )

    for ds in datasets:
        root = silver_root / ds
        if not root.is_dir():
            continue
        for part in root.iterdir():
            if not part.is_dir():
                continue
            if not _is_yyyymm_dir(part.name):
                actions["warnings"].append(
                    {
                        "dataset": ds,
                        "issue": "non_yyyymm_partition",
                        "path": str(part),
                    }
                )
                continue

            parquet_files = sorted(part.glob("*.parquet"))
            if not parquet_files:
                continue

            if ds in ("prices", "chip", "per", "dividend"):
                for f in parquet_files:
                    if f.name == "data.parquet":
                        continue
                    if f.name.startswith("ing_"):
                        actions["warnings"].append(
                            {
                                "dataset": ds,
                                "issue": "ing_parquet_detected",
                                "path": str(f),
                            }
                        )
                        continue
                    qroot = _quarantine_root(datahub_root) / ds
                    qdst = qroot / part.name / f.name
                    actions["moves"].append(
                        {
                            "reason": "hhf_non_canonical_parquet",
                            "src": str(f),
                            "dst": str(qdst),
                        }
                    )
            else:
                prefix = f"{ds}_"
                for f in parquet_files:
                    if not f.name.startswith(prefix):
                        actions["moves"].append(
                            {
                                "reason": "hhd_non_canonical_parquet",
                                "src": str(f),
                                "dst": str(_quarantine_root(datahub_root) / ds / part.name / f.name),
                            }
                        )

    return actions


def _apply_moves(moves: List[Dict[str, str]]) -> List[Dict[str, str]]:
    applied: List[Dict[str, str]] = []
    for move in moves:
        src = Path(move["src"])
        dst = Path(move["dst"])
        if not src.exists():
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            fallback = dst.with_suffix(dst.suffix + ".dup")
            dst = fallback
        shutil.move(str(src), str(dst))
        applied.append({**move, "dst": str(dst)})
    return applied


def run_migration(
    repo_root: Path,
    run_id: str,
    apply: bool,
    dry_run: bool,
) -> Dict[str, str]:
    datahub_root = _datahub_root(repo_root)
    datasets = [
        "prices",
        "chip",
        "per",
        "dividend",
        "shareholding",
        "inst_total",
        "gov_bank",
    ]
    actions = _collect_actions(datahub_root, datasets)
    applied: List[Dict[str, str]] = []
    if apply and not dry_run:
        applied = _apply_moves(actions["moves"])

    report = {
        "run_id": run_id,
        "generated_at": _now_iso(),
        "repo_root": str(repo_root),
        "datahub_root": str(datahub_root),
        "dry_run": bool(dry_run),
        "apply": bool(apply),
        "planned_moves": actions["moves"],
        "warnings": actions["warnings"],
        "notes": actions["notes"],
        "applied_moves": applied,
    }

    out_dir = repo_root / "reports" / "phase1_runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "migrate_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return {"report_path": str(report_path)}


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Phase-1 layout migration (dry-run by default).")
    ap.add_argument("--run-id", default=datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    return ap.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = _repo_root()
    dry_run = True
    if args.apply:
        dry_run = False
    if args.dry_run:
        dry_run = True
    run_migration(
        repo_root=repo_root,
        run_id=args.run_id,
        apply=bool(args.apply),
        dry_run=bool(dry_run),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
