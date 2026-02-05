from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

from .contracts import FactorStatus, now_iso
from . import paths


def _extract_yyyymm(name: str) -> Optional[str]:
    if not name.startswith("yyyymm="):
        return None
    value = name.replace("yyyymm=", "").strip()
    if len(value) != 6 or not value.isdigit():
        return None
    return value


def _latest_partition(factor_dir: Path) -> Optional[str]:
    if not factor_dir.exists():
        return None
    latest: Optional[str] = None
    for p in factor_dir.iterdir():
        if not p.is_dir():
            continue
        yyyymm = _extract_yyyymm(p.name)
        if not yyyymm:
            continue
        if latest is None or yyyymm > latest:
            latest = yyyymm
    return latest


def _has_any_parquet(factor_dir: Path) -> bool:
    if not factor_dir.exists():
        return False
    for p in factor_dir.glob("yyyymm=*/*.parquet"):
        if p.is_file():
            return True
    return False


def build_factor_status(
    root: Path,
    factor_defs: Mapping[str, object],
    engine: str,
) -> List[FactorStatus]:
    root = root.resolve()
    statuses: List[FactorStatus] = []
    factor_root = paths.factor_root(root)

    for fid in sorted(factor_defs.keys()):
        factor_dir = factor_root / fid
        has_data = _has_any_parquet(factor_dir)
        latest = _latest_partition(factor_dir)
        eval_path = root / "reports" / "factor_eval" / f"{fid}_summary.json"
        has_eval = eval_path.is_file()
        statuses.append(
            FactorStatus(
                factor_id=fid,
                engine=engine,
                has_data=has_data,
                has_eval=has_eval,
                latest_partition=latest,
                eval_path=eval_path if has_eval else None,
            )
        )
    return statuses


def write_status_file(
    path: Path,
    *,
    as_of: str,
    engine: str,
    profile: str,
    statuses: Iterable[FactorStatus],
) -> None:
    items = [s.to_dict() for s in statuses]
    total = len(items)
    with_data = sum(1 for s in statuses if s.has_data)
    with_eval = sum(1 for s in statuses if s.has_eval)
    payload = {
        "schema": "phase2_status.v1",
        "as_of": as_of,
        "engine": engine,
        "profile": profile,
        "generated": now_iso(),
        "counts": {
            "total": total,
            "with_data": with_data,
            "with_eval": with_eval,
        },
        "factors": items,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
