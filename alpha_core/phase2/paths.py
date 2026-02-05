from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from alpha_core import dates as date_lib


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ensure_root(root: Optional[Path]) -> Path:
    return (root or repo_root()).resolve()


def rules_factors_path(root: Optional[Path] = None) -> Path:
    return _ensure_root(root) / "rules_factors.yaml"


def datahub_root(root: Optional[Path] = None) -> Path:
    return _ensure_root(root) / "datahub"


def factor_root(root: Optional[Path] = None) -> Path:
    return datahub_root(root) / "silver" / "alpha" / "factor"


def reports_root(root: Optional[Path] = None) -> Path:
    return _ensure_root(root) / "reports"


def phase2_root(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "p2"


def status_path(root: Optional[Path], as_of: str, engine: str, profile: str) -> Path:
    return phase2_root(root) / f"status.{as_of}.{engine}.{profile}.json"


def plan_path(root: Optional[Path], as_of: str, engine: str, profile: str) -> Path:
    return phase2_root(root) / f"plan.{as_of}.{engine}.{profile}.json"


def corr_summary_path(root: Optional[Path], as_of: str, engine: str, profile: str) -> Path:
    return phase2_root(root) / f"corr.{as_of}.{engine}.{profile}.json"


def evidence_dir(root: Optional[Path], run_id: str) -> Path:
    return phase2_root(root) / "evidence" / run_id


def wf_summary_path(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "wf_summary.json"


def gate_summary_path(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "gate_summary.json"


def p1_gate_summary_path(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "p1" / "gate_summary.json"


def pass_results_path(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "pass_results.csv"


def fail_results_path(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "fail_results.csv"


def factor_eval_dir(root: Optional[Path] = None) -> Path:
    return reports_root(root) / "factor_eval"


def default_as_of_date(today: Optional[date] = None) -> date:
    d = today or date.today()
    return date_lib.get_weekly_friday(d)


def format_as_of(as_of: date) -> str:
    return as_of.isoformat()
