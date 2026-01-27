from __future__ import annotations

from typing import Dict, List, TypedDict

P5_SCHEMA_VERSION = "phase5.v1.0"

TARGET_PORTFOLIO_COLUMNS: List[str] = ["symbol", "target_qty", "strategy_id"]


class ArtifactNames:
    P5_SUMMARY_JSON = "p5_summary.json"
    P5_RUN_LOG = "p5_run.log"
    STRATEGY_POOL_JSON = "strategy_pool.json"
    STRATEGY_CORR_FILE = "strategy_corr.csv"
    DECISION_TRACE_JSON = "decision_trace.json"
    STRATEGY_ALLOC_CSV = "strategy_alloc.csv"
    TARGET_PORTFOLIO_CSV_FMT = "target_portfolio_{as_of}.csv"


class ResolvedPaths(TypedDict):
    root: str
    as_of: str
    out_dir: str
    universe_path: str | None
    prices_path: str
    reports_target_path: str
    out_target_path: str


class P5Summary(TypedDict):
    schema_version: str
    as_of: str
    run_id: str
    profile: str
    status: str
    reason_code: str
    exit_code: int
    created_at: str
    resolved_paths: ResolvedPaths
    gates: Dict[str, object]
    artifacts: Dict[str, str]
    notes: Dict[str, object] | None
