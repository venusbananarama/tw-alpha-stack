from __future__ import annotations

from pathlib import Path
from typing import Dict, Mapping

import pandas as pd

from .ledger import write_json_atomic


def compose_p4_summary(
    *,
    as_of: str,
    run_id: str,
    status: str,
    reason_code: str,
    gates: Mapping[str, Dict[str, object]],
    artifacts: Mapping[str, str],
) -> Dict[str, object]:
    return {
        "as_of": as_of,
        "run_id": run_id,
        "status": status,
        "reason_code": reason_code,
        "gates": dict(gates),
        "artifacts": dict(artifacts),
    }


def _df_to_html_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    if df is None or df.empty:
        return "<p>no data</p>"
    return df.head(max_rows).to_html(index=False, border=0)


def render_drift_dashboard_html(summary_dict: Dict[str, object], tables: Dict[str, pd.DataFrame]) -> str:
    drift_table = _df_to_html_table(tables.get("drift_monthly", pd.DataFrame()))
    replay_table = _df_to_html_table(tables.get("replay_stats", pd.DataFrame()))
    gate_block = summary_dict.get("gates") or summary_dict.get("gate") or {}
    metrics_block = summary_dict.get("metrics") or {}
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Phase4 Drift Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111; }}
    h1 {{ margin-bottom: 8px; }}
    .meta {{ margin-bottom: 16px; }}
    .card {{ border: 1px solid #ddd; padding: 12px 16px; margin-bottom: 16px; border-radius: 8px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
    th {{ background: #f6f6f6; }}
  </style>
</head>
<body>
  <h1>Phase4 Drift Dashboard</h1>
  <div class="meta">
    <div>as_of: {summary_dict.get("as_of")}</div>
    <div>run_id: {summary_dict.get("run_id")}</div>
    <div>status: {summary_dict.get("status")}</div>
    <div>reason_code: {summary_dict.get("reason_code")}</div>
  </div>
  <div class="card">
    <h2>Gate Snapshot</h2>
    <pre>{gate_block}</pre>
  </div>
  <div class="card">
    <h2>Metrics Snapshot</h2>
    <pre>{metrics_block}</pre>
  </div>
  <div class="card">
    <h2>Drift (Monthly)</h2>
    {drift_table}
  </div>
  <div class="card">
    <h2>Replay Summary</h2>
    {replay_table}
  </div>
</body>
</html>
"""


def write_summary_atomic(summary: Dict[str, object], path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(summary, target)
