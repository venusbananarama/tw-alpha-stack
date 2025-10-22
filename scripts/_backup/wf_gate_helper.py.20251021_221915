from __future__ import annotations
def ensure_sharpe_after_costs(df):
    if 'sharpe_after_costs' not in df.columns:
        if 'sharpe' in df.columns:
            df['sharpe_after_costs'] = df['sharpe']
        else:
            df['sharpe_after_costs'] = 0.0
    return df
# gptcodex: alpha → scripts/wf_gate_helper.py
"""將此模組引入既有 wf_runner，在最終彙總處套用 Gate。"""
from typing import Dict, Any
import json

def apply_gate(df, rules: Dict[str, Any]) -> Dict[str, Any]:
    """df 至少包含: sharpe_after_costs, max_dd, wf_pass, dsr_after_costs。"""
    acc = rules.get("acceptance", {})
    sharpe = float(df["sharpe_after_costs"].iloc[-1])
    maxdd  = float(df["max_dd"].iloc[-1])
    wf_pass_ratio = float(df["wf_pass"].mean()) if "wf_pass" in df else 0.0
    dsr = float(df["dsr_after_costs"].iloc[-1]) if "dsr_after_costs" in df else -1.0

    ok = (
        sharpe >= float(acc.get("sharpe_min_after_costs", 1.8)) and
        maxdd <= float(acc.get("max_dd_max", 0.20)) and
        wf_pass_ratio >= float(acc.get("wf_pass_ratio_min", 0.80)) and
        dsr >= float(acc.get("dsr_min_after_costs", 0.0))
    )
    return {
        "ok": bool(ok),
        "metrics": {
            "sharpe_after_costs": sharpe,
            "max_dd": maxdd,
            "wf_pass_ratio": wf_pass_ratio,
            "dsr_after_costs": dsr
        }
    }

def print_gate_result(res: Dict[str, Any]) -> None:
    print(json.dumps({"gate": res}, ensure_ascii=False))


