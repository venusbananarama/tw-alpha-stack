# gptcodex: alpha → scripts/preflight_check.py
from __future__ import annotations
import json, sys, hashlib, pathlib, datetime as dt
from typing import Dict, Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
CFG  = ROOT / "configs" / "rules.yaml"
MANI = ROOT / "reports" / "run_manifest.json"

def sha256(p: pathlib.Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def load_yaml(p: pathlib.Path) -> Dict[str, Any]:
    import yaml
    return yaml.safe_load(p.read_text(encoding="utf-8"))

def assert_true(cond: bool, msg: str):
    if not cond:
        raise SystemExit(f"[PRECHECK_FAIL] {msg}")

def main():
    # 0) 存在性
    assert_true(CFG.exists(), "configs/rules.yaml not found")
    cfg = load_yaml(CFG)

    # 1) 綁定 SSOT 雜湊與 schema 版本
    h = sha256(CFG)
    schema_ver = cfg.get("schema_ver", "NA")

    # 2) 日曆與 as-of / 滯後檢查
    cal_path = (cfg.get("validation", {})                  .get("data", {})                  .get("calendar_path", "cal/trading_days.csv"))
    cal_file = (ROOT / cal_path)
    assert_true(cal_file.exists(), f"calendar not found: {cal_path}")
    cal_lines = cal_file.read_text(encoding="utf-8").strip().splitlines()
    assert_true(len(cal_lines) > 2500, "trading_days.csv too short")
    asof_key = cfg.get("asof", {}).get("weekly_anchor", "W-FRI")
    assert_true(asof_key in ("W-FRI","W-THU"), f"invalid weekly anchor: {asof_key}")
    embargo = int(cfg.get("validation", {}).get("cv", {}).get("embargo_days", 5))
    assert_true(embargo >= 5, "embargo_days < 5")

    # 3) Gate 參數存在性與下限
    acc = cfg.get("acceptance", {})
    assert_true(acc.get("wf_pass_ratio_min", 0) >= 0.80, "wf_pass_ratio_min < 0.80")
    assert_true(acc.get("dsr_min_after_costs", -1) >= 0.0, "DSR_after_costs gate missing or < 0")
    assert_true(cfg.get("leverage_cap", 1.0) <= 1.3, "leverage_cap > 1.3 not allowed")

    # 4) 產出 run_manifest（不覆蓋專案，僅寫入 reports）
    MANI.parent.mkdir(parents=True, exist_ok=True)
    MANI.write_text(json.dumps({
        "ts": dt.datetime.utcnow().isoformat()+"Z",
        "ssot_hash": h,
        "schema_ver": schema_ver,
        "rules_path": str(CFG),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[PRECHECK_OK] ssot_hash={h[:12]} schema_ver={schema_ver}")

if __name__ == "__main__":
    main()
