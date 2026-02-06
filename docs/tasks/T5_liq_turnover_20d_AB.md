# T5 liq_turnover_20d A/B

## A Section (Purpose / Symptoms / Dependencies / Direction / Output)
- Purpose: fix microstructure_v1 liq_turnover_20d so run_liquidity_factor loads, factor_engine is not skipped, and outputs the canonical schema for eval/compose.
- Symptoms: run_liquidity_factor is None due to liq_impl import error (cannot import winsorize_by_quantile from alpha_core.phase2.corelib.factor_xform), leading to not implemented / skipped.
- Data dependencies: rules specify inputs = ["prices", "shareholding"]; turnover uses prices columns (turnover_rate or turnover value) with proxy fallback.
- Direction: direction=illiquid with transform=log1p; lower turnover implies higher score after cross-sectional transforms.
- Output schema: date (datetime64), stock_id (str), factor_value (float).

## B Section (Design / API / Numeric Guards / Extremes)
- Root cause fix: choose option 1 (liq_impl only) by removing winsorize_by_quantile import and using winsorize_xsection row-wise; keep pipeline order.
- Turnover priority: turnover_rate / Turnover_rate / turnoverRatio / turnover_ratio, then turnover / Trading_turnover / turnover_value / total_turnover, then close*volume or adj_close*volume proxy.
- 20d smoothing: rolling mean per stock with window=20 and min_periods=max(1, window//2).
- Transform guard: log1p/log/inv requires liq > 0; non-positive -> NaN to avoid inf.
- Winsor / neutralize / zscore order: pivot to wide, winsor row-wise, neutralize by size if injected, zscore row-wise, stack to long, apply illiquid sign fix (skip double-invert when transform is inverse).

## Acceptance Commands (as-of 2025-11-28)
```powershell
Set-Location C:\AI\tw-alpha-stack
$asOf = "2025-11-28"

python .\scripts\p2\factor_engine.py `
  --root . `
  --rules .\rules_factors.yaml `
  --factors liq_turnover_20d `
  --windows 6,12,24 `
  --end $asOf

python .\scripts\p2\factor_eval.py `
  --root . `
  --factors liq_turnover_20d `
  --windows 6,12,24 `
  --as-of $asOf

python .\scripts\p2\factor_diag.py eval `
  --root . `
  --rules .\rules_factors.yaml `
  --factor-id liq_turnover_20d `
  --windows 6,12,24 `
  --min-days 60

python .\scripts\compose_factors_to_wf.py `
  --root . `
  --rules-file .\rules_factors.yaml `
  --wf-summary .\reports\wf_summary.json `
  --factor-eval-dir .\reports\factor_eval `
  --wf-windows 6 12 24 `
  --mode all `
  --slo-profile live `
  --slo-engine classic

@'
import json
from pathlib import Path

wf = json.loads(Path("reports/wf_summary.json").read_text(encoding="utf-8"))
F = wf.get("factors", {}) or {}
passed = set((F.get("passed", {}) or {}).keys())
cands = set((F.get("candidates", {}) or {}).keys())

targets = ["liq_turnover_20d"]
print("PASSED =", sorted(passed))
print("CANDS  =", sorted(cands))

ok = True
for t in targets:
    where = "passed" if t in passed else ("candidates" if t in cands else "missing")
    print(f"{t}: {where}")
    if where != "passed":
        ok = False

print("\nALL_TARGETS_PASSED =", ok)
'@ | python -
```

