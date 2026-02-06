# T1 mom_12m A/B

## A Section (Plan)
- Goal: strict 12M lookback + 1M skip using trading-day shift; stronger momentum -> higher score.
- Symptoms: misaligned shift/rolling order, nondeterministic dedup/sort, or weak min_history gating.
- Data dependencies: prices-only (adj_close/close/Close).
- Direction: higher factor_value means stronger momentum.
- Output schema: date, stock_id, factor_value (float).

## B Section (Design Details)
- Strict definition: per stock, use shift(skip_td) and shift(skip_td + lookback_td) on price (trading days, not calendar).
- Skip strategy: skip_td = 21 trading days; lookback_td = 252 trading days; no calendar offset or forward-fill.
- Min history: require at least skip_td + lookback_td + 1 unique trading days; otherwise output NaN (no row).
- Dedup/sort: sort by stock_id/date with stable order; drop duplicates deterministically before shift.
- Missing values: price <= 0 -> NaN; if any required price is NaN, factor_value is NaN; avoid look-ahead.

## Acceptance Commands (as-of 2025-11-28)
```powershell
Set-Location C:\AI\tw-alpha-stack
$asOf = "2025-11-28"

python .\scripts\p2\factor_engine.py `
  --root . `
  --rules .\rules_factors.yaml `
  --factors mom_12m `
  --windows 6,12,24 `
  --end $asOf

python .\scripts\p2\factor_eval.py `
  --root . `
  --factors mom_12m `
  --windows 6,12,24 `
  --as-of $asOf

python .\scripts\p2\factor_diag.py eval `
  --root . `
  --rules .\rules_factors.yaml `
  --factor-id mom_12m `
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

targets = ["mom_12m"]
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

