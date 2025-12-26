# T3 beta_252d A/B

## A Section (Plan)
- Goal: stabilize rank_ic for declared WF windows (12/24) with a low-beta style signal.
- Symptoms: unstable rank_ic and coverage from return misalignment or zero-variance windows.
- Data dependencies: prices-only (adj_close/close/Close).
- Direction: higher score means lower beta (factor_value = -beta or monotonic equivalent).
- Output schema: date, stock_id, factor_value (float).

## B Section (Design Details)
- Market proxy: if no explicit market series, use equal-weight mean return of the same-day universe (prices-only).
- Return alignment: compute same-day log returns for stocks and market, aligned on date index (no calendar offset).
- Min history / min periods: window_days=252; min_obs defaults to ~0.8 * window (>=60); require min_obs for both return and beta.
- Numeric protection: price <= 0 -> NaN; replace inf with NaN; market variance <= 0 -> NaN; avoid divide-by-zero and all-NaN propagation.

## Acceptance Commands (as-of 2025-11-28)
```powershell
Set-Location C:\AI\tw-alpha-stack
$asOf = "2025-11-28"

python .\scripts\factor_engine.py `
  --root . `
  --rules .\rules_factors.yaml `
  --factors beta_252d `
  --windows 6,12,24 `
  --end $asOf

python .\scripts\factor_eval.py `
  --root . `
  --factors beta_252d `
  --windows 6,12,24 `
  --as-of $asOf

python .\scripts\factor_diag.py eval `
  --root . `
  --rules .\rules_factors.yaml `
  --factor-id beta_252d `
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

targets = ["beta_252d"]
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
