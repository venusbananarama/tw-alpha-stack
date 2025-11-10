# Phase-2 Factor Tools (No wrappers, single Gate)

## Entry points
- `tools\factors\eval\Run-FactorEval.ps1`
- `tools\factors\corr\Run-FactorCorr.ps1`
- `tools\factors\combo\Run-FactorCombo.ps1`
- `tools\factors\status\Show-FactorStatus.ps1`

## Typical flow
```
pwsh -NoProfile -File .\tools\factors\eval\Run-FactorEval.ps1 -Date 2025-11-07 -Families tech,chip
pwsh -NoProfile -File .\tools\factors\corr\Run-FactorCorr.ps1 -Date 2025-11-07 -Families tech,chip
pwsh -NoProfile -File .\tools\factors\combo\Run-FactorCombo.ps1 -Date 2025-11-07 -TopN 20 -MaxWeightPct 0.05 -GuardAdv 5
pwsh -NoProfile -File .\tools\gate\Run-WFGate.ps1 -WFDir .\tools\gate\wf_configs
```
