
# AlphaCity Metrics Verif Patch — fix3 + Quick (2025-09-21)

## What's inside
- `scripts/ps/Invoke-AlphaVerification.ps1` — sync streaming with 30s heartbeat, `-Quick` mode.
- `scripts/emit_metrics_wrapper.py` — always emits absolute `metrics:` line; robust fallback to latest CSV under `metrics/`.

## Install
```powershell
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250921_fix3_quick.zip -DestinationPath G:\AI\tw-alpha-stack -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## Usage
- Full run (2015–today):
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```
- Quick run (last 1y market, last 30d single-stock):
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```
