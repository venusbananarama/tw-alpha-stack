# AlphaCity v6.3 Strict Patch

**Build:** 2025-09-22T15:57:26

This package delivers a safer verification wrapper (`Invoke-AlphaVerification.ps1` + `emit_metrics_v63.py`),
scheduler helpers, integrity tools, and a stricter baseline schema.

## Install (in project root)

```powershell
cd G:\AI\tw-alpha-stack
Expand-Archive .\AlphaCity_v63_strict_patch.zip -DestinationPath . -Force

mkdir -Force .\backup\v63 > $null
Copy-Item .\scripts\ps\Invoke-AlphaVerification.ps1 .\backup\v63\ -Force -ErrorAction SilentlyContinue

Copy-Item .\pkg_v63_strict\scripts\ps\Invoke-AlphaVerification.ps1 .\scripts\ps\Invoke-AlphaVerification.ps1 -Force
Copy-Item .\pkg_v63_strict\scripts\emit_metrics_v63.py           .\scripts\emit_metrics_v63.py           -Force

Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## Run (manual)

```powershell
ack -Start '2025-09-12' -End '2025-09-12' -SkipFull -Symbol 2330.TW -Workers 6 -Qps 1.6 -CalendarCsv .\cal\trading_days.csv -VerboseCmd
```

The summary is written to `metrics\verify_summary_latest.json` with top-level fields:

- `status` (`PASS` | `PASS_NOOP` | `FAIL`)
- `reason` (`write` | `api_empty` | `end_is_non_trading_day` | `wrapper_error`)
- `rows`, `landing`, `noop`
- `results.single.parquetFiles` (list of changed files)
- `results.single.passReason`

## Optional: tasks

```powershell
.\pkg_v63_strict\tasks\Unregister-AlphaCity-VerifyTasks_v63.ps1
.\pkg_v63_strict\tasks\Register-AlphaCity-VerifyTasks_v63.ps1 -Root 'G:\AI\tw-alpha-stack'
```