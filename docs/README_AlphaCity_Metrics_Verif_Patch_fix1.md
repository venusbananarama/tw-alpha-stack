# AlphaCity Metrics Verif Patch — fix1 (2025-09-21)

## What's fixed
- PowerShell verifier now **parses the wrapper stdout** for `metrics: <path>` and, if missing or not found on disk, **falls back** to the latest CSV in `metrics/` by `LastWriteTime`.
- Wrapper now **prints an absolute path** in the final `metrics:` line and has stronger fallback scanning (`*.csv`, `ingest*.csv`, `ingest_summary_*.csv`, `*metrics*.csv`).

## Install
1. Unzip into your repo root (e.g., `G:\AI\tw-alpha-stack\`).
2. Unblock the new PS script once:
   ```powershell
   Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
   ```

## Run
```powershell
cd G:\AI\tw-alpha-stack
.\scripts\ps\Invoke-AlphaVerification.ps1 `
  -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') `
  -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```

If metrics path is not printed by the inner script, the verifier will now pick the latest CSV under `metrics/` automatically.