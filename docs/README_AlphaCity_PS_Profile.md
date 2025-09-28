# AlphaCity PowerShell Shortcuts (CD / venv / Verify / Backfill)

## Install into your PowerShell profile
1. Open your profile for all hosts:
   ```powershell
   notepad $PROFILE.CurrentUserAllHosts
   ```
2. Paste the content of `AlphaCity.Profile.ps1` into the file, save.
3. Restart Terminal.

> If your repo root isn't `G:\AI\tw-alpha-stack`, set your own path first:
> ```powershell
> $Env:TW_ALPHA_ROOT = 'D:\path\to\tw-alpha-stack'
> ```

## Commands
- `acd` or `ac` — cd to project root, or `ac scripts` to jump to subfolder.
- `acopen` — open project in Explorer.
- `acvenv` — activate venv.
- `acp` / `acpy` — run Python from venv (UTF-8, arg passthrough).
- `ack` / `accheck` — run verification. Examples:
  ```powershell
  ack -Quick -Symbol 2330.TW -Workers 6 -Qps 1.8
  ack -Symbol 2330.TW -Workers 6 -Qps 1.6  # full (2015–today)
  ```
- `acb` / `acfm` — FinMind backfill via wrapper. Examples:
  ```powershell
  acb --start 2015-01-01 --end (Get-Date).ToString('yyyy-MM-dd') `
      --datasets TaiwanStockPrice --universe TSE `
      --workers 6 --qps 1.6 --hourly-cap 6000

  acb --start (Get-Date).AddDays(-30).ToString('yyyy-MM-dd') `
      --end (Get-Date).ToString('yyyy-MM-dd') `
      --symbols 2330 `
      --datasets TaiwanStockPrice TaiwanStockInstitutionalInvestorsBuySell `
      --workers 2 --qps 1
  ```