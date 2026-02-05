# Fubon sidecar tools

This folder provides a minimal sidecar pipeline:

1) login_check.py - login validation
2) record_trades_ndjson.py - record trades channel into NDJSON
3) replay_ndjson.py - replay NDJSON into 1m bars
4) mock_exec_ledger.py - generate a mock ledger from bars

NDJSON schema (one JSON per line):
{
  "ingest_ts": "<ISO8601 with tz>",
  "source": "fubon_neo",
  "event": "trade",
  "symbol": "<symbol>",
  "dedup_key": "<symbol>|<trade_time>|<serial>",
  "data": { ... }
}

Notes:
- Secrets are always read from getpass; use --echo only for local debug.
- Parquet-only output; pyarrow is required.
- Symbol must match investable_universe.txt (no .TW suffix).

Dependencies:
- FubonSDK (fubon_neo wheel)
- pyarrow

Examples (PowerShell):
$PY = "C:\AI\tw-alpha-stack\.venv_trade\Scripts\python.exe"

& $PY tools\fubon\login_check.py

& $PY tools\fubon\record_trades_ndjson.py --symbol 2330 --out datahub\bronze\fubon\trades --rotate daily --mode Speed

& $PY tools\fubon\replay_ndjson.py --input tools\fubon\samples\trades.sample.ndjson --out reports\fubon_replay --tz Asia/Taipei --bar 1m

& $PY tools\fubon\mock_exec_ledger.py --bars reports\fubon_replay\bars_1m.parquet --out reports\fubon_ledger --side buy --qty 1000

No wrapper scheduled task (PowerShell 5.1, absolute paths):
```powershell
$Repo = "C:\AI\tw-alpha-stack"
$Pyw = Join-Path $Repo ".venv_trade\Scripts\pythonw.exe"
if(-not (Test-Path -LiteralPath $Pyw)){
  $Pyw = Join-Path $Repo ".venv_trade\Scripts\python.exe"
}

$Script = Join-Path $Repo "tools\fubon\record_trades_ndjson.py"
$OutDir = Join-Path $Repo "datahub\bronze\fubon\trades"
$LogDir = Join-Path $Repo "reports\fubon_recorder"
$TradingDays = Join-Path $Repo "datahub\ref\trading_days.csv"

$argLine = [string]("$Script --symbol 2330 --out $OutDir --use-keyring --status-interval 30 --log-dir $LogDir --only-trading-day --trading-days-csv $TradingDays")

$Action = New-ScheduledTaskAction -Execute $Pyw -Argument $argLine
$Trigger = New-ScheduledTaskTrigger -Daily -At 8:30am
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 6 -Minutes 5)
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive

Register-ScheduledTask -TaskName "FubonRecorder2330" -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal
```

Note:
- LogonType must be Interactive for keyring access.
- ExecutionTimeLimit 6:05 matches the 08:30-14:35 window.
- Scheduling Mon-Fri 08:30 is still recommended; with --only-trading-day the task exits 0 on holidays.
- Do not use `$args` in PowerShell snippets; use `$argLine` and cast to `[string]`.

When Task Result is non-zero, check:
`reports\fubon_recorder\record_YYYY-MM-DD_2330.log`
The startup header includes sys.executable, cwd, script path, argv, and key env presence to diagnose launch failures.

Executable drift guard (venv enforcement):
- If the log shows `executable_check=DRIFT`, the task is using the wrong python.
- Task Scheduler Action Execute must point to: `C:\AI\tw-alpha-stack\.venv_trade\Scripts\python.exe`
- Do not use system python/pythonw.

Verified fix snippet (PowerShell 5.1):
```powershell
$Repo = "C:\AI\tw-alpha-stack"
$Py = Join-Path $Repo ".venv_trade\Scripts\python.exe"
$Script = Join-Path $Repo "tools\fubon\record_trades_ndjson.py"
$OutDir = Join-Path $Repo "datahub\bronze\fubon\trades"
$LogDir = Join-Path $Repo "reports\fubon_recorder"
$TradingDays = Join-Path $Repo "datahub\ref\trading_days.csv"

$argLine = [string]("$Script --symbol 2330 --out $OutDir --use-keyring --status-interval 30 --log-dir $LogDir --only-trading-day --trading-days-csv $TradingDays")

$Action = New-ScheduledTaskAction -Execute $Py -Argument $argLine
Set-ScheduledTask -TaskName "P1_MD_Fubon_2330_0830_1435" -TaskPath "\AlphaCity\Phase1_MarketData\Fubon\" -Action $Action
```
