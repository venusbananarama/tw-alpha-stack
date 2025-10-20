[CmdletBinding()]
param(
  [string]$RootPath = "C:\AI\tw-alpha-stack",
  [string]$DataHubRoot = "datahub",
  [string[]]$Symbols = @("2330","2317"),  # 單股最小測試
  [string]$Start = "",                    # 預設自動抓（= expect_date - 1）
  [switch]$FullMarket                     # 亦可加此參數再跑一次「全市場」當天
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ProgressPreference    = "SilentlyContinue"

function Fail($m){ throw $m }

# ── 基本環境 ───────────────────────────────────────────────
if (-not (Test-Path $RootPath)) { Fail "RootPath not found: $RootPath" }
Set-Location $RootPath

$env:ALPHACITY_ALLOW = "1"
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue
$PY = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $PY)) { Fail "找不到 Python venv：$PY" }

# FINMIND 設定
if (-not $env:FINMIND_TOKEN) { Fail "FINMIND_TOKEN 未設定" }
$env:FINMIND_TOKEN = $env:FINMIND_TOKEN.Trim()
if (-not $env:FINMIND_BASE_URL -or [string]::IsNullOrWhiteSpace($env:FINMIND_BASE_URL)) {
  $env:FINMIND_BASE_URL = "https://api.finmindtrade.com/api/v4/data"
}
$env:FINMIND_BASE_URL = ($env:FINMIND_BASE_URL -replace "[^\x20-\x7E]","").Trim('"').Trim()

# ── 工具函式（代號六：臨時 .py 檔）──────────────────────────
function Normalize-Symbols {
  param([string[]]$List)
  if (-not $List) { return @() }
  $out = @()
  foreach ($item in $List) {
    if ([string]::IsNullOrWhiteSpace($item)) { continue }
    $parts = ($item -split "[,\s]+" | Where-Object { $_ -ne "" })
    foreach ($p in $parts) {
      $q = ($p -replace "\.TW$","").Trim()
      if ($q -match "^\d{4}$") { $out += $q }
    }
  }
  return ($out | Sort-Object -Unique)
}

function Test-SilverHas {
  param(
    [ValidateSet("prices","chip","per","dividend")][string]$Kind,
    [string]$Day,
    [string]$Hub = "datahub"
  )
  $code = @"
import sys, os, glob, pandas as pd
hub, kind, day = sys.argv[1], sys.argv[2], sys.argv[3]
base = os.path.join(hub, 'silver', 'alpha', kind)
cnt, mx = 0, None
for f in glob.glob(os.path.join(base, '**', '*.parquet'), recursive=True):
    try:
        df = pd.read_parquet(f, columns=['date'])
        s = df['date'].astype(str)
        cnt += int((s == day).sum())
        dmax = pd.to_datetime(s, errors='coerce').max()
        if pd.notna(dmax):
            v = dmax.date().isoformat()
            mx = v if (mx is None or v > mx) else mx
    except Exception:
        pass
print(cnt if cnt else 0, mx if mx else '')
"@
  $tmp = Join-Path $env:TEMP ("smoke_check_" + [guid]::NewGuid().ToString() + ".py")
  Set-Content -Path $tmp -Value $code -Encoding UTF8
  try {
    $raw = & $PY $tmp $Hub $Kind $Day
    $parts = ($raw -split "\s+", 2)
    return [pscustomobject]@{
      count    = [int]$parts[0]
      max_date = (($parts[1]) ? $parts[1].Trim() : "")
    }
  } finally { Remove-Item $tmp -ErrorAction SilentlyContinue }
}

# ── 取 expect_date 與 TargetDay（= expect_date - 1）───────────
& $PY .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root .
$pf = Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json
$Expect = $pf.meta.expect_date
$TargetDay = (Get-Date $Expect).AddDays(-1).ToString('yyyy-MM-dd')

if ([string]::IsNullOrWhiteSpace($Start)) { $Start = $TargetDay }
$EndExcl = (Get-Date $Start).AddDays(1).ToString('yyyy-MM-dd')

$SYMS = Normalize-Symbols -List $Symbols
Write-Host "==== Smoke 窗口  $Start .. < $EndExcl  (Target=$TargetDay; expect=$Expect)" -ForegroundColor Cyan
Write-Host "Symbols         $($SYMS -join ', ')"
Write-Host "DataHubRoot     $DataHubRoot"

# ── 最小回補：price/chip（單股）、dividend/per（全市場）──────
& $PY .\scripts\finmind_backfill.py --datasets TaiwanStockPrice                         --symbols $SYMS --start $Start --end $EndExcl --datahub-root $DataHubRoot
& $PY .\scripts\finmind_backfill.py --datasets TaiwanStockInstitutionalInvestorsBuySell --symbols $SYMS --start $Start --end $EndExcl --datahub-root $DataHubRoot
& $PY .\scripts\finmind_backfill.py --datasets TaiwanStockDividend                      --start  $Start --end $EndExcl --datahub-root $DataHubRoot
& $PY .\scripts\finmind_backfill.py --datasets TaiwanStockPER                           --start  $Start --end $EndExcl --datahub-root $DataHubRoot

# ── 銀層驗收（當日是否落地；同時列最大日）────────────────────
$kinds = "prices","chip","per","dividend"
$results = @{}
foreach ($k in $kinds) {
  $r = Test-SilverHas -Kind $k -Day $Start -Hub $DataHubRoot
  $results[$k] = $r
  $tag = if ($r.count -gt 0) { "PASS" } else { "WARN" }
  $mx  = if ($r.max_date) { $r.max_date } else { "(none)" }
  $fg  = if ($tag -eq "PASS") { "Green" } else { "Yellow" }
  Write-Host ("[{0}] {1}  has_day={2}  max_date={3}" -f $k.ToUpper(), $tag, $r.count, $mx) -ForegroundColor $fg
}

# ── 可選：同日「全市場」再跑一輪（若 -FullMarket）────────────
if ($PSBoundParameters.ContainsKey("FullMarket") -and $FullMarket) {
  Write-Host "---- Full Market round (同日) ----" -ForegroundColor Cyan
  & $PY .\scripts\finmind_backfill.py --datasets TaiwanStockPrice                         --start $Start --end $EndExcl --datahub-root $DataHubRoot
  & $PY .\scripts\finmind_backfill.py --datasets TaiwanStockInstitutionalInvestorsBuySell --start $Start --end $EndExcl --datahub-root $DataHubRoot
  & $PY .\scripts\finmind_backfill.py --datasets TaiwanStockDividend                      --start $Start --end $EndExcl --datahub-root $DataHubRoot
  & $PY .\scripts\finmind_backfill.py --datasets TaiwanStockPER                           --start $Start --end $EndExcl --datahub-root $DataHubRoot
  foreach ($k in $kinds) {
    $r = Test-SilverHas -Kind $k -Day $Start -Hub $DataHubRoot
    $tag = if ($r.count -gt 0) { "PASS" } else { "WARN" }
    $mx  = if ($r.max_date) { $r.max_date } else { "(none)" }
    Write-Host ("[FULL/{0}] {1}  has_day={2}  max_date={3}" -f $k.ToUpper(), $tag, $r.count, $mx)
  }
}

# ── 硬門檻：prices / chip 至少有當日一筆 ────────────────────
$hardFail = @()
foreach ($k in @("prices","chip")) {
  if ($results[$k].count -lt 1) { $hardFail += $k }
}
if ($hardFail.Count -gt 0) {
  Write-Error ("SMOKE FAIL: required datasets missing day {0}: {1}" -f $Start, ($hardFail -join ", "))
  exit 2
} else {
  Write-Host "SMOKE PASS (core datasets ok)." -ForegroundColor Green
  exit 0
}
