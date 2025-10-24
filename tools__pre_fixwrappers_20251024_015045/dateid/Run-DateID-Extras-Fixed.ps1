#requires -Version 7.0
<#
Run-DateID-Extras (fixed): non-interactive orchestrator to call fm_dateid_fetch.py by Date + IDs.
- Sets --end = Date + 1 (exclusive).
- IDs from -IDs "2330,2317" or -IDsFile (default: .\configs\investable_universe.txt).
- Group A = TaiwanStockPrice; Group B = TaiwanStockInstitutionalInvestorsBuySell; All = both.
- Uses .\.venv\Scripts\python.exe when available; falls back to system "python".
- Log: .\reports\Run-DateID-Extras_YYYYMMDD.log
#>

[CmdletBinding()]
param(
    [ValidateSet('A','B','All')]
    [string]$Group = 'All',
    [string]$IDs = '',
    [string]$IDsFile = '.\configs\investable_universe.txt',
    [datetime]$Date = (Get-Date).Date,
    [int]$Retries = 3,
    [string]$DataHubRoot = '.\datahub',
    [int]$RPM = 25,
    [string]$Python = '.\.venv\Scripts\python.exe'
)

$ErrorActionPreference = 'Stop'
$env:ALPHACITY_ALLOW = '1'
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue

# 找 Python
if (-not (Test-Path $Python)) {
    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd) { $Python = $pyCmd.Source } else { throw "Python interpreter not found." }
}

# 找 fm_dateid_fetch.py（優先 scripts\）
$candidates = @('.\scripts\fm_dateid_fetch.py', '.\tools\fm_dateid_fetch.py', '.\fm_dateid_fetch.py')
$Fetcher = $null
foreach ($c in $candidates) { if (Test-Path $c) { $Fetcher = $c; break } }
if (-not $Fetcher) { throw "fm_dateid_fetch.py not found. Checked: $($candidates -join ', ')" }

# IDs：字串或檔案
$idsList = @()
if ($IDs -and $IDs.Trim()) {
    $idsList = $IDs.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
} elseif (Test-Path $IDsFile) {
    $idsList = Get-Content -LiteralPath $IDsFile | ForEach-Object { $_.Trim() } | Where-Object { $_ -and ($_ -notmatch '^\s*(#|$)') }
} else {
    throw "No IDs provided. Use -IDs '2330,2317' or -IDsFile '.\configs\investable_universe.txt'."
}
$idsList = $idsList | Select-Object -Unique
$symbolsArg = [string]::Join(',', $idsList)

# 日期：--end 不含 → +1 日
$start = $Date.Date
$end   = $start.AddDays(1)
$startS = $start.ToString('yyyy-MM-dd')
$endS   = $end.ToString('yyyy-MM-dd')

# 組別
$datasetsA = @('TaiwanStockPrice')
$datasetsB = @('TaiwanStockInstitutionalInvestorsBuySell')
switch ($Group) { 'A' { $datasets=$datasetsA } 'B' { $datasets=$datasetsB } default { $datasets=$datasetsA+$datasetsB } }

# 日誌
if (-not (Test-Path .\reports)) { New-Item -ItemType Directory -Force -Path .\reports | Out-Null }
$logFile = ".\reports\Run-DateID-Extras_{0}.log" -f $start.ToString('yyyyMMdd')
"==== Run-DateID-Extras (Fixed) ====" | Tee-Object -FilePath $logFile
"Date=$startS (end=$endS excl)  Group=$Group  N_IDS=$($idsList.Count)" | Tee-Object -FilePath $logFile -Append
"Python=$Python  Fetcher=$Fetcher" | Tee-Object -FilePath $logFile -Append

# 執行
$globalOk = $true
foreach ($ds in $datasets) {
    ">> Dataset=$ds" | Tee-Object -FilePath $logFile -Append
    $pyArgs = @(
        $Fetcher, '--dataset', $ds, '--id-key', 'stock_id', '--symbols', $symbolsArg,
        '--start', $startS, '--end', $endS, '--datahub-root', $DataHubRoot,
        '--timeout', '60', '--max-retries', "$Retries", '--rpm', "$RPM"
    )
    & $Python $pyArgs 2>&1 | Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) { "!! FAILED dataset=$ds exit=$LASTEXITCODE" | Tee-Object -FilePath $logFile -Append; $globalOk = $false } else { "OK dataset=$ds" | Tee-Object -FilePath $logFile -Append }
}
if ($globalOk) { "[{0}] DONE (Group={1})." -f (Get-Date -Format s), $Group | Tee-Object -FilePath $logFile -Append; exit 0 }
else { "[{0}] COMPLETED WITH ERRORS." -f (Get-Date -Format s) | Tee-Object -FilePath $logFile -Append; exit 1 }
