<#
.SYNOPSIS
  FinMind 全市場增量（Backfill）— 嚴謹修正版

.DESCRIPTION
  以安全參數化方式呼叫 scripts\finmind_backfill.py。
  支援參數：-Start -End -Datasets[] -Universe -Workers -Qps -VerboseCmd
  不含 ResumeLog（你的 finmind_backfill.py 不支援）。
#>

param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
  [string]$Start,

  [Parameter(Mandatory = $true)]
  [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
  [string]$End,

  [string[]]$Datasets = @('TaiwanStockPrice','TaiwanStockInstitutionalInvestorsBuySell'),

  [ValidateSet('TSE','OTC','All')]
  [string]$Universe = 'TSE',

  [ValidateRange(1,128)]
  [int]$Workers = 6,

  [double]$Qps = 1.6,

  [switch]$VerboseCmd
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 0) 專案路徑與 Python 解析
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot   = Resolve-Path (Join-Path $ScriptRoot '..\..')
$PyBackfill = Join-Path $RepoRoot 'scripts\finmind_backfill.py'

if (-not (Test-Path $PyBackfill)) {
  throw "找不到 backfill 腳本：$PyBackfill"
}

# 找 python 可執行檔（優先 venv，否則系統 python）
$PythonCmd = 'python'
if (-not (Get-Command $PythonCmd -ErrorAction SilentlyContinue)) {
  $VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (Test-Path $VenvPy) { $PythonCmd = $VenvPy }
}
if (-not (Get-Command $PythonCmd -ErrorAction SilentlyContinue)) {
  throw "找不到可執行的 python，請確認 PATH 或使用 venv。"
}

# 1) 參數檢查
if (-not $Datasets -or $Datasets.Count -eq 0) {
  throw "Datasets 不可為空，至少提供 1 個資料集名稱。"
}

# 2) 組命令列參數
$argv = @(
  $PyBackfill,
  '--start',   $Start,
  '--end',     $End,
  '--universe', $Universe,
  '--workers',  $Workers,
  '--qps',      $Qps,
  '--datasets'
)
$argv += $Datasets   # 多個 dataset 逐一附加

# 3) 顯示命令（僅供診斷）
if ($VerboseCmd) {
  $disp = $argv | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }
  Write-Host ("{0} {1}" -f $PythonCmd, ($disp -join ' ')) -ForegroundColor Yellow
}

# 4) 執行
& $PythonCmd @argv
if ($LASTEXITCODE -ne 0) {
  throw "finmind_backfill.py 返回非 0：$LASTEXITCODE"
}

Write-Host "Backfill 完成。" -ForegroundColor Green
