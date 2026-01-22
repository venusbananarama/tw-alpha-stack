param(
  [string]$Symbol = "2330",
  [string]$OutDir = "datahub\bronze\fubon\trades",
  [int]$StatusInterval = 30
)

$ErrorActionPreference = "Stop"

# repo root
$Repo = Split-Path -Parent $PSScriptRoot   # ...\tools\fubon -> ...\tools
$Repo = Split-Path -Parent $Repo           # ...\tools -> repo root
Set-Location $Repo

# log：先建好，任何錯誤都會落在這裡
$logDir = Join-Path $Repo "reports\fubon_recorder"
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir ("task_" + (Get-Date -Format "yyyy-MM-dd_HHmmss") + "_$Symbol.log")

function Write-Log([string]$s){
  $s | Add-Content -Encoding UTF8 -LiteralPath $log
}

Write-Log ("START " + (Get-Date -Format o))
Write-Log ("user=" + "$env:USERDOMAIN\$env:USERNAME")
Write-Log ("repo=" + $Repo)
Write-Log ("psver=" + $PSVersionTable.PSVersion.ToString())
Write-Log ("arch=" + $env:PROCESSOR_ARCHITECTURE)

try {
  $PY = Join-Path $Repo ".venv_trade\Scripts\python.exe"
  if(-not (Test-Path -LiteralPath $PY)){ throw "python not found: $PY" }
  Write-Log ("PY=" + $PY)

  # 用常數路徑，不再依賴 $pfxRoot 變數（避免你遇到的「變數莫名變空」）
  $pfxRootExists = Test-Path -LiteralPath 'C:\CAFubon'
  Write-Log ("Test-Path C:\CAFubon = " + $pfxRootExists)
  if(-not $pfxRootExists){ throw "pfx root not found: C:\CAFubon" }

  $all = Get-ChildItem -LiteralPath 'C:\CAFubon' -Recurse -File -Filter *.pfx -ErrorAction SilentlyContinue
  Write-Log ("pfx_count=" + $all.Count)

  $pfx = $all | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if(-not $pfx){ throw "Cannot find any .pfx under C:\CAFubon" }

  $env:FUBON_ID        = Split-Path (Split-Path $pfx.FullName -Parent) -Leaf
  $env:FUBON_CERT_PATH = $pfx.FullName
  $env:PYTHONUNBUFFERED = "1"

  Write-Log ("FUBON_ID=" + $env:FUBON_ID)
  Write-Log ("FUBON_CERT_PATH=" + $env:FUBON_CERT_PATH)

  # 讓 python stdout/stderr 都進 log（避免排程閃一下就消失你看不到原因）
  & $PY "tools\fubon\record_trades_ndjson.py" --symbol $Symbol --out $OutDir --use-keyring --status-interval $StatusInterval *>> $log

  Write-Log ("END " + (Get-Date -Format o) + " exit_code=" + $LASTEXITCODE)
  exit $LASTEXITCODE
}
catch {
  Write-Log ("ERROR " + (Get-Date -Format o))
  ($_ | Out-String) | Add-Content -Encoding UTF8 -LiteralPath $log
  exit 1
}
