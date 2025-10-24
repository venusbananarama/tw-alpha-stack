param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest
$pwsh = Join-Path $PSHOME 'pwsh.exe'
if(-not (Test-Path $pwsh)){ $pwsh = (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source }
if(-not $pwsh){ $pwsh = (Get-Command powershell -ErrorAction SilentlyContinue)?.Source }
if(-not $pwsh){ Write-Error "No pwsh/powershell found in PATH or PSHOME"; exit 1 }
& $pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\gate\Run-WFGate.ps1" @Rest
