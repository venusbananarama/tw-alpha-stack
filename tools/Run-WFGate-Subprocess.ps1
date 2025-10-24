# Subprocess Gate Runner (stable)
param(
  [string]$Dir = $(Resolve-Path .\runs\wf_configs).Path,
  [string]$Log = $(Join-Path (Resolve-Path .\reports) ("nightly_{0}.log" -f (Get-Date -Format 'yyyy-MM-dd')))
)
$ErrorActionPreference = 'Stop'
$env:ALPHACITY_ALLOW = '1'
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue

$target = Join-Path $PSScriptRoot 'gate\Run-WFGate.ps1'
if(-not (Test-Path $target)){ throw "Missing $target" }

$args2 = @('-NoProfile','-ExecutionPolicy','Bypass','-File', $target, '-Dir', $Dir)

# 記錄輸出（若 $Log 為空字串就直接輸出到主控台）
if([string]::IsNullOrWhiteSpace($Log)){
  & pwsh @args2
} else {
  New-Item -ItemType Directory -Force -Path (Split-Path $Log) | Out-Null
  & pwsh @args2 *>> $Log
}

$code = $LASTEXITCODE
exit $code
