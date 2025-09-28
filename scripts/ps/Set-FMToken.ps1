param(
  [Parameter(Mandatory=$true)][string]$Token,
  [switch]$OnlyCurrentSession = $false
)
$ErrorActionPreference = "Stop"
$env:FINMIND_TOKEN = $Token
Write-Host "[OK] Set FINMIND_TOKEN for current session."
if (-not $OnlyCurrentSession) {
  setx FINMIND_TOKEN $Token | Out-Null
  Write-Host "[OK] Persisted FINMIND_TOKEN to user environment. (Open a new PowerShell)"
}