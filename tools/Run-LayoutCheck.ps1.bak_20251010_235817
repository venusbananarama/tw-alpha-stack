$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$LogDir   = Join-Path $RepoRoot 'logs'
$LogFile  = Join-Path $LogDir  'layout_check.log'
$CheckPs1 = Join-Path $RepoRoot 'tools\Check-CanonicalLayout.ps1'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $RepoRoot
if (Test-Path "$env:ProgramFiles\PowerShell\7\pwsh.exe") { $Executor = "$env:ProgramFiles\PowerShell\7\pwsh.exe" } else { $Executor = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" }
$ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
Add-Content -Path $LogFile -Value "$ts [START] JSON layout check (executor=$Executor)"
$raw = & $Executor -NoProfile -ExecutionPolicy Bypass -File $CheckPs1 -Json 2>&1
$ok = $false
try {
  $obj = $raw | ConvertFrom-Json
  $ok  = [bool]$obj.ok
  if (-not $ok) {
    Add-Content -Path $LogFile -Value ("[DETAIL] required_missing={0}" -f (($obj.required_missing  | % { $_ }) -join ', '))
    Add-Content -Path $LogFile -Value ("[DETAIL] disallowed_found={0}" -f (($obj.disallowed_found | % { $_ }) -join ', '))
    Add-Content -Path $LogFile -Value ("[DETAIL] stray_scripts={0}"    -f (($obj.stray_scripts    | % { $_ }) -join ', '))
    Add-Content -Path $LogFile -Value ("[DETAIL] misplaced_items={0}"  -f (($obj.misplaced_items  | % { $_ }) -join ', '))
  }
} catch {
  Add-Content -Path $LogFile -Value "[ERROR] JSON parse failed. Raw output follows:"
  Add-Content -Path $LogFile -Value ($raw -join "`n")
  $ok = $false
}
$code = if ($ok) { 0 } else { 1 }
$ts2 = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
Add-Content -Path $LogFile -Value "$ts2 [END] ok=$ok code=$code"
exit $code
