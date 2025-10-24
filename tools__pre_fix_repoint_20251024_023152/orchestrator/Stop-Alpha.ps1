param([switch]$S1, [switch]$S4, [switch]$All)
if ($All -or (-not $S1 -and -not $S4)) {
  $S1 = $true
  $S4 = $true
}

if ($S1) {
  Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" |
    Where-Object { $_.CommandLine -match 'Run-Max-Recent|Run-FullMarket-DateID-MaxRate' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
  Write-Host "🛑 已停止 S1"
}

if ($S4) {
  Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" |
    Where-Object { $_.CommandLine -match 'Run-Max-SmartBackfill' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
  Write-Host "🛑 已停止 S4"
}
