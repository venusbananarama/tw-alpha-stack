#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [string]$UniversePath = '.\configs\derived\universe_ids_only.txt',
  [string]$Start = (Get-Date).ToString('yyyy-MM-dd'),
  [string]$End   = (Get-Date).AddDays(1).ToString('yyyy-MM-dd'),  # end exclusive
  [int]   $BatchSize = 80,
  [ValidateSet('All','Prices','Chip')][string]$Group='All'
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# --- repo root & Extras-Fixed (強制以專案根解析) ---
$RepoRoot    = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ExtrasFixed = Join-Path $RepoRoot 'tools\dateid\Run-DateID-Extras-Fixed.ps1'
if(-not (Test-Path -LiteralPath $ExtrasFixed)){ throw "Extras not found: $ExtrasFixed" }

# --- 讀 ID 清單 ---
if(-not (Test-Path -LiteralPath $UniversePath)){ throw "UniversePath not found: $UniversePath" }
$IDs = Get-Content -LiteralPath $UniversePath |
         Where-Object { $_ -match '^\s*\d{4}\s*$' } |
         ForEach-Object { $_.Trim() }
if(-not $IDs -or $IDs.Count -eq 0){ throw "Universe empty: $UniversePath" }

function New-Batches([string[]]$a,[int]$size=80){
  if(-not $a -or $a.Count -eq 0){ return @() }
  for($i=0; $i -lt $a.Count; $i+=$size){
    $j=[Math]::Min($i+$size-1,$a.Count-1)
    ($a[$i..$j] -join ' ')   # 注意：以空白分隔給 -IDs
  }
}

# --- 驅動日期（End 為不含）---
$d0 = Get-Date $Start; $d1 = Get-Date $End
if($d1 -le $d0){ throw "End must be > Start (end exclusive). Start=$Start End=$End" }

for($d=$d0; $d -lt $d1; $d=$d.AddDays(1)){
  $D = $d.ToString('yyyy-MM-dd')
  foreach($batch in (New-Batches $IDs $BatchSize)){
    Write-Host ("== {0} ids~{1} group={2}" -f $D, ($batch -split '\s+').Count, $Group)
    pwsh -NoProfile -ExecutionPolicy Bypass `
      -File $ExtrasFixed `
      -Date $D -IDs $batch -Group $Group
    if($LASTEXITCODE -ne 0){ throw "Extras-Fixed failed: date=$D ids=($batch)" }
  }
}
Write-Host ("[OK] FullMarket Date-ID done: Start={0} End(excl)={1} IDs={2} Group={3}" `
            -f $Start, $End, $IDs.Count, $Group) -ForegroundColor Green