#requires -Version 7.0
[CmdletBinding()]
param([double]$WrapperKB=1.6)
$ErrorActionPreference='Stop'
$root=(Resolve-Path .).Path
$toolsDir=Join-Path $root 'tools'
$files=Get-ChildItem -Recurse -File $toolsDir -Filter *.ps1
$rows=foreach($f in $files){
  $sizeKB=[Math]::Round($f.Length/1KB,2)
  $text=Get-Content -LiteralPath $f.FullName -Raw -EA SilentlyContinue
  $isWrap = ($sizeKB -le $WrapperKB) -or ($text -match 'Auto-generated wrapper')
  [pscustomobject]@{
    Path=$f.FullName; Name=$f.Name; SizeKB=$sizeKB
    IsWrapper=$isWrap; CanonicalPath=$f.FullName; Impact='KEEP'; TargetResolved=$null
  }
}
$rep=Join-Path $root 'reports'; if(-not(Test-Path $rep)){New-Item -ItemType Directory -Force -Path $rep|Out-Null}
$ts=Get-Date -Format yyyyMMdd_HHmmss
$planJson=Join-Path $rep ("tools_tidy_plan_{0}.json" -f $ts)
$rows | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $planJson -Encoding utf8
Write-Host "PLAN -> $planJson"
