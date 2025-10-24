param(
  [string]$Root = (Get-Location).Path,
  [string]$Group = 'ALL',
  [string]$UniverseFile = '.\configs\investable_universe.txt',
  [string]$OutFile = '',
  [string]$IncludePattern = '',
  [string]$ExcludePattern = ''
)

$ErrorActionPreference='Stop'
function Read-Ids([string]$p){
  if(-not (Test-Path $p)){ return @() }
  Get-Content $p | Where-Object { $_ -match '^\s*\d{4}\s*$' } | ForEach-Object { $_.Trim() }
}

$groupFile = Join-Path $Root ("configs\groups\{0}.txt" -f $Group)
$ids = Read-Ids $groupFile

$universePath = if([IO.Path]::IsPathRooted($UniverseFile)){ $UniverseFile } else { Join-Path $Root $UniverseFile }
$univ = Read-Ids $universePath
if($univ.Count -gt 0){ $ids = $ids | Where-Object { $univ -contains $_ } }

if($IncludePattern){ $ids = $ids | Where-Object { $_ -match $IncludePattern } }
if($ExcludePattern){ $ids = $ids | Where-Object { $_ -notmatch $ExcludePattern } }

if([string]::IsNullOrWhiteSpace($OutFile)){
  $ids
}else{
  $outPath = if([IO.Path]::IsPathRooted($OutFile)){ $OutFile } else { Join-Path $Root $OutFile }
  $dir = Split-Path -Parent $outPath
  if(-not (Test-Path $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $ids | Set-Content -Path $outPath -Encoding UTF8
  Write-Host ("[Select-IDs] N={0} -> {1}" -f $ids.Count, $outPath)
}
