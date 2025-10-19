param(
  [Parameter(Mandatory=$true)][string]$In,
  [Parameter(Mandatory=$true)][string]$Out,
  [switch]$Overwrite
)
if(-not (Test-Path $In)){ throw "Input not found: $In" }

$dir = Split-Path $Out
if($dir){ New-Item -ItemType Directory -Force $dir | Out-Null }

$raw = Get-Content $In -Encoding UTF8
$ids = $raw `
  | Where-Object { $_ -notmatch '^\s*(#|$)' } `
  | ForEach-Object {
      if($_ -match '^\s*(\d{4})\s*(?:\.TW)?\s*$'){ $Matches[1] }
    } `
  | Where-Object { $_ -match '^\d{4}$' } `
  | Sort-Object -Unique

if(-not $Overwrite -and (Test-Path $Out)){
  Copy-Item $Out "$Out.bak_$(Get-Date -Format yyyyMMdd_HHmmss)" -Force
}

Set-Content -Path $Out -Value $ids -Encoding UTF8
"IDs=$($ids.Count) -> $Out"
