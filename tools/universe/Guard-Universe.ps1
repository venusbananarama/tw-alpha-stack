param([int]$MinLines=50)
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest

$DST = '.\configs\investable_universe.txt'
$DER = '.\configs\derived\universe_ids_only.txt'
$CAND = @('.\configs\universe.tw_all', '.\configs\universe.tw_all.txt', '.\configs\investable_universe.bak') | ?{ Test-Path $_ }

$lines = (Get-Content $DST -ErrorAction SilentlyContinue | ?{$_ -match '^\S+$'} | Measure-Object -Line).Lines
if($lines -lt $MinLines){
  if(-not $CAND){ throw "No fallback universe found." }
  $src = $CAND | Select -First 1
  $ids = Get-Content $src | ?{$_ -match '^\S+$'} | %{$_.Trim()} | Sort-Object -Unique
  if($ids.Count -lt $MinLines){ throw "Fallback has too few lines ($($ids.Count))." }
  New-Item -ItemType Directory -Force -Path (Split-Path $DER) | Out-Null
  $ids | Set-Content -LiteralPath $DST -Encoding UTF8
  $ids | Set-Content -LiteralPath $DER -Encoding UTF8
  "Universe restored from: $src (lines=$($ids.Count))"
}
