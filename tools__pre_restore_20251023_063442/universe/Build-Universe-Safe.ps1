param(
  [string]$Rules = ".\configs\rules.yaml",
  [string]$Config = ".\configs\universe.yaml",
  [string]$Out = ".\configs\investable_universe.txt"
)
$PY="./.venv/Scripts/python.exe"
$tmp = [IO.Path]::ChangeExtension($Out,'tmp')

# 不要吞錯誤，讓你看得到 build 失敗原因
& $PY .\scripts\build_universe.py --config $Config --rules $Rules --out $tmp

$lines = (Get-Content $tmp -ErrorAction SilentlyContinue | ?{$_ -match '^\S+$'} | Measure-Object -Line).Lines
if($lines -gt 0){
  Move-Item $tmp $Out -Force
  New-Item -ItemType Directory -Force -Path .\configs\derived | Out-Null
  Copy-Item $Out .\configs\derived\universe_ids_only.txt -Force
  "Universe updated; lines=$lines"
}else{
  Remove-Item $tmp -ErrorAction SilentlyContinue
  Write-Warning "Universe build produced 0 lines; keep existing file."
}
