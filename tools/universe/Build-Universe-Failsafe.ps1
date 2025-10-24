# failsafe：從 repo root 呼叫對應 Python，禁止使用 -S
$ErrorActionPreference='Stop'
$repo = (Get-Item $PSScriptRoot).Parent.Parent.FullName
$py   = Join-Path $repo '.venv\Scripts\python.exe'
if(-not (Test-Path $py)){
  $py = 'python'   # 次佳方案：走系統 python
}
& $py "scripts\build_universe_failsafe.py" --rules "rules.yaml" --config "configs\universe.yaml" --out "configs\investable_universe.txt"
