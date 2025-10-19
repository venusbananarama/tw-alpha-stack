# RUNBOOK（操作 → 指令對照）

1) Preflight（新鮮度/結構/滯後）
$PY = ".\.venv\Scripts\python.exe"
& $PY .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root .
Get-Content .\reports\preflight_report.json -TotalCount 80

2) 建投資池（build）
& $PY .\scripts\build_universe.py --config .\configs\universe.yaml --rules .\rules.yaml --out .\configs\investable_universe.txt --drop-empty

3) 回測 / 網格（示例）
& $PY .\scripts\backtest\longonly_topN.py --symbols-file .\configs\investable_universe.txt --start 2018-01-01 --end 2025-10-03 --topN 20 --rebalance Weekly --cost-bps 10 --tax-bps 30

4) Walk-forward 與 Gate
& $PY .\scripts\wf_runner.py --dir .\runs\wf_configs --export .\reports
.\tools\Run-WFGate.ps1
Get-Content .\reports\gate_summary.json -TotalCount 120

5) 交接發版
pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Publish-Handover.ps1 -RepoSlug "venusbananarama/tw-alpha-stack"
