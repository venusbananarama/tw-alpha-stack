### [代號四] WF 設定路徑修正（2025-11-04 05:20:18）
結論：wf_runner 請用 tools\gate\wf_configs（非 runs\wf_configs），否則 Gate 只會讀 _runner_results.json(single) 導致 pass_rate 0/0.5。
執行口徑：="2025-11-03"; ="2025-11-03"; .\.venv\Scripts\python.exe .\scripts\wf_runner.py --dir .\tools\gate\wf_configs --export .\reports; pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\gate\Run-WFGate.ps1
