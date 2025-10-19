# LLM_BOOTSTRAP（tw-alpha-stack）

**語言**：繁中｜**Shell**：PowerShell 7｜**Git**：**禁止 rebase**；非快轉用 ours-merge；**大檔走 GitHub Releases**。  
**SSOT/Gate/KPI（v4X）**：W‑FRI 週錨；WF 三窗 Pass ≥ 0.80；週 RankIC ≥ 0.03；PSR ≥ 0.9；DSR_after_costs > 0；MaxDD ≤ 20%；年換手 ≤ 500%；Replay MAE ≤ 2 bps。詳見 FACTSHEET。  

## Git 規範（固定口訣）
git fetch origin --tags
git merge --allow-unrelated-histories -s ours --no-edit origin/main
git push --force-with-lease origin main

## 證據鏈
preflight_report.json／gate_summary.json／configs/investable_universe.txt／reports/*.html|xlsx／run_manifest.json

## 起步檢查（不改檔）
$PY = ".\.venv\Scripts\python.exe"
& $PY .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root .
.\tools\Run-WFGate.ps1
