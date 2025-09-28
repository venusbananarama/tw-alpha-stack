# run_all_once.ps1 — 一鍵手動執行（每日ETL + 週回測）
param(
  [string]$Root = "G:\AI\tw-alpha-stack"
)
.\scripts\noagent\run_daily_etl.ps1 -Root $Root
.\scripts\noagent\run_weekly_backtest.ps1 -Root $Root
