PackMonthly for Windows (PowerShell)
====================================

This script packages your monthly SSOT bundle on Windows.

Files created:
  - configs/snapshots/rules_<ASOF>_<HASH8>.yaml
  - docs/manifest_<ASOF>.json
  - SSOT_monthly_<ASOF>.zip

Usage (run from your project root):
  # Dry run (validate presence of required files)
  powershell -ExecutionPolicy Bypass -File .\Pack-Monthly.ps1 -Asof 2025-10-05 -DryRun

  # Package with optional reports (default)
  powershell -ExecutionPolicy Bypass -File .\Pack-Monthly.ps1 -Asof 2025-10-05

  # Package without optional reports (skip Polaris/Factor evals)
  powershell -ExecutionPolicy Bypass -File .\Pack-Monthly.ps1 -Asof 2025-10-05 -NoOptional

Required docs present under docs/ (exact names):
  SSOT_master_<ASOF>.md
  changelog_<ASOF>.md
  page_index_<ASOF>.json
  gate_checklist_<ASOF>.md
  ssot_keys_<ASOF>.yaml
  patch_suggestions_<ASOF>.yaml

Required reports:
  reports/preflight/preflight_<ASOF>.json

Notes:
  - rules.yaml is treated as the single source of truth. The script snapshots it with SHA256[0:8].
  - The zip contains: docs/, configs/snapshots/, and the day's preflight JSON. Optional: Polaris scorecard, factor evals.
  - Run from project root. Do not package datahub/.
  - If you routinely change GPT accounts, paste docs/manifest_<ASOF>.json to the chat along with the zip.
