---
name: Monthly Handover
about: 固定格式的月交接（進度/風險/下一步）
title: "Monthly Handover {{date}}"
labels: ["handover"]
---

## 基本資訊
- 版本/Tag：`<填入 例如 handover-YYYYMMDD-HHMM>`
- Release 連結：`<貼上>`
- CI 狀態：`<latest run link>`

## 進度
- [ ] 冒煙測試（CI `smoke`）通過
- [ ] 證據鏈：`run_manifest.json`、`reports/preflight_report.json`、`reports/gate_summary.json`、`reports/snapshot.txt`
- [ ] 無 >100MB 追蹤檔；大檔在 Release / LFS
- [ ] README 快速上手可跑通

## 風險/阻塞
- 重大問題：
- 依賴/金鑰（僅列變數名）：

## 下一步（給新帳號）
- [ ] `tools\Switch-GitHubAccount.ps1` 切換身份與 SSH
- [ ] `git pull`；本地 `tools\Test-RepoHealth.ps1` 應通過
