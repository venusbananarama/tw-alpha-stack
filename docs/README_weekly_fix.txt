# weekly_fix_bundle_v2

這包是「一次到位」的快速修復版，直接覆蓋到 **tw-alpha-stack** 專案根目錄即可用。

## 內含

- `check_weekly_after_patch.ps1`
  - 支援參數：`-Factors` `-OutDir` `-Start` `-End` `-FactorsPath` `-Config`（新增）
  - 會自動呼叫 `scripts/project_check.py` 做 **W-FRI 週期的週內最後交易日** 快照與 summary。

- `scripts/project_check.py`
  - 讀取你的 factor parquet（例如 `G:\AI\datahub\alpha\alpha_factors_fixed.parquet`）。
  - 對每支股票、每個 **W-FRI** 週期，保留該週 **最後一個實際交易日** 的資料列。
  - 輸出：
    - `weekly_snapshot.csv`
    - `preview.csv`（每週前 10 檔）
    - `summary.txt`
    - `log_args.txt`（完整參數以利重現）

- `scripts/debug_weekly_snapshot.py`
  - 超小工具，用來快速確認每週最後交易日抓取是否正確。

## 安裝／放置

1. 解壓縮到你的專案根目錄：`G:\AI\tw-alpha-stack\`
   - 會得到：
     - `G:\AI\tw-alpha-stack\check_weekly_after_patch.ps1`
     - `G:\AI\tw-alpha-stack\scripts\project_check.py`
     - `G:\AI\tw-alpha-stack\scripts\debug_weekly_snapshot.py`

2. 建議使用既有的 `.venv`（腳本會自動偵測）；沒有也可用系統 `python`。

## 使用範例

```powershell
# 最常用（跟你之前一樣，但現在 -Config 完整支援，不會再報錯）
.\check_weekly_after_patch.ps1 `
  -Factors "composite_score mom_252_21 vol_20" `
  -OutDir "G:\AI\datahublphaacktests\grid_test\_weekly_check" `
  -Start "2015-01-01" `
  -End "2020-12-31" `
  -Config "configsacktest_topN_fixed.yaml" `
  -FactorsPath "G:\AI\datahublphalpha_factors_fixed.parquet"
```

## 產出說明

- `weekly_snapshot.csv`：欄位含 `week`, `last_trade_date`, `symbol`, 以及你指定的 factor 欄位（存在者）。
- `summary.txt`：起迄週、週數、每週股票數、缺漏欄位等。
- `preview.csv`：每週前 10 檔的樣本，方便肉眼確認。

## 小提示

- 如果你不確定 parquet 路徑，直接用 `-FactorsPath` 指定即可。
- 你的舊指令多了 `-Config` 才報錯；此版已新增支援，向下相容。
