# Phase-2 Runbook（封關版）

本文件定義 Phase-2 Step-3/Step-3b 的唯一 CLI 入口與驗證流程。

## 1) 入口（SSOT）

- Step-3（factor combo）唯一入口：`scripts/p2/factor_combo.py`
- Step-3b（capacity gate）唯一入口：`scripts/p2/factor_capacity.py`
- 舊路徑 `scripts/factor_capacity.py` 不保留、不可呼叫、不可建立 shim。

## 2) 執行順序

1. 先產生 combo plan（Step-3）
2. 再讀取 combo plan 進行 capacity 評估（Step-3b）

## 3) 指令範本

```bash
# Step-3
python scripts/p2/factor_combo.py \
  --root . \
  --as-of 2025-11-28 \
  --windows 6 12 24 \
  --max-per-window 3

# Step-3b
python scripts/p2/factor_capacity.py \
  --root . \
  --as-of 2025-11-28 \
  --windows 6 12 24
```

## 4) 產物路徑

- Combo plan：`reports/factor_combo.<as_of>.json`
- Capacity summary：`reports/factor_capacity.<as_of>.json`

## 5) 封關驗證

```bash
# 入口存在與參數
python scripts/p2/factor_combo.py --help
python scripts/p2/factor_capacity.py --help

# 舊入口不得存在
test ! -e scripts/factor_capacity.py

# 不可再有舊路徑引用（runbook 本身除外）
rg -n "scripts/factor_capacity.py" . -g '!docs/runbooks/phase2.md'
```

預期：

- `--help` 正常顯示（exit code 0）
- `scripts/factor_capacity.py` 不存在
- `rg` 無命中（exit code 1）
