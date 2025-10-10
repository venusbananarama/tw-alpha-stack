# 何時需要重建投資池（Universe）
需要：
- 修改 configs/universe.yaml（板別、行業白名單、流動性門檻、最小股價）。
- 更新 symbol 映射或退市名單。
- 交易日曆或 as-of/週錨規則變動導致切片改變。

不需要：
- 只新增 Gate 指標（DSR/PSR/t）。
- 只調整執行旗標（POV、limit_protect、replay 門檻）。
- 開啟 industry_neutral（它作用在權重，不改 Universe 內容）。
