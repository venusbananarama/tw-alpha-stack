# backtest_fix_bundle_v1
蝯曹??隞 + 敺??葫撘?嚗? weekly_check ?詨捆??

**?亙嚗?* `unified_run_backtest.ps1`
?舀嚗-Factors -OutDir -Start -End -FactorsPath -Config -TopN -Rebalance -Costs -Seed`

**瘚?嚗?*
1) ?蔥閮剖? ??`merged_config.yaml`
2) ?芸??澆? `backtest\longonly_topN.py`
3) ?亙仃????? `backtest\simulate_topN.py`嚗?甈?TopN嚗???撟唾﹛嚗?

**蝷箔?嚗?*
.\unified_run_backtest.ps1 `
  -Factors "composite_score mom_252_21 vol_20" `
  -OutDir "G:\AI\datahub\alpha\backtests\topN_50_W" `
  -Start "2015-01-01" -End "2020-12-31" `
  -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
  -Config "configs\backtest_topN_example.yaml" `
  -TopN 50 -Rebalance "W" -Costs 0.0005
