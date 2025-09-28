# 驗證週五錨點補丁是否生效：輸出 metrics/weekly_anchor_report.csv
& python scripts\checks\verify_weekly_anchor.py --config configs\data_sources.yaml --out metrics\weekly_anchor_report.csv
Get-Content metrics\weekly_anchor_report.csv | Select-Object -First 5
