param(
    [string]$SYM = $(Read-Host "請輸入股票代號 (例如 6669.TW)")
)

& "G:\AI\tw-alpha-stack\.venv\Scripts\Activate.ps1"

python "G:\AI\tw-alpha-stack\scripts\export_symbol.py" `
  --factors "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
  --symbol $SYM `
  --out "G:\AI\fatai\out"

Write-Host ""
Write-Host "[完成] 已經把 $SYM 的 ohlcv.csv 存到 G:\AI\fatai\out\$SYM\" -ForegroundColor Green
Pause
