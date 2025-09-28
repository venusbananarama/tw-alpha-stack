param(
    [string]$SYM = $(Read-Host "請輸入股票代號 (例如 6669.TW)")
)

# ===== 設定路徑 =====
$PROJECT   = "G:\AI\tw-alpha-stack"
$FACTORS   = "G:\AI\datahub\alpha\alpha_factors_fixed.parquet"
$FATAI_OUT = "G:\AI\fatai\out"
$FATAI     = "G:\AI\fatai"
# ====================

# 啟動 venv
& "$PROJECT\.venv\Scripts\Activate.ps1"

# 匯出單一股票 ohlcv.csv
python "$PROJECT\scripts\export_symbol.py" --factors "$FACTORS" --symbol $SYM --out "$FATAI_OUT"

# 執行 FATAI 指標整合
Set-Location $FATAI
python integrated_main.py --out "$FATAI_OUT"

Write-Host ""
Write-Host "[完成] 已經把 $SYM 的 ohlcv.csv 存到 $FATAI_OUT\$SYM 並執行 FATAI 報表" -ForegroundColor Green
Pause
