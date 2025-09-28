param(
    [string]$SYM = $(Read-Host "�п�J�Ѳ��N�� (�Ҧp 6669.TW)")
)

# ===== �]�w���| =====
$PROJECT   = "G:\AI\tw-alpha-stack"
$FACTORS   = "G:\AI\datahub\alpha\alpha_factors_fixed.parquet"
$FATAI_OUT = "G:\AI\fatai\out"
$FATAI     = "G:\AI\fatai"
# ====================

# �Ұ� venv
& "$PROJECT\.venv\Scripts\Activate.ps1"

# �ץX��@�Ѳ� ohlcv.csv
python "$PROJECT\scripts\export_symbol.py" --factors "$FACTORS" --symbol $SYM --out "$FATAI_OUT"

# ���� FATAI ���о�X
Set-Location $FATAI
python integrated_main.py --out "$FATAI_OUT"

Write-Host ""
Write-Host "[����] �w�g�� $SYM �� ohlcv.csv �s�� $FATAI_OUT\$SYM �ð��� FATAI ����" -ForegroundColor Green
Pause
