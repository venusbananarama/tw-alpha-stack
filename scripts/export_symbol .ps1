param(
    [string]$SYM = $(Read-Host "�п�J�Ѳ��N�� (�Ҧp 6669.TW)")
)

& "G:\AI\tw-alpha-stack\.venv\Scripts\Activate.ps1"

python "G:\AI\tw-alpha-stack\scripts\export_symbol.py" `
  --factors "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
  --symbol $SYM `
  --out "G:\AI\fatai\out"

Write-Host ""
Write-Host "[����] �w�g�� $SYM �� ohlcv.csv �s�� G:\AI\fatai\out\$SYM\" -ForegroundColor Green
Pause
