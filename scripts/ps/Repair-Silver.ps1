param(
  [string]$DatahubRoot = "G:\AI\tw-alpha-stack\datahub",
  [string]$SchemaPath  = "G:\AI\tw-alpha-stack\schemas\datasets_schema.yaml",
  [string]$ReportCsv   = "G:\AI\tw-alpha-stack\metrics\repair_report.csv"
)

Write-Host "== Repair-Silver (Auto Fix + Validate) =="

# 掃描 Datahub 所有 parquet 檔案
$files = Get-ChildItem -Path $DatahubRoot -Recurse -Filter *.parquet

foreach ($f in $files) {
  try {
    # 呼叫內嵌 Python 腳本修復 dtype
    & "$PSScriptRoot\..\..\.venv\Scripts\python.exe" - <<'PYCODE' $f.FullName $SchemaPath
import sys, pandas as pd
fname = sys.argv[1]
try:
    df = pd.read_parquet(fname, engine="pyarrow")
    # 修正常見 dtype 問題
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype("string")
    for col in ["volume", "chip"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df.to_parquet(fname, engine="pyarrow", index=False)
except Exception as e:
    print(f"[WARN] 修復失敗 {fname}: {e}")
PYCODE
  } catch {
    Write-Warning "修復失敗 $f : $_"
  }
}

# 修復後立即執行驗證器
& "$PSScriptRoot\..\..\.venv\Scripts\python.exe" `
  "$PSScriptRoot\..\validate_silver.py" `
  --datahub-root $DatahubRoot `
  --schema-path  $SchemaPath `
  --strict `
  --report-csv   $ReportCsv

Write-Host "== Repair-Silver 完成，報告輸出至 $ReportCsv =="
