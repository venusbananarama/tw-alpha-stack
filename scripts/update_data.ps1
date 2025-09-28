# 切換到專案根目錄
Set-Location "G:\AI\tw-alpha-stack"

# 啟動 venv
& ".\.venv\Scripts\Activate.ps1"

# 確保 Python 能找到 src\twalpha
$env:PYTHONPATH = "G:\AI\tw-alpha-stack\src"

# 建立 log 資料夾（如果不存在）
$logDir = "G:\AI\tw-alpha-stack\logs"
if (!(Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}
$logFile = Join-Path $logDir "update_data.log"

# 紀錄開始時間
$startTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content -Path $logFile -Value "[$startTime] 開始更新行情資料..."

# 用 Yahoo 更新行情，只補齊最新
# 每批 20 檔，批與批之間停 5 秒
python "scripts\fetch_all.py" --mode yahoo --out "data" --update --batch-size 20 --pause 5
$exitCode = $LASTEXITCODE

# 紀錄完成狀態
$endTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
if ($exitCode -eq 0) {
    Add-Content -Path $logFile -Value "[$endTime] ✅ 更新成功"
    Write-Host "[完成] 已補齊最新行情資料，紀錄寫入 logs\update_data.log" -ForegroundColor Green
} else {
    Add-Content -Path $logFile -Value "[$endTime] ❌ 更新失敗 (ExitCode=$exitCode)"
    Write-Host "[警告] 更新失敗，請檢查 logs\update_data.log" -ForegroundColor Red
}

Pause
