Set-Location "G:\AI\tw-alpha-stack"
& ".\.venv\Scripts\Activate.ps1"
$env:PYTHONPATH = "G:\AI\tw-alpha-stack\src"

$logDir = "G:\AI\tw-alpha-stack\logs"
if (!(Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$logFile = Join-Path $logDir "update_all_night.log"
$failFile = Join-Path $logDir "failed_tickers.txt"
if (Test-Path $failFile) { Clear-Content $failFile }

$startTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$startTime] 🌙 夜間全市場更新開始..." | Out-File -Encoding utf8 -Append $logFile
Write-Host "🌙 夜間全市場更新開始 [$startTime]" -ForegroundColor Cyan

function Run-FetchAndCapture([string]$extraArgs, [string]$failOut, [string]$label) {
    Write-Host "▶️ 開始 $label ..." -ForegroundColor White

    # 即時顯示 Python 輸出
    $process = Start-Process python -ArgumentList @("scripts\fetch_all.py", "--mode", "yahoo", "--out", "data", "--update", "--batch-size", "10", "--pause", "10") `
        -WorkingDirectory "G:\AI\tw-alpha-stack" `
        -RedirectStandardOutput "$logDir\last_output.log" `
        -RedirectStandardError "$logDir\last_error.log" `
        -PassThru -NoNewWindow

    $process.WaitForExit()
    $exitCode = $process.ExitCode

    # 顯示輸出
    if (Test-Path "$logDir\last_output.log") {
        Get-Content "$logDir\last_output.log" | Tee-Object -Append $logFile
    }
    if (Test-Path "$logDir\last_error.log") {
        Get-Content "$logDir\last_error.log" | Tee-Object -Append $logFile
    }

    if ($exitCode -ne 0) {
        "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ❌ 程式錯誤 ExitCode=$exitCode" | Out-File -Encoding utf8 -Append $logFile
        Write-Host "❌ fetch_all.py 出錯 ExitCode=$exitCode" -ForegroundColor Red
    }

    # 擷取失敗股票
    $failed = @()
    if (Test-Path "$logDir\last_output.log") {
        $failed = Get-Content "$logDir\last_output.log" | Select-String "Failed download" | ForEach-Object {
            if ($_ -match "\['([^']+)'\]") { $matches[1] }
        }
    }

    if ($failed.Count -gt 0) {
        $failed | Sort-Object -Unique | Out-File -Encoding utf8 $failOut
        Write-Host "⚠️ 本輪失敗 $($failed.Count) 檔，清單已存到 $failOut" -ForegroundColor Yellow
    } else {
        Write-Host "✅ 本輪成功（或無失敗記錄）" -ForegroundColor Green
    }

    return $failed.Count
}

# 第一次全市場更新
$failCount = Run-FetchAndCapture "" $failFile "第一次全市場更新"

Pause
