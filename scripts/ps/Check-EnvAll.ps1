param(
    [string]$PythonExe = ".\.venv\Scripts\python.exe",
    [string]$MinimalFile = "configs\env_constraints-311.txt",
    [string]$StrictFile  = "configs\env_constraints-311-strict.txt"
)

Write-Host "=== AlphaCity 環境檢查 + 冒煙測試 ===" -ForegroundColor Cyan

$CurrentFile = "_env_current.txt"
& $PythonExe -m pip freeze | Out-File -Encoding UTF8 $CurrentFile

function Compare-Env($refFile, $tag) {
    if (!(Test-Path $refFile)) {
        Write-Host "[SKIP] $tag 檔案不存在: $refFile" -ForegroundColor Yellow
        return
    }
    Write-Host "`n--- Compare vs $tag ---" -ForegroundColor Cyan
    $diff = git diff --no-index --color-words $refFile $CurrentFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "🟢 完全一致 ($tag)" -ForegroundColor Green
    } else {
        Write-Host $diff
        Write-Host "🟡 與 $tag 有差異 (請檢查)" -ForegroundColor Yellow
    }
}

Compare-Env $MinimalFile "Minimal"
Compare-Env $StrictFile "Strict"

Write-Host "`n=== 執行冒煙測試 (Run-SmokeTests.ps1) ===" -ForegroundColor Cyan
try {
    & .\scripts\ps\Run-SmokeTests.ps1 -PythonExe $PythonExe
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n🟢 環境 + 冒煙測試全部通過" -ForegroundColor Green
    } else {
        Write-Host "`n🔴 冒煙測試失敗，請考慮退回 strict constraints" -ForegroundColor Red
    }
}
catch {
    Write-Host "[ERROR] 無法執行 Run-SmokeTests.ps1: $_" -ForegroundColor Red
}
