param(
    [string]$Date = (Get-Date).ToString('yyyy-MM-dd'),
    [string[]]$Datasets = @('all'),

    # === 速率（優先序：單一 dataset > 分流(三表/股利) > 全域預設） ===
    [double]$QpsDefault = 1.67,
    [int]   $RpmDefault = 100,
    [Nullable[double]]$QpsTrio,
    [Nullable[int]]  $RpmTrio,
    [Nullable[double]]$QpsDiv,
    [Nullable[int]]  $RpmDiv,
    [Nullable[double]]$QpsPrices,
    [Nullable[int]]  $RpmPrices,
    [Nullable[double]]$QpsChip,
    [Nullable[int]]  $RpmChip,
    [Nullable[double]]$QpsPer,
    [Nullable[int]]  $RpmPer,

    # 若 runner 支援會生效，不支援則忽略
    [int]$BatchSize = 40,
    [int]$MaxConcurrency = 1,

    [switch]$Preflight,
    [switch]$Gate,
    [string]$Root = 'C:\AI\tw-alpha-stack'
)

$ErrorActionPreference = 'Stop'
Set-Location $Root

# === 單日、半開 End ===
$S = (Get-Date $Date).ToString('yyyy-MM-dd')
$E = (Get-Date $Date).AddDays(1).ToString('yyyy-MM-dd')

# === ToExpect：半開 End 指向 $E；僅寫 _state\ingest ===
$env:ALPHACITY_ALLOW  = '1'
$env:EXPECT_DATE_FIXED = $E
$env:EXPECT_DATE       = $E

# 可能被 runner 吃到（不支援則忽略）
$runnerArgs = @()
if($BatchSize)      { $runnerArgs += @('-BatchSize', $BatchSize) }
if($MaxConcurrency) { $runnerArgs += @('-MaxConcurrency', $MaxConcurrency) }

# Dataset 處理
if($Datasets.Count -eq 1 -and $Datasets[0].ToLower() -eq 'all'){
    $Datasets = @('prices','chip','per','dividend')
}
$Datasets = $Datasets | ForEach-Object { $_.ToLower() } | Where-Object { $_ -in @('prices','chip','per','dividend') }
if(-not $Datasets){ throw 'Datasets 不可為空。可用：prices, chip, per, dividend, all' }

function Set-Rate([Nullable[double]]$qps, [Nullable[int]]$rpm){
    if($null -ne $qps){ $env:FINMIND_QPS = [string]$qps } else { $env:FINMIND_QPS = [string]$QpsDefault }
    if($null -ne $rpm){ $env:FINMIND_RPM = [string]$rpm } else { $env:FINMIND_RPM = [string]$RpmDefault }
}

Write-Host "[代號D] Date=$S (半開到 $E) | Datasets=$(($Datasets -join ','))" -ForegroundColor Cyan

foreach($d in $Datasets){
    switch ($d) {
        'prices' {
            $qps = $QpsPrices; if($null -eq $qps){ $qps = $QpsTrio }
            $rpm = $RpmPrices; if($null -eq $rpm){ $rpm = $RpmTrio }
            Set-Rate $qps $rpm
            Write-Host ("  [prices] QPS={0} | RPM={1}" -f $env:FINMIND_QPS,$env:FINMIND_RPM) -ForegroundColor DarkCyan
            Write-Host "→ Run-FullMarket-ToExpect.ps1 [prices]" -ForegroundColor Yellow
            pwsh -NoProfile -File .\tools\daily\Run-FullMarket-ToExpect.ps1 -Start $S -End $E -Dataset prices @runnerArgs
        }
        'chip' {
            $qps = $QpsChip; if($null -eq $qps){ $qps = $QpsTrio }
            $rpm = $RpmChip; if($null -eq $rpm){ $rpm = $RpmTrio }
            Set-Rate $qps $rpm
            Write-Host ("  [chip]   QPS={0} | RPM={1}" -f $env:FINMIND_QPS,$env:FINMIND_RPM) -ForegroundColor DarkCyan
            Write-Host "→ Run-FullMarket-ToExpect.ps1 [chip]" -ForegroundColor Yellow
            pwsh -NoProfile -File .\tools\daily\Run-FullMarket-ToExpect.ps1 -Start $S -End $E -Dataset chip @runnerArgs
        }
        'per' {
            $qps = $QpsPer; if($null -eq $qps){ $qps = $QpsTrio }
            $rpm = $RpmPer; if($null -eq $rpm){ $rpm = $RpmTrio }
            Set-Rate $qps $rpm
            Write-Host ("  [per]    QPS={0} | RPM={1}" -f $env:FINMIND_QPS,$env:FINMIND_RPM) -ForegroundColor DarkCyan
            Write-Host "→ Run-FullMarket-ToExpect.ps1 [per]" -ForegroundColor Yellow
            pwsh -NoProfile -File .\tools\daily\Run-FullMarket-ToExpect.ps1 -Start $S -End $E -Dataset per @runnerArgs
        }
        'dividend' {
            $qps = $QpsDiv
            $rpm = $RpmDiv
            Set-Rate $qps $rpm
            Write-Host ("  [dividend] QPS={0} | RPM={1}" -f $env:FINMIND_QPS,$env:FINMIND_RPM) -ForegroundColor DarkCyan
            Write-Host "→ Backfill-Dividend-Force.ps1" -ForegroundColor Yellow
            pwsh -NoProfile -File .\tools\daily\Backfill-Dividend-Force.ps1 -Start $S -End $E @runnerArgs
        }
    }
}

# （可選）Preflight + Gate（Gate 驗收日鎖回 $S）
if($Preflight -or $Gate){
    $env:EXPECT_DATE_FIXED = $S
    $env:EXPECT_DATE = $S
}
if($Preflight){
    Write-Host "→ Preflight (EXPECT_DATE=$S)" -ForegroundColor Green
    .\.venv\Scripts\python.exe .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root .
}
if($Gate){
    Write-Host "→ WFGate (唯一入口)" -ForegroundColor Green
    pwsh -NoProfile -File .\tools\gate\Run-WFGate.ps1
}

Write-Host "完成。ok 僅落於 .\_state\ingest\<dataset>\<date>.ok；不碰 mainline。" -ForegroundColor Cyan
