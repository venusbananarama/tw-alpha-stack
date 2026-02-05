# File: tools/daily/FullMarket-DateID.ps1
# Phase-1 dateID ingestion entrypoint
# DEPRECATED: use scripts/p1_daily_routine.py as the Phase-1 entrypoint.
# 責任：
#   - 統一驅動六個 dateID 類 dataset（finstmt/bs/cfs/shareholding/inst_total/gov_bank）
#   - 管理 RunType（backfill/live）、Mode（single/roundrobin）、日期半開區間 [Start,End)
#   - 依 Universe 分批呼叫 Python 引擎（finmind_dateid_backfill.py）
#   - 寫入 _state\<line>\<dataset>\YYYY-MM-DD.ok 與 metrics\ingest_ledger.jsonl
#   - 避免 wrapper，只允許單一 PS1 入口 + 單一 Python 引擎

#requires -Version 7
[CmdletBinding(PositionalBinding = $false)]
param(
    # 資料線別：全歷史（mainline）或每日補（ingest）
    [ValidateSet('backfill','live')]
    [string]$RunType = 'backfill',

    # 跑法：single：每個 dataset 走完再換下一個；roundrobin：逐日交錯輪跑
    [ValidateSet('single','roundrobin')]
    [string]$Mode = 'roundrobin',

    # 資料表：finstmt/bs/cfs/shareholding/inst_total/gov_bank 或 all
    [ValidateSet('finstmt','bs','cfs','shareholding','inst_total','gov_bank','all')]
    [string[]]$Datasets = @('all'),

    # 單日模式（[D, D+1)）
    [string]$Date,

    # 多日模式（[Start, End)；End 半開）
    [string]$Start,
    [string]$End,

    # 速率控制（QPS/RPM 會寫入 FINMIND_QPS / FINMIND_RPM）
    [double]$Qps = 1.0,
    [int]   $Rpm = 60,

    # Universe 分批大小（每批最多幾個 IDs）
    [int]$BatchSize = 200,

    # 是否跳過已有 .ok 的日子
    [switch]$SkipIfOk,

    # 安全上限：最多允許幾天（避免一次撈爆全歷史）
    [int]$MaxDays = 365 * 40,

    # Universe 檔案路徑（相對路徑會從 repo root 展開）
    # 預設對齊根目錄 SSOT：.\investable_universe.txt
    [string]$UniversePath = 'investable_universe.txt',

    # 流控退避設定
    [int]$MaxRateRetries = 8,
    [int]$BaseBackoffSec = 15,
    [int]$MaxBackoffSec  = 600,

    # 執行批次識別
    [string]$RunId = "$(Get-Date -Format 'yyyyMMdd-HHmmss')",

    # 排練模式：只列出要跑的計畫，不真的打 API / 不寫檔
    [switch]$DryRun
)

begin {
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'

    # === 1. 解析路徑：從 FullMarket-DateID.ps1 → tools → repo root ===
    $ScriptDir = if ($PSScriptRoot) {
        $PSScriptRoot
    } elseif ($MyInvocation.MyCommand.Path) {
        Split-Path -Parent $MyInvocation.MyCommand.Path
    } else {
        (Resolve-Path '.').Path
    }

    # ScriptDir 通常是 C:\AI\tw-alpha-stack\tools\daily
    $ToolsDir = Split-Path -Parent $ScriptDir          # ...\tools
    $RepoRoot = Split-Path -Parent $ToolsDir           # ...\tw-alpha-stack

    Set-Location $RepoRoot

    $CheckpointRel  = if ($RunType -eq 'backfill') { '_state\mainline' } else { '_state\ingest' }
    $CheckpointRoot = Join-Path $RepoRoot $CheckpointRel
    $LedgerPath     = Join-Path $RepoRoot 'metrics\ingest_ledger.jsonl'
    $DataRoot       = Join-Path $RepoRoot 'datahub'

    # Python 引擎（dateID 專用）
    $PythonExe     = Join-Path $RepoRoot '.venv\Scripts\python.exe'
    $DateIdScript  = Join-Path $RepoRoot 'scripts\finmind_dateid_backfill.py'
    $ConfigPath    = Join-Path $RepoRoot 'configs\dateid_datasets.yaml'

    if (-not (Test-Path $PythonExe)) {
        Write-Warning "找不到 Python 執行檔：$PythonExe，將改用 PATH 上的 python"
        $PythonExe = 'python'
    }
    if (-not (Test-Path $DateIdScript)) {
        Write-Warning "找不到 finmind_dateid_backfill.py：$DateIdScript，請確認 scripts 目錄"
    }

    # 確保 datahub 目錄存在（Python 會在裡面寫入 parquet）
    if (-not (Test-Path $DataRoot)) {
        New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
    }

    # === 2. Dataset 展開 ===

    if ($Datasets -contains 'all') {
        $Datasets = @('finstmt','bs','cfs','shareholding','inst_total','gov_bank')
    }
    $Datasets = $Datasets | Select-Object -Unique
    if (-not $Datasets) {
        throw "Datasets 不可為空"
    }

    # === 3. Universe 載入 ===

    function Load-UniverseIds {
        param(
            [Parameter(Mandatory)][string]$Path
        )
        if (-not (Test-Path $Path)) {
            throw "Universe 檔不存在：$Path"
        }

        $lines = Get-Content -Path $Path -ErrorAction Stop
        $ids = @()
        foreach ($line in $lines) {
            $t = $line.Trim()
            if (-not $t) { continue }             # 跳過空行
            if ($t.StartsWith('#')) { continue }  # 跳過註解
            $ids += $t
        }

        $ids = $ids | Sort-Object -Unique
        if (-not $ids -or $ids.Count -eq 0) {
            throw "Universe 檔 $Path 內容為空"
        }
        return $ids
    }

    $UniversePathFull = if ([System.IO.Path]::IsPathRooted($UniversePath)) {
        $UniversePath
    } else {
        Join-Path $RepoRoot $UniversePath
    }
    $UniverseIds = Load-UniverseIds -Path $UniversePathFull
    $UniverseCount = $UniverseIds.Count

    # === 4. 時間區間解析（Date / Start / End） ===

    if ($Date) {
        if ($Start -or $End) {
            Write-Warning "-Date 與 -Start/-End 同時給，將以 -Date 優先"
        }
        $Start = $Date
        $End   = (Get-Date $Date).AddDays(1).ToString('yyyy-MM-dd')
    }

    $UserPinnedRange = $PSBoundParameters.ContainsKey('Date') -or
                       $PSBoundParameters.ContainsKey('Start') -or
                       $PSBoundParameters.ContainsKey('End')

    $today = (Get-Date).Date

    $S = if ($Start) { [datetime]$Start } else { [datetime]'2004-01-01' }
    $E = if ($End)   { [datetime]$End   } else { $today.AddDays(1) }

    if ($E -le $S) {
        throw "End ($($E.ToString('yyyy-MM-dd'))) 必須晚於 Start ($($S.ToString('yyyy-MM-dd')))"
    }

    # 不允許一次跑超過 MaxDays
    $spanDays = [int]($E - $S).TotalDays
    if ($spanDays -gt $MaxDays) {
        throw "區間長度 $spanDays 天 > MaxDays=$MaxDays，請分段回補"
    }

    # End 不含當日；若超過今天+1 仍裁到今天+1
    $maxEnd = $today.AddDays(1)
    if ($E -gt $maxEnd) {
        $E = $maxEnd
        Write-Warning "End 超過未來，已裁到 $($E.ToString('yyyy-MM-dd'))"
    }

    # === 4.5 交易日行事曆（僅 gov_bank 使用） ===

    $TradingDaysSet = $null

    function Load-TradingDaysSet {
        param(
            [Parameter(Mandatory)][string]$Path
        )
        if (-not (Test-Path $Path)) {
            throw "交易日檔不存在：$Path"
        }

        $rows = Import-Csv -Path $Path
        if (-not $rows) {
            throw "交易日檔 $Path 內容為空"
        }

        $props = $rows[0].PSObject.Properties.Name
        $col = if ($props -contains 'date') { 'date' } else { $props[0] }

        $set = @{}
        foreach ($row in $rows) {
            $raw = $row.$col
            if (-not $raw) { continue }
            $key = $raw.ToString().Trim()
            if (-not $key) { continue }
            try {
                $key = (Get-Date -Date $key -ErrorAction Stop).ToString('yyyy-MM-dd')
            }
            catch {
                throw "交易日檔 $Path 含無效日期：$raw"
            }
            $set[$key] = $true
        }

        if ($set.Count -eq 0) {
            throw "交易日檔 $Path 內容為空"
        }
        return $set
    }

    function Is-TradingDay {
        param(
            [Parameter(Mandatory)][datetime]$Day
        )
        if (-not $TradingDaysSet) {
            throw "交易日行事曆尚未載入"
        }
        $key = $Day.ToString('yyyy-MM-dd')
        return $TradingDaysSet.ContainsKey($key)
    }

    if ($Datasets -contains 'gov_bank') {
        $TradingDaysPath = Join-Path $DataRoot 'ref\trading_days.csv'
        $TradingDaysSet = Load-TradingDaysSet -Path $TradingDaysPath
    }

    # === 5. .ok / ledger / 游標 helpers ===

    function Get-OkPath {
        param(
            [Parameter(Mandatory)][string]$root,
            [Parameter(Mandatory)][string]$ds,
            [Parameter(Mandatory)][datetime]$d
        )
        $dir  = Join-Path $root $ds
        $name = $d.ToString('yyyy-MM-dd') + '.ok'
        return Join-Path $dir $name
    }

    function Has-Ok {
        param(
            [Parameter(Mandatory)][string]$root,
            [Parameter(Mandatory)][string]$ds,
            [Parameter(Mandatory)][datetime]$d
        )
        $ok = Get-OkPath -root $root -ds $ds -d $d
        return (Test-Path $ok)
    }

    function Get-GovBankParquetPath {
        param(
            [Parameter(Mandatory)][datetime]$Day
        )
        $yyyymm = $Day.ToString('yyyyMM')
        $dateStr = $Day.ToString('yyyy-MM-dd')
        $rel = "silver\alpha\gov_bank\yyyymm=$yyyymm\gov_bank_$dateStr.parquet"
        return Join-Path $DataRoot $rel
    }

    function ShouldSkip {
        param(
            [Parameter(Mandatory)][string]$ds,
            [Parameter(Mandatory)][datetime]$d
        )
        if (-not $SkipIfOk) {
            return $false
        }
        if ($ds -ne 'gov_bank') {
            return (Has-Ok -root $CheckpointRoot -ds $ds -d $d)
        }

        $ok = Has-Ok -root $CheckpointRoot -ds $ds -d $d
        if (-not $ok) {
            return $false
        }
        $parquetPath = Get-GovBankParquetPath -Day $d
        return (Test-Path $parquetPath)
    }

    function Write-Ok {
        param(
            [Parameter(Mandatory)][string]$root,
            [Parameter(Mandatory)][string]$ds,
            [Parameter(Mandatory)][datetime]$d
        )
        $ok  = Get-OkPath -root $root -ds $ds -d $d
        $dir = Split-Path -Parent $ok
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        $tmp = "$ok.tmp"
        Set-Content -Encoding UTF8 -NoNewline -Path $tmp -Value ''
        if (Test-Path $ok) { Remove-Item $ok -Force }
        Rename-Item $tmp $ok
    }

    function Get-LastOkDay {
        param(
            [Parameter(Mandatory)][string]$root,
            [Parameter(Mandatory)][string]$ds
        )
        $dir = Join-Path $root $ds
        if (Test-Path $dir) {
            $last = Get-ChildItem $dir -Filter '*.ok' |
                    Sort-Object Name |
                    Select-Object -Last 1
            if ($last) {
                return [datetime]$last.BaseName
            }
        }
        return $null
    }

    function NextStartFromOk {
        param(
            [Parameter(Mandatory)][string]$ds,
            [Parameter(Mandatory)][datetime]$fallback
        )
        # 先看當前線別（backfill→mainline；live→ingest）
        $chk = Get-LastOkDay -root $CheckpointRoot -ds $ds
        if ($chk) {
            return $chk.AddDays(1)
        }

        # live 線若沒有 ingest 游標，往 mainline 接上去
        if ($RunType -eq 'live') {
            $mlRoot = Join-Path $RepoRoot '_state\mainline'
            $chkml  = Get-LastOkDay -root $mlRoot -ds $ds
            if ($chkml) {
                return $chkml.AddDays(1)
            }
        }

        return $fallback
    }

    function Write-Ledger {
        param(
            [Parameter(Mandatory)][string]$ds,
            [Parameter(Mandatory)][datetime]$d,
            [Parameter(Mandatory)][double]$qps,
            [Parameter(Mandatory)][int]$rpm,
            [Parameter(Mandatory)][int]$exitCode,
            [Parameter(Mandatory)][int]$retries,
            [Parameter(Mandatory)][int]$durationMs,
            [Parameter()][string]$message
        )
        $dir = Split-Path -Parent $LedgerPath
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        $obj = [ordered]@{
            ts          = (Get-Date).ToString('s')
            dataset     = $ds
            day         = $d.ToString('yyyy-MM-dd')
            qps         = $qps
            rpm         = $rpm
            exit        = $exitCode
            retries     = $retries
            duration_ms = $durationMs
            run_type    = $RunType
            run_id      = $RunId
            message     = $message
        } | ConvertTo-Json -Compress
        Add-Content -Encoding UTF8 -Path $LedgerPath -Value $obj
    }

    # Universe 分批
    function Split-IdsToBatches {
        param(
            [Parameter(Mandatory)][string[]]$Ids,
            [Parameter(Mandatory)][int]$BatchSize
        )
        if ($BatchSize -le 0) {
            throw "BatchSize 必須 > 0"
        }
        $count = $Ids.Count
        $result = @()
        for ($offset = 0; $offset -lt $count; $offset += $BatchSize) {
            $end = [Math]::Min($offset + $BatchSize, $count)
            $result += ,($Ids[$offset..($end - 1)])
        }
        return $result
    }

    function Is-RateLimitError {
        param(
            [Parameter(Mandatory)][string]$Message
        )
        # 除了 402/429/WinError10060 之外，明確把 Python exit=3 視為「API/Rate 類可重試錯誤」
        return ($Message -match '402' -or
                $Message -match '429' -or
                $Message -match '(?i)rate.*limit' -or
                $Message -match '(?i)quota' -or
                $Message -match 'WinError 10060' -or
                $Message -match 'exit=3')
    }

    # === 6. 單日 dateID 引擎（dataset × day） ===

    function Invoke-DateIdEngine-OneDay {
        param(
            [Parameter(Mandatory)][datetime]$Day,
            [Parameter(Mandatory)][string]$Dataset,
            [Parameter(Mandatory)][string[]]$UniverseIds
        )

        $Sday = $Day.ToString('yyyy-MM-dd')

        if ($DryRun) {
            if ($Dataset -eq 'gov_bank') {
                $batches = ,@(@('ALL'))
            }
            else {
                $batches = Split-IdsToBatches -Ids $UniverseIds -BatchSize $BatchSize
            }
            $batchCount = $batches.Count
            Write-Host "[DRY] $Dataset $Sday | Universe=$($UniverseIds.Count) 批數=$batchCount BatchSize=$BatchSize" -ForegroundColor Cyan
            return
        }

        if ($Dataset -eq 'gov_bank') {
            $batches = ,@(@('ALL'))
        }
        else {
            $batches = Split-IdsToBatches -Ids $UniverseIds -BatchSize $BatchSize
        }
        $totalRetries = 0
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $exitCode = 0
        $errMsg = ''

        $rpmGateMs = [int][Math]::Ceiling(60000 / [Math]::Max(1, $Rpm))

        try {
            $batchIndex = 0
            foreach ($batch in $batches) {
                $batchIndex++
                while ($true) {
                    try {
                        Write-Host "開始 $Dataset $Sday batch $batchIndex/$($batches.Count) (ids=$($batch.Count))" -ForegroundColor White

                        # QPS/RPM 傳給底層（Python 會使用 FINMIND_QPS 做 per-ID 限速）
                        $env:FINMIND_QPS = "$Qps"
                        $env:FINMIND_RPM = "$Rpm"

                        $idsArg = $batch -join ','
                        $args = @(
                            $DateIdScript,
                            '--dataset',      $Dataset,
                            '--date',         $Sday,
                            '--ids',          $idsArg,
                            '--datahub-root', $DataRoot
                        )
                        if (Test-Path $ConfigPath) {
                            $args += @('--config', $ConfigPath)
                        }

                        & $PythonExe @args
                        $exit = $LASTEXITCODE

                        if ($exit -ne 0) {
                            throw "finmind_dateid_backfill exit=$exit for dataset=$Dataset day=$Sday batch=$batchIndex"
                        }

                        Write-Host "完成 $Dataset $Sday batch $batchIndex/$($batches.Count) OK" -ForegroundColor Green
                        break
                    }
                    catch {
                        $msg = $_.Exception.Message
                        $totalRetries++

                        $isRate = Is-RateLimitError -Message $msg
                        if ($isRate -and $totalRetries -le $MaxRateRetries) {
                            $delaySec = [Math]::Min(
                                [int]($BaseBackoffSec * [Math]::Pow(1.7, $totalRetries - 1)),
                                $MaxBackoffSec
                            )
                            Write-Host "流控/配額/網路，$Dataset $Sday batch $batchIndex 重試第 $totalRetries 次，等待 $delaySec 秒：$msg" -ForegroundColor DarkYellow
                            Start-Sleep -Seconds $delaySec
                            continue
                        }
                        else {
                            throw
                        }
                    }
                    finally {
                        # RPM Gate：每個 batch 完成後都等一下，避免打太快
                        Start-Sleep -Milliseconds $rpmGateMs
                    }
                }
            }

            $sw.Stop()
            $ms = [int]$sw.ElapsedMilliseconds
            if ($Dataset -eq 'gov_bank') {
                $parquetPath = Get-GovBankParquetPath -Day $Day
                if (-not (Test-Path $parquetPath)) {
                    throw "gov_bank parquet 不存在：$parquetPath"
                }
            }
            Write-Ok -root $CheckpointRoot -ds $Dataset -d $Day
            Write-Ledger -ds $Dataset -d $Day -qps $Qps -rpm $Rpm -exitCode 0 -retries $totalRetries -durationMs $ms -message ''
            Write-Host "完成 $Dataset $Sday 全部批次 OK （$([math]::Round($ms / 1000.0, 1)) 秒，重試 $totalRetries 次）" -ForegroundColor Green
        }
        catch {
            $sw.Stop()
            $ms = [int]$sw.ElapsedMilliseconds
            $errMsg = $_.Exception.Message
            Write-Ledger -ds $Dataset -d $Day -qps $Qps -rpm $Rpm -exitCode 1 -retries $totalRetries -durationMs $ms -message $errMsg
            Write-Host "失敗 $Dataset $Sday：$errMsg" -ForegroundColor Red
            throw
        }
    }

    # === 7. 抬頭 + 游標初始化 ===

    $dsStr = ($Datasets -join ',')
    Write-Host "FullMarket-DateID | Mode=$Mode | RunType=$RunType | Datasets=$dsStr" -ForegroundColor Cyan
    Write-Host "Qps=$Qps | RPM=$Rpm | BatchSize=$BatchSize | Universe=$UniverseCount" -ForegroundColor Yellow
    Write-Host "Range=$($S.ToString('yyyy-MM-dd')) → $($E.ToString('yyyy-MM-dd')) (End 半開)" -ForegroundColor Yellow
    Write-Host "RepoRoot=$RepoRoot" -ForegroundColor DarkCyan
    Write-Host "Checkpoints=$CheckpointRoot" -ForegroundColor DarkCyan
    Write-Host "DataRoot=$DataRoot" -ForegroundColor DarkCyan
    Write-Host "Ledger=$LedgerPath" -ForegroundColor DarkCyan
    Write-Host "UniversePath=$UniversePathFull" -ForegroundColor DarkCyan

    $Cursors = [ordered]@{}
    foreach ($ds in $Datasets) {
        # 先從 .ok 游標算出下一天，若沒有 .ok 就用 Start 當 fallback
        # 若使用者明確指定區間，游標必須固定從 Start 開始
        if ($UserPinnedRange) {
            $start0 = $S
        }
        else {
            $start0 = NextStartFromOk -ds $ds -fallback $S
        }

        # 游標不得早於 Start（避免你指定 Start=某日還從更早開始跑）
        if ($start0 -lt $S) {
            $start0 = $S
        }

        $Cursors[$ds] = $start0
        Write-Host "游標 $ds next_start=$($start0.ToString('yyyy-MM-dd'))" -ForegroundColor DarkCyan
    }
}

process {
    switch ($Mode) {
        'single' {
            foreach ($ds in $Datasets) {
                for ($d = $Cursors[$ds]; $d -lt $E; $d = $d.AddDays(1)) {
                    if ($ds -eq 'gov_bank' -and -not (Is-TradingDay -Day $d)) {
                        continue
                    }
                    if (ShouldSkip -ds $ds -d $d) {
                        continue
                    }
                    Invoke-DateIdEngine-OneDay -Day $d -Dataset $ds -UniverseIds $UniverseIds
                }
            }
        }
        'roundrobin' {
            $active = $true
            while ($active) {
                $active = $false
                foreach ($ds in $Datasets) {
                    $d = $Cursors[$ds]

                    # SkipIfOk / 交易日：一路往後找到下一個可跑的日子
                    while ($d -lt $E) {
                        if ($ds -eq 'gov_bank' -and -not (Is-TradingDay -Day $d)) {
                            $d = $d.AddDays(1)
                            continue
                        }
                        if (ShouldSkip -ds $ds -d $d) {
                            $d = $d.AddDays(1)
                            continue
                        }
                        break
                    }

                    if ($d -lt $E) {
                        Invoke-DateIdEngine-OneDay -Day $d -Dataset $ds -UniverseIds $UniverseIds
                        $d = $d.AddDays(1)
                        $Cursors[$ds] = $d
                        $active = $true
                    }
                }
            }
        }
        default {
            throw "未知 Mode=$Mode"
        }
    }
}

end {
    Write-Host "結束 FullMarket-DateID | RunType=$RunType | Datasets=$($Datasets -join ',')" -ForegroundColor Green
}
