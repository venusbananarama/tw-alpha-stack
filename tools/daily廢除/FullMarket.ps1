# File: tools/daily/FullMarket.ps1
# Phase-1 FinMind 主線（四表）入口
# DEPRECATED: use scripts/p1_daily_routine.py as the Phase-1 entrypoint.
# 責任：
#   - 驅動四個 dataset：prices/chip/per/dividend（或其子集）
#   - 管理 RunType（backfill/live）、Mode（single/roundrobin）、日期半開區間 [Start,End)
#   - 每個 dataset × day 呼叫 Python 引擎 scripts/finmind_backfill.py
#   - 寫入 _state\<line>\<dataset>\YYYY-MM-DD.ok 與 metrics\ingest_ledger.jsonl
#   - 只跑「有開盤的交易日」（透過 cal/trading_days.csv）
#   - 避免 wrapper，確保單一入口

#requires -Version 7
[CmdletBinding(PositionalBinding = $false)]
param(
    [ValidateSet('backfill','live')]
    [string]$RunType = 'backfill',

    [ValidateSet('single','roundrobin')]
    [string]$Mode = 'roundrobin',

    [ValidateSet('prices','chip','per','dividend','all')]
    [string[]]$Datasets = @('all'),

    # 單日模式（[D, D+1)）
    [string]$Date,

    # 多日模式（[Start, End)；End 半開）
    [string]$Start,
    [string]$End,

    # 速率控制：寫入 FINMIND_QPS / FINMIND_RPM 給 Python 用
    [double]$Qps = 1.0,
    [int]   $Rpm = 60,

    # 是否跳過已有 .ok 的日子
    [switch]$SkipIfOk,

    # 安全上限（曆日）：最多允許幾天
    [int]$MaxDays = 365 * 40,

    # Universe 檔案路徑（僅用於顯示 UniverseCount，真正投資池以 repo root 為 SSOT）
    [string]$UniversePath = 'investable_universe.txt',

    # 執行批次識別
    [string]$RunId = "$(Get-Date -Format 'yyyyMMdd-HHmmss')",

    # 排練模式：不呼叫 Python、不寫檔，只印計畫
    [switch]$DryRun
)

begin {
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $script:NoWork = $false

    # === 1. 解析路徑：從 FullMarket.ps1 → tools → repo root ===
    $ScriptDir = if ($PSScriptRoot) {
        $PSScriptRoot
    } elseif ($MyInvocation.MyCommand.Path) {
        Split-Path -Parent $MyInvocation.MyCommand.Path
    } else {
        (Resolve-Path '.').Path
    }

    $ToolsDir = Split-Path -Parent $ScriptDir
    $RepoRoot = Split-Path -Parent $ToolsDir
    Set-Location $RepoRoot

    # 匯入交易日共用模組
    $TradingModulePath = Join-Path $RepoRoot 'tools\common\TradingCalendar.psm1'
    if (Test-Path $TradingModulePath) {
        $TradingModule = Import-Module $TradingModulePath -Force -PassThru
        $TradingModuleName = $TradingModule.Name
    }
    else {
        throw "找不到交易日模組：$TradingModulePath，請先建立 tools\common\TradingCalendar.psm1"
    }

    $CheckpointRel  = if ($RunType -eq 'backfill') { '_state\mainline' } else { '_state\ingest' }
    $CheckpointRoot = Join-Path $RepoRoot $CheckpointRel
    $LedgerPath     = Join-Path $RepoRoot 'metrics\ingest_ledger.jsonl'
    $DataRoot       = Join-Path $RepoRoot 'datahub'

    $PythonExe   = Join-Path $RepoRoot '.venv\Scripts\python.exe'
    $EngineScript = Join-Path $RepoRoot 'scripts\finmind_backfill.py'

    if (-not (Test-Path $PythonExe)) {
        Write-Warning "找不到 Python 執行檔：$PythonExe，將改用 PATH 上的 python"
        $PythonExe = 'python'
    }
    if (-not (Test-Path $EngineScript)) {
        Write-Warning "找不到 finmind_backfill.py：$EngineScript，請確認 scripts 目錄"
    }

    if (-not (Test-Path $DataRoot)) {
        New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
    }

    # === 2. Dataset 展開 ===

    if ($Datasets -contains 'all') {
        $Datasets = @('prices','chip','per','dividend')
    }
    $Datasets = $Datasets | Select-Object -Unique
    if (-not $Datasets) {
        throw "Datasets 不可為空"
    }

    # === 3. Universe 只讀取一次，用來顯示 UniverseCount ===

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
            if (-not $t) { continue }
            if ($t.StartsWith('#')) { continue }
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
    $UniverseIds   = Load-UniverseIds -Path $UniversePathFull
    $UniverseCount = $UniverseIds.Count

    # === 4. 時間區間解析（Date / Start / End） ===

    if ($Date) {
        if ($Start -or $End) {
            Write-Warning "-Date 與 -Start/-End 同時給，將以 -Date 優先"
        }
        $Start = $Date
        $End   = (Get-Date $Date).AddDays(1).ToString('yyyy-MM-dd')
    }

    $today = (Get-Date).Date

    $S = if ($Start) { [datetime]$Start } else { [datetime]'2004-01-01' }
    $E = if ($End)   { [datetime]$End   } else { $today.AddDays(1) }

    if ($E -le $S) {
        throw "End ($($E.ToString('yyyy-MM-dd'))) 必須晚於 Start ($($S.ToString('yyyy-MM-dd'))"
    }

    # 不允許一次跑超過 MaxDays（曆日）
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

    # === 4.1 交易日載入 ===

    # Debug：確認目前 resolve 到哪個 Get-TradingDaysInRange（避免同名衝突）
    $gtd = Get-Command Get-TradingDaysInRange -All -ErrorAction SilentlyContinue
    Write-Host ("Resolve(Get-TradingDaysInRange) = " + (($gtd | Select-Object -First 1 | ForEach-Object { "$($_.Source)::$($_.Name) [$($_.CommandType)]" }) -join '; ')) -ForegroundColor DarkGray

    $calPath        = & "$TradingModuleName\Get-TradingCalendarPath" -RepoRoot $RepoRoot
    $calendar       = & "$TradingModuleName\Import-TradingCalendar" -CalendarPath $calPath
    $TradingDaysRaw = & "$TradingModuleName\Get-TradingDaysInRange" -Calendar $calendar -Start $S -End $E

    # Normalize: 任何層級的 array 都攤平；最後強制 datetime + sort unique
    $TradingDays = @(
        $TradingDaysRaw |
          Where-Object { $_ } |
          ForEach-Object { [datetime]$_ } |
          Sort-Object -Unique
    )

    if (-not $TradingDays -or $TradingDays.Count -eq 0) {
        Write-Host "指定區間內沒有交易日，無需執行。" -ForegroundColor Yellow
        $script:NoWork = $true
        $script:TradingDays = @()
        $script:Cursors = [ordered]@{}
        return
    }

    $TradingIndex = & "$TradingModuleName\Build-TradingDayIndexMap" -TradingDays ([datetime[]]$TradingDays)

    # === 5. .ok / ledger helpers ===

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
        $chk = Get-LastOkDay -root $CheckpointRoot -ds $ds
        if ($chk) {
            return $chk.AddDays(1)
        }

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
            retries     = 0
            duration_ms = $durationMs
            run_type    = $RunType
            run_id      = $RunId
            message     = $message
        } | ConvertTo-Json -Compress
        Add-Content -Encoding UTF8 -Path $LedgerPath -Value $obj
    }

    # === 6. 單日 FinMind 引擎（dataset × day） ===

    function Invoke-FinMind-OneDay {
        param(
            [Parameter(Mandatory)][datetime]$Day,
            [Parameter(Mandatory)][string]$Dataset
        )

        $Sday = $Day.ToString('yyyy-MM-dd')
        $Eday = $Day.AddDays(1).ToString('yyyy-MM-dd')

        if ($DryRun) {
            Write-Host "[DRY] $Dataset $Sday → $Eday | Qps=$Qps Rpm=$Rpm" -ForegroundColor Cyan
            return
        }

        $env:FINMIND_QPS = "$Qps"
        $env:FINMIND_RPM = "$Rpm"

        $args = @(
            $EngineScript,
            '--datasets',    $Dataset,
            '--start',       $Sday,
            '--end',         $Eday,
            '--datahub-root',$DataRoot
        )

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $message = ''
        $exit    = 0

        try {
            & $PythonExe @args
            $exit = $LASTEXITCODE
            if ($exit -ne 0) {
                throw "finmind_backfill exit=$exit for dataset=$Dataset day=$Sday"
            }
            Write-Host "完成 $Dataset $Sday OK" -ForegroundColor Green
        }
        catch {
            $exit = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 1 }
            $message = $_.Exception.Message
            Write-Host "失敗 $Dataset $Sday：$message" -ForegroundColor Red
            throw
        }
        finally {
            $sw.Stop()
            $ms = [int]$sw.ElapsedMilliseconds
            Write-Ledger -ds $Dataset -d $Day -qps $Qps -rpm $Rpm -exitCode $exit -durationMs $ms -message $message
            if ($exit -eq 0 -and -not $DryRun) {
                Write-Ok -root $CheckpointRoot -ds $Dataset -d $Day
            }
        }
    }

    # === 7. 抬頭 + 游標初始化（以交易日 index 為單位） ===

    $dsStr = ($Datasets -join ',')
    Write-Host "FullMarket | Mode=$Mode | RunType=$RunType | Datasets=$dsStr" -ForegroundColor Cyan
    Write-Host "Qps=$Qps | RPM=$Rpm | Range=$($S.ToString('yyyy-MM-dd')) → $($E.ToString('yyyy-MM-dd')) (End 半開)" -ForegroundColor Yellow
    Write-Host "RepoRoot=$RepoRoot" -ForegroundColor DarkCyan
    Write-Host "Checkpoints=$CheckpointRoot" -ForegroundColor DarkCyan
    Write-Host "DataRoot=$DataRoot" -ForegroundColor DarkCyan
    Write-Host "Ledger=$LedgerPath" -ForegroundColor DarkCyan
    Write-Host "Universe=$UniverseCount Path=$UniversePathFull" -ForegroundColor DarkCyan

    $Cursors = [ordered]@{}
    foreach ($ds in $Datasets) {
        $start0 = NextStartFromOk -ds $ds -fallback $S
        if ($start0 -lt $S) {
            $start0 = $S
        }

        $idx = & "$TradingModuleName\Find-NearestTradingIndex" -IndexMap $TradingIndex -TradingDays $TradingDays -StartDate $start0
        $Cursors[$ds] = $idx

        if ($idx -ge 0) {
            $nextDay = $TradingDays[$idx].ToString('yyyy-MM-dd')
            Write-Host "游標 $ds next_index=$idx next_day=$nextDay" -ForegroundColor DarkCyan
        }
        else {
            Write-Host "游標 $ds 在區間內沒有待跑交易日（可能已完成）" -ForegroundColor DarkGray
        }
    }
}

process {
    if ($script:NoWork) { return }
    switch ($Mode) {
        'single' {
            foreach ($ds in $Datasets) {
                $idx = [int]$Cursors[$ds]
                if ($idx -lt 0) {
                    Write-Host "dataset $ds 已無待處理交易日" -ForegroundColor DarkGray
                    continue
                }

                for (; $idx -lt @($TradingDays).Count; $idx++) {
                    $d = $TradingDays[$idx]
                    if ($SkipIfOk -and (Has-Ok -root $CheckpointRoot -ds $ds -d $d)) {
                        continue
                    }
                    Invoke-FinMind-OneDay -Day $d -Dataset $ds
                }

                $Cursors[$ds] = -1
            }
        }
        'roundrobin' {
            $active = $true
            while ($active) {
                $active = $false
                foreach ($ds in $Datasets) {
                    $idx = [int]$Cursors[$ds]

                    if ($idx -lt 0 -or $idx -ge @($TradingDays).Count) {
                        continue
                    }

                    while ($idx -lt @($TradingDays).Count -and $SkipIfOk) {
                        $day = $TradingDays[$idx]
                        if (-not (Has-Ok -root $CheckpointRoot -ds $ds -d $day)) {
                            break
                        }
                        $idx++
                    }

                    if ($idx -ge @($TradingDays).Count) {
                        $Cursors[$ds] = -1
                        continue
                    }

                    $runDay = $TradingDays[$idx]
                    Invoke-FinMind-OneDay -Day $runDay -Dataset $ds

                    $idx++
                    $Cursors[$ds] = if ($idx -lt @($TradingDays).Count) { $idx } else { -1 }
                    $active = $true
                }
            }
        }
        default {
            throw "未知 Mode=$Mode"
        }
    }
}

end {
    Write-Host "結束 FullMarket | RunType=$RunType | Datasets=$($Datasets -join ',')" -ForegroundColor Green
}
