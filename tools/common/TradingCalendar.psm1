# File: tools/common/TradingCalendar.psm1
# 共用交易日工具：
#   - Get-TradingCalendarPath: 解析 trading_days.csv 路徑
#   - Import-TradingCalendar: 讀取 CSV，轉為物件陣列
#   - Get-TradingDaysInRange: 取出區間內的交易日（只含有開盤日）
#   - Build-TradingDayIndexMap: 建立日期 → index 對照表
#   - Find-NearestTradingIndex: 找到第一個 >= StartDate 的交易日 index

#requires -Version 7
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-TradingCalendarPath {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot
    )

    # 新版優先：datahub\ref\trading_days.csv
    $candidates = @(
        (Join-Path $RepoRoot 'datahub\ref\trading_days.csv'),
        (Join-Path $RepoRoot 'datahub\ref\trading_days.txt'),
        # 舊版相容：cal\trading_days.csv / .txt
        (Join-Path $RepoRoot 'cal\trading_days.csv'),
        (Join-Path $RepoRoot 'cal\trading_days.txt')
    )

    foreach ($p in $candidates) {
        if (Test-Path $p) {
            return $p
        }
    }

    $list = $candidates -join '; '
    throw "Trading calendar not found. Tried: $list"
}

function Import-TradingCalendar {
    [CmdletBinding()]
    [OutputType([object[]])]
    param(
        [Parameter(Mandatory = $true)]
        [string]$CalendarPath
    )

    if (-not (Test-Path $CalendarPath)) {
        throw "Trading calendar file not found: $CalendarPath"
    }

    $rows = Import-Csv -Path $CalendarPath
    if (-not $rows -or $rows.Count -eq 0) {
        throw "Trading calendar is empty: $CalendarPath"
    }

    # 嘗試找出日期欄位名稱
    $first   = $rows[0]
    $props   = $first.PSObject.Properties.Name
    $dateCol = $props | Where-Object {
        $_ -in @('date', 'Date', 'trade_date', 'calendar_date', 'cal_date')
    } | Select-Object -First 1

    if (-not $dateCol) {
        throw "Unable to find date column in trading calendar $CalendarPath"
    }

    # 嘗試找出「是否為交易日」欄位（若沒有，就全部當成有開盤）
    $flagCol = $props | Where-Object {
        $_ -in @('is_trading_day', 'is_open', 'is_trade', 'open')
    } | Select-Object -First 1

    $result = @()

    foreach ($row in $rows) {
        $rawDate = $row.$dateCol
        if (-not $rawDate) { continue }

        try {
            $dt = [datetime]::Parse($rawDate)
        }
        catch {
            continue
        }

        $isTrading = $true
        if ($flagCol) {
            $rawFlag = [string]$row.$flagCol
            if ($rawFlag -match '^(0|false|no|n)$') {
                $isTrading = $false
            }
            elseif ($rawFlag -match '^(1|true|yes|y)$') {
                $isTrading = $true
            }
            else {
                # 不認得的值，一律當作有開盤（保守）
                $isTrading = $true
            }
        }

        $result += [pscustomobject]@{
            Date         = $dt.Date
            IsTradingDay = $isTrading
        }
    }

    $result = $result | Sort-Object Date
    if (-not $result -or $result.Count -eq 0) {
        throw "Trading calendar contains no valid dates: $CalendarPath"
    }

    return $result
}

function Get-TradingDaysInRange {
    [CmdletBinding()]
    [OutputType([datetime[]])]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Calendar,

        [Parameter(Mandatory = $true)]
        [datetime]$Start,

        [Parameter(Mandatory = $true)]
        [datetime]$End
    )

    if ($End -le $Start) {
        throw "End ($($End.ToString('yyyy-MM-dd'))) must be greater than Start ($($Start.ToString('yyyy-MM-dd')))."
    }

    $s = $Start.Date
    $e = $End.Date

    $days =
        $Calendar |
        Where-Object {
            $_.IsTradingDay -and
            $_.Date -ge $s   -and
            $_.Date -lt $e
        } |
        Sort-Object Date |
        Select-Object -ExpandProperty Date

    return ,$days
}

function Build-TradingDayIndexMap {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory = $true)]
        [datetime[]]$TradingDays
    )

    $map = @{}
    for ($i = 0; $i -lt $TradingDays.Count; $i++) {
        $key = $TradingDays[$i].ToString('yyyy-MM-dd')
        if (-not $map.ContainsKey($key)) {
            $map[$key] = $i
        }
    }
    return $map
}

function Find-NearestTradingIndex {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$IndexMap,

        [Parameter(Mandatory = $true)]
        [datetime[]]$TradingDays,

        [Parameter(Mandatory = $true)]
        [datetime]$StartDate
    )

    if (-not $TradingDays -or $TradingDays.Count -eq 0) {
        return -1
    }

    $target = $StartDate.Date
    for ($i = 0; $i -lt $TradingDays.Count; $i++) {
        if ($TradingDays[$i] -ge $target) {
            return $i
        }
    }
    return -1
}

Export-ModuleMember -Function `
    Get-TradingCalendarPath, `
    Import-TradingCalendar, `
    Get-TradingDaysInRange, `
    Build-TradingDayIndexMap, `
    Find-NearestTradingIndex
