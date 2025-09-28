# Invoke-FMAll.ps1
#
# Unified FinMind backfill wrapper for full-market and custom symbol modes.
#
# This script wraps the FinMind Python backfill tool with a consistent interface
# that works for both single-stock and whole-market retrievals.  It performs
# symbol normalisation (e.g. strips ".TW"/".TWO" suffixes), maps friendly
# dataset aliases to the underlying FinMind dataset names, and decides whether
# to download data on a per‑stock basis or via a single aggregated call.
#
# The goal is to minimise pointless API calls, avoid "empty" results caused
# by malformed tickers, and provide sensible defaults for QPS and worker
# settings.  It also supports an optional investable universe file for
# custom symbol lists when no explicit symbols are specified.

param(
    [Parameter(Mandatory=$true)]
    [string]$Start,
    [Parameter(Mandatory=$true)]
    [string]$End,
    # One or more dataset aliases or FinMind dataset names to fetch.  Examples:
    # 'prices','chip','macro_others'.  See $AliasMap below for supported aliases.
    [Parameter(Mandatory=$true)]
    [string[]]$Datasets,
    # Market universe: TSE (上市), OTC (上櫃), or TSEOTC (整體市值)。  Only used
    # when no Symbols list is provided and when dataset is aggregated.
    [ValidateSet('TSE','OTC','TSEOTC')]
    [string]$Universe = 'TSE',
    # Optional explicit list of stock symbols.  Overrides Universe if provided.
    # Symbols may include suffixes like `.TW` or prefixes like `TPE:`; these
    # will be normalised automatically.
    [Alias('Symbol')]
    [string[]]$Symbols,
    # Worker process count.  Defaults to 6 for market‑level fetches.
    [int]$Workers = 6,
    # Queries per second.  Defaults to roughly 1.6; adjust as needed.
    [double]$Qps = 1.6,
    # Hourly API call cap.  FinMind default is 6000.
    [int]$HourlyCap = 6000,
    # Print the constructed Python command before execution.
    [switch]$VerboseCmd,
    # Plan the backfill without actually fetching data.  Passes through to
    # finmind_backfill.py.
    [switch]$PlanOnly,
    # Path to a custom investable universe file.  If not specified, defaults
    # to `configs/investable_universe.txt` relative to the project root.
    [string]$UniverseFile
)

<#
    Define dataset alias mappings.  Keys are friendly names accepted on the
    command line; values are the underlying FinMind dataset names.  If a
    dataset alias is absent from this map, it is assumed to be a FinMind
    dataset name already.  To add support for more aliases, extend this
    hashtable accordingly.
#>
$AliasMap = @{
    'prices'       = 'TaiwanStockPrice'
    'chip'         = 'TaiwanStockInstitutionalInvestorsBuySell'
    'macro_others' = 'TaiwanStockMacroEconomics'
    'stock_info'   = 'TaiwanStockInfo'
}

<#
    Define which dataset aliases (or underlying names) should be retrieved as
    aggregated calls instead of per‑stock calls.  Typically this applies to
    datasets that return the entire market without needing a stock_id.  If a
    dataset is listed here, the script performs a single call with `--universe`.
#>
$MarketLevelDatasets = @(
    'prices',               # Alias for TaiwanStockPrice (full market)
    'TaiwanStockPrice'      # Underlying dataset name
)

<#
    A helper function to strip suffixes and prefixes from ticker symbols.  It
    removes `.TW` or `.TWO` suffixes and any leading `TPE:` or other
    uppercase prefixes followed by a colon.  If the input symbol is empty or
    null, it is returned unchanged.
#>
function Normalize-Symbol {
    param([string]$Sym)
    if (-not $Sym) { return $Sym }
    $s = $Sym -replace '\.TW$', '' -replace '\.TWO$', ''
    $s = $s -replace '^[A-Z]+:', ''
    return $s
}

<#
    Load the investable universe file if present.  Each line should contain
    either a numeric stock_id or a ticker with suffix (e.g. 2330 or 2330.TW).
    Empty lines and comment lines starting with `#` are ignored.  Symbols are
    normalised via Normalize-Symbol.  If the file does not exist, an empty
    array is returned.
#>
function Load-Investable-Universe {
    param([string]$FilePath)
    if (-not (Test-Path -Path $FilePath)) { return @() }
    $raw = Get-Content -Path $FilePath -ErrorAction SilentlyContinue
    return $raw | Where-Object { $_ -and -not ($_.Trim().StartsWith('#')) } |
        ForEach-Object { Normalize-Symbol $_.Trim() } |
        Where-Object { $_ }
}

<#
    Resolve the list of symbols to use for per‑stock datasets.  If
    `$Symbols` was provided explicitly, normalise those; otherwise, load the
    investable universe from the given file (defaulting to
    `configs/investable_universe.txt`).  The returned list is de‑duplicated.
#>
function Resolve-Symbols {
    param([string[]]$Explicit, [string]$UniverseFilePath)
    if ($Explicit -and $Explicit.Count -gt 0) {
        return ($Explicit | ForEach-Object { Normalize-Symbol $_ }) | Sort-Object -Unique
    }
    $fileToLoad = $UniverseFilePath
    if (-not $fileToLoad) {
        $fileToLoad = Join-Path -Path '.' -ChildPath 'configs/investable_universe.txt'
    }
    return (Load-Investable-Universe -FilePath $fileToLoad) | Sort-Object -Unique
}

<#
    Build and execute the Python backfill command for a given dataset.  If
    `$market` is `$true`, the call uses `--universe` and does not include
    `--symbols`.  Otherwise, it uses `--symbols` and passes the resolved
    symbols array.  Common parameters like start/end dates, workers, qps, and
    hourly cap are always included.  When `$VerboseCmd` is set, the
    constructed command is printed before execution.  When `$PlanOnly` is
    set, the `--plan-only` flag is passed through.
#>
function Invoke-Fetch {
    param(
        [string]$DatasetAlias,
        [string]$DatasetName,
        [bool]$Market,
        [string[]]$SymbolList
    )
    # Determine Python executable and script path relative to project root.
    $pythonExe    = Join-Path -Path '.' -ChildPath '.venv/Scripts/python.exe'
    $scriptRel    = Join-Path -Path 'scripts' -ChildPath 'finmind_backfill.py'
    $args         = @('--start', $Start, '--end', $End)
    # Dataset
    $args += '--datasets'
    $args += $DatasetName
    if ($Market) {
        # Use universe for aggregated datasets
        $args += '--universe'
        $args += $Universe
    } else {
        # Use explicit symbol list
        if (-not $SymbolList -or $SymbolList.Count -eq 0) {
            Write-Warning "[WARN] No symbols resolved for dataset $DatasetAlias; skipping fetch."
            return
        }
        $args += '--symbols'
        $args += $SymbolList
    }
    # Workers/QPS/Cap
    $args += '--workers';    $args += $Workers
    $args += '--qps';        $args += $Qps
    $args += '--hourly-cap'; $args += $HourlyCap
    # Plan only flag
    if ($PlanOnly.IsPresent) { $args += '--plan-only' }
    # Verbose
    if ($VerboseCmd.IsPresent) {
        $cmdString = $args -join ' '
        Write-Output "[RUN] $pythonExe $scriptRel $cmdString"
    }
    # Execute the Python command
    & $pythonExe $scriptRel $args
}

# Main logic
# Resolve the symbols once (for stock‑level datasets) unless Symbols were explicitly passed.
$resolvedSymbols = Resolve-Symbols -Explicit $Symbols -UniverseFilePath $UniverseFile

foreach ($ds in $Datasets) {
    # Determine underlying dataset name
    $datasetName = if ($AliasMap.ContainsKey($ds)) { $AliasMap[$ds] } else { $ds }
    # Determine if this dataset should be fetched as a single aggregated call
    $isMarket = $false
    # Check by alias or by underlying name
    if ($MarketLevelDatasets -contains $ds -or $MarketLevelDatasets -contains $datasetName) {
        $isMarket = $true
    }
    Invoke-Fetch -DatasetAlias $ds -DatasetName $datasetName -Market:$isMarket -SymbolList $resolvedSymbols
}