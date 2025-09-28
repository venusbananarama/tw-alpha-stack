# Invoke-FMSingle.ps1
#
# This PowerShell script provides a simple wrapper for running FinMind single-stock
# data downloads via the Python `finmind_backfill.py` script. It handles
# parameters for start/end dates, symbol, datasets, worker count and QPS, and
# automatically strips common exchange suffixes (".TW" or ".TWO") from the
# provided stock symbol.  A `-VerboseCmd` switch is provided so you can see
# exactly what command will be executed.

param(
    [Parameter(Mandatory = $true)]
    [string]$Start,
    [Parameter(Mandatory = $true)]
    [string]$End,
    [Parameter(Mandatory = $true)]
    [string]$Symbol,
    # A list of FinMind dataset names to retrieve (e.g. 'TaiwanStockPER')
    [string[]]$Datasets,
    # Number of worker processes (defaults to 2)
    [int]$Workers = 2,
    # Queries per second allowed (defaults to 1.0)
    [double]$Qps = 1.0,
    # Include this switch to see the full command line before execution
    [switch]$VerboseCmd
)

# Normalize the stock symbol by removing common exchange suffixes and uppercase prefixes.
$normalizedSymbol = $Symbol
$normalizedSymbol = $normalizedSymbol -replace '\.TW$', '' -replace '\.TWO$', ''
$normalizedSymbol = $normalizedSymbol -replace '^[A-Z]+:', ''

<#
    Map friendly dataset aliases to FinMind dataset names.  Extend this
    hashtable to support additional aliases.  If no alias is present, the
    dataset name is passed through unchanged.
#>
$aliasMap = @{
    'prices'       = 'TaiwanStockPrice'
    'chip'         = 'TaiwanStockInstitutionalInvestorsBuySell'
    'macro_others' = 'TaiwanStockMacroEconomics'
    'stock_info'   = 'TaiwanStockInfo'
}

<#
    Build dataset arguments.  Each dataset name must be supplied separately
    following the `--datasets` flag.  Alias names are resolved into
    underlying FinMind dataset names on the fly.
#>
$datasetArgs = @()
if ($Datasets -and $Datasets.Length -gt 0) {
    $datasetArgs += '--datasets'
    foreach ($ds in $Datasets) {
        if ($aliasMap.ContainsKey($ds)) {
            $datasetArgs += $aliasMap[$ds]
        } else {
            $datasetArgs += $ds
        }
    }
}

# Prepare the Python executable and backfill script paths.
$pythonExe = Join-Path -Path "." -ChildPath ".venv/Scripts/python.exe"
$scriptRelPath = Join-Path -Path "scripts" -ChildPath "finmind_backfill.py"

# Build the full command line for python.
$cmdLine = @(
    '--start', $Start,
    '--end', $End,
    '--symbols', $normalizedSymbol
)

# Append dataset arguments if provided.
if ($datasetArgs.Count -gt 0) {
    $cmdLine += $datasetArgs
}

# Append other optional arguments.
if ($Workers) {
    $cmdLine += @('--workers', $Workers)
}
if ($Qps) {
    $cmdLine += @('--qps', $Qps)
}

# Convert the array into a space-separated string for display.
$cmdString = $cmdLine -join ' '

if ($VerboseCmd.IsPresent) {
    Write-Output "[RUN] $pythonExe $scriptRelPath $cmdString"
}

# Invoke the python backfill script with argument array.
& $pythonExe $scriptRelPath $cmdLine