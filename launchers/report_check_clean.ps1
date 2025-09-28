param(
    [string]$Root = "G:\AI\datahub\alpha\backtests\grid_test",
    [switch]$Open
)

# Find the latest run folder
$latest = Get-ChildItem -Path $Root -Directory -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if (-not $latest) {
    Write-Output "No run folder found under $Root"
    exit 1
}

Write-Output "Latest run: $($latest.FullName)"
Write-Output ""

# Scan for reports and errors
$reports = Get-ChildItem -Path $latest.FullName -Recurse -Filter *_report.md -ErrorAction SilentlyContinue
$errors  = Get-ChildItem -Path $latest.FullName -Recurse -Filter _report_error.txt -ErrorAction SilentlyContinue

if ($reports) {
    Write-Output "Reports:"
    $reports | ForEach-Object { $_.FullName }
    Write-Output ""
} else {
    Write-Output "Reports: none"
}

if ($errors) {
    Write-Output "Report errors:"
    $errors | ForEach-Object { $_.FullName }
    Write-Output ""
} else {
    Write-Output "Report errors: none"
}

# Optional: open the first report or error in Notepad
if ($Open) {
    if ($reports -and $reports.Count -gt 0) {
        & notepad $reports[0].FullName
    } elseif ($errors -and $errors.Count -gt 0) {
        & notepad $errors[0].FullName
    }
}