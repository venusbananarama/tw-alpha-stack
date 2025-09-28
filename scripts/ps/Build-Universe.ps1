
# File: scripts\ps\Build-Universe.ps1
param(
    [string]$OutFile = "configs\universe.tw_all_sorted.txt"
)

Write-Host "== [AlphaCity] Build Universe List =="

$response = Invoke-RestMethod `
    -Headers @{ Authorization = "Bearer $env:FINMIND_TOKEN" } `
    "https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockInfo"

if ($response.status -ne 200) {
    throw "API error: $($response.msg)"
}

$symbols = $response.data.stock_id | Sort-Object
$symbols | Out-File -FilePath $OutFile -Encoding utf8

Write-Host "[OK] Total symbols:" $symbols.Count
Write-Host "[OK] Wrote universe file:" $OutFile
