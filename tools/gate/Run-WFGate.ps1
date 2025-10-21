param(
  [string]$Python="./.venv/Scripts/python.exe",
  [string]$Runner="./scripts/wf_runner.py",
  [string]$Dir="./runs/wf_configs",
  [string]$Export="./reports"
)
if ($env:ALPHACITY_ALLOW -ne "1") { Write-Error "ALPHACITY_ALLOW=1 not set. Aborting." -ErrorAction Stop }
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest
if(-not (Test-Path $Export)){ New-Item -ItemType Directory -Force -Path $Export|Out-Null }
$outFile = Join-Path $Export 'gate_summary.json'

# 1) 原生 runner（注意：現階段只撿 $Dir 內的 *.csv）
& $Python $Runner --dir $Dir --export $outFile

# 2) 門檻
$passMin = 0.80
$rulesPath = ".\configs\rules.yaml"
if (Test-Path $rulesPath) {
  $m = Select-String -Path $rulesPath -Pattern 'wf_pass_min\s*:\s*([0-9]*\.?[0-9]+)' -AllMatches | Select-Object -First 1
  if ($m) { $passMin = [double]$m.Matches[0].Groups[1].Value }
}

function Normalize-List([object[]]$runs,[double]$passMin){
  $passes = 0
  foreach($it in $runs){
    if($null -eq $it){ continue }
    $ok = $false
    $ov = $it.PSObject.Properties['overall']?.Value
    if($ov -and ($ov -match 'PASS')) { $ok = $true }
    if(-not $ok){
      $p = $it.PSObject.Properties['pass']?.Value
      if($null -ne $p -and [bool]$p){ $ok = $true }
    }
    if(-not $ok -and $it.PSObject.Properties['wf']){
      $wf=$it.wf
      $wfp = $wf.PSObject.Properties['pass']?.Value
      $wfs = $wf.PSObject.Properties['status']?.Value
      if($null -ne $wfp -and [bool]$wfp){ $ok=$true }
      elseif($wfs -and ($wfs -match 'PASS')){ $ok=$true }
    }
    if(-not $ok){
      $wf_pass = $it.PSObject.Properties['wf_pass']?.Value
      if($null -ne $wf_pass -and [int]$wf_pass -eq 1){ $ok=$true }
    }
    if($ok){ $passes++ }
  }
  $count   = $runs.Count
  $rate    = if($count){ [math]::Round($passes/$count, 4) } else { 0.0 }
  $overall = if($count -eq 0){ 'N/A' } else { (($rate -ge $passMin) ? 'PASS' : 'FAIL') }
  [pscustomobject]@{
    overall    = $overall
    wf         = [pscustomobject]@{ pass_rate = $rate }
    runs_count = $count
    normalized = $true
    pass_min   = $passMin
  }
}

# 3) 正規化（list 或 null → 物件）
try {
  $raw = (Get-Content $outFile -Raw) | ConvertFrom-Json -NoEnumerate
  $isDict = $raw -is [System.Collections.IDictionary]
  $isEnum = ($raw -is [System.Collections.IEnumerable]) -and -not ($raw -is [string])
  $isList = $isEnum -and -not $isDict
  if ($null -eq $raw) {
    Normalize-List @() $passMin | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $outFile -Encoding UTF8
  }
  elseif ($isList) {
    Normalize-List @($raw) $passMin | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $outFile -Encoding UTF8
  }
} catch {
  Write-Warning "Gate summary normalize failed: $($_.Exception.Message)"
}

"Gate summary: " + (Get-Item $outFile).FullName
