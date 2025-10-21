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

# 1) 執行 runner（注意：runner 目前只掃 $Dir 裡的 *.csv）
& $Python $Runner --dir $Dir --export $outFile

# 2) 讀 Gate 門檻（預設 0.80；若 rules.yaml 有 wf_pass_min 就覆蓋）
$passMin = 0.80
$rulesPath = ".\configs\rules.yaml"
if (Test-Path $rulesPath) {
  $m = Select-String -Path $rulesPath -Pattern 'wf_pass_min\s*:\s*([0-9]*\.?[0-9]+)' -AllMatches | Select-Object -First 1
  if ($m) { $passMin = [double]$m.Matches[0].Groups[1].Value }
}

# 3) 正規化（list 或 null → object），並把 gate.ok/gate.status 納入通過判斷
try {
  $rawText = Get-Content $outFile -Raw
  $raw = $rawText | ConvertFrom-Json -NoEnumerate
  $isDict = $raw -is [System.Collections.IDictionary]
  $isList = ($raw -is [System.Collections.IEnumerable]) -and -not ($raw -is [string]) -and -not $isDict
  if ($null -eq $raw -or $isList) {
    $runs = if ($null -eq $raw) { @() } else { @($raw) }
    $passes = 0
    foreach($it in $runs){
      if($null -eq $it){ continue }
      $ok = $false

      # 3a) 優先採用 gate.ok / gate.status
      if($it.PSObject.Properties['gate']){
        $gate = $it.gate
        if($gate -and $gate.PSObject.Properties['ok'] -and [bool]$gate.ok){ $ok = $true }
        elseif($gate -and $gate.PSObject.Properties['status'] -and ($gate.status -match 'PASS')){ $ok = $true }
      }

      # 3b) 後備判斷（兼容早期欄位）
      if(-not $ok){
        $ov = $it.PSObject.Properties['overall']?.Value
        if($ov -and ($ov -match 'PASS')) { $ok = $true }
      }
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
    $count = $runs.Count
    $rate  = if($count){ [math]::Round($passes/$count,4) } else { 0.0 }
    $overall = if($count -eq 0){ 'N/A' } else { ( if($rate -ge $passMin){ 'PASS' } else { 'FAIL' } ) }
    $obj = [pscustomobject]@{
      overall    = $overall
      wf         = [pscustomobject]@{ pass_rate = $rate }
      runs_count = $count
      normalized = $true
      pass_min   = $passMin
    }
    $obj | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $outFile -Encoding UTF8
  }
} catch {
  Write-Warning "Gate summary normalize failed: $($_.Exception.Message)"
}

"Gate summary: " + (Get-Item $outFile).FullName
