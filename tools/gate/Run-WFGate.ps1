param(
  [string]$Python="./.venv/Scripts/python.exe",
  [string]$Runner="./scripts/wf_runner.py",
  [string]$Dir="./runs/wf_configs",
  [string]$Export="./reports"
)
if ($env:ALPHACITY_ALLOW -ne "1") { Write-Error "ALPHACITY_ALLOW=1 not set. Aborting." -ErrorAction Stop }
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest

if(-not (Test-Path $Export)){ New-Item -ItemType Directory -Force -Path $Export | Out-Null }
$outFile = Join-Path $Export 'gate_summary.json'

# 1) 執行原生 runner（runner 只會撿 $Dir 裡的 CSV）
& $Python $Runner --dir $Dir --export $outFile

# 2) 讀 wf_pass_min（預設 0.80）
$passMin = 0.80
$rulesPath = ".\configs\rules.yaml"
if (Test-Path $rulesPath) {
  $m = Select-String -Path $rulesPath -Pattern 'wf_pass_min\s*:\s*([0-9]*\.?[0-9]+)' -AllMatches | Select-Object -First 1
  if ($m) { $passMin = [double]$m.Matches[0].Groups[1].Value }
}

# 安全取屬性（StrictMode-safe）
function Get-Prop([object]$o, [string]$name){
  if($null -eq $o){ return $null }
  $p = $o.PSObject.Properties[$name]
  if($null -eq $p){ return $null }
  return $p.Value
}

# 3) 正規化（list 或 null -> object）
try {
  $raw = (Get-Content $outFile -Raw) | ConvertFrom-Json -NoEnumerate
  $isDict = $raw -is [System.Collections.IDictionary]
  $isList = ($raw -is [System.Collections.IEnumerable]) -and -not ($raw -is [string]) -and -not $isDict

  if ($null -eq $raw -or $isList) {
    $runs = if ($null -eq $raw) { @() } else { @($raw) }
    $passCount = 0

    foreach($it in $runs){
      if($null -eq $it){ continue }
      $ok = $false

      # gate.ok / gate.status
      $g = Get-Prop $it 'gate'
      if($g){
        $gOk     = Get-Prop $g 'ok'
        $gStatus = Get-Prop $g 'status'
        if($gOk -ne $null -and [bool]$gOk){ $ok = $true }
        elseif($gStatus -and ($gStatus -match 'PASS')){ $ok = $true }
      }

      # 兼容舊欄位
      if(-not $ok){
        $ov = Get-Prop $it 'overall'
        if($ov -and ($ov -match 'PASS')){ $ok = $true }
      }
      if(-not $ok){
        $p = Get-Prop $it 'pass'
        if($p -ne $null -and [bool]$p){ $ok = $true }
      }
      if(-not $ok){
        $wf = Get-Prop $it 'wf'
        if($wf){
          $wfp = Get-Prop $wf 'pass'
          $wfs = Get-Prop $wf 'status'
          if($wfp -ne $null -and [bool]$wfp){ $ok = $true }
          elseif($wfs -and ($wfs -match 'PASS')){ $ok = $true }
        }
      }
      if(-not $ok){
        $wf_pass = Get-Prop $it 'wf_pass'
        if($wf_pass -ne $null -and [int]$wf_pass -eq 1){ $ok = $true }
      }

      if($ok){ $passCount++ }
    }

    $count = $runs.Count
    $rate  = if($count){ [math]::Round($passCount / $count, 4) } else { 0.0 }

    $overall = 'N/A'
    if($count -gt 0){
      $overall = 'FAIL'
      if($rate -ge $passMin){ $overall = 'PASS' }
    }

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
