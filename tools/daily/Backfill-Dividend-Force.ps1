#requires -Version 7
[CmdletBinding(PositionalBinding = $false)]
param(
  [Parameter(Mandatory)][string]$Start,
  [Parameter(Mandatory)][string]$End,
  [string]$RootPath = "C:\AI\tw-alpha-stack"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
Set-Location $RootPath

# 防遞迴守門
if ($env:DIVIDEND_FORCE_GUARD -eq '1') { return }
$env:DIVIDEND_FORCE_GUARD = '1'

Write-Host "Dividend backfill $Start → $End" -ForegroundColor Cyan

function Invoke-RatePlan {
  $rp = ".\tools\daily\Backfill-RatePlan.ps1"
  if(Test-Path $rp){
    Write-Host "→ Using RatePlan: $rp" -ForegroundColor DarkCyan
    $env:BACKFILL_DATASETS = 'dividend'
    $env:FINMIND_QPS = $env:FINMIND_QPS ?? '1.67'
    $env:FINMIND_RPM = $env:FINMIND_RPM ?? '100'
    try{
      & $rp -Start $Start -End $End -InformationAction Continue -ErrorAction Stop
      return $true
    } catch {
      Write-Warning ("RatePlan failed: " + $_.Exception.Message)
      return $false
    }
  }
  return $false
}

function Invoke-PythonDividend {
  Write-Host "→ Using Python dividend backfill" -ForegroundColor DarkCyan
  $env:PYTHONUNBUFFERED = '1'
  $env:FINMIND_QPS = $env:FINMIND_QPS ?? '1.67'
  $env:FINMIND_RPM = $env:FINMIND_RPM ?? '100'

  $py = @(".\.venv\Scripts\python.exe","python","py -3") | Where-Object { $_ }
  $script = if(Test-Path ".\scripts\dividend_backfill.py"){ ".\scripts\dividend_backfill.py" }
            elseif(Test-Path ".\scripts\finmind_backfill.py"){ ".\scripts\finmind_backfill.py" }
            else { $null }
  if(-not $script){ return $false }

  foreach($p in $py){
    try{
      $psi = [System.Diagnostics.ProcessStartInfo]::new()
      $psi.FileName = $p
      if($script -like "*finmind_backfill.py"){
        $psi.Arguments = "$script --datasets dividend --start $Start --end $End"
      } else {
        $psi.Arguments = "$script --start $Start --end $End"
      }
      $psi.UseShellExecute = $false
      $psi.RedirectStandardOutput = $true
      $psi.RedirectStandardError  = $true
      $proc = [System.Diagnostics.Process]::Start($psi)

      # 120 秒看門狗 + 即時轉印
      $deadline = (Get-Date).AddSeconds(120)
      while(-not $proc.HasExited){
        while($proc.StandardOutput.Peek() -ge 0){ $line=$proc.StandardOutput.ReadLine(); if($line){ Write-Host $line } }
        while($proc.StandardError.Peek()  -ge 0){ $eline=$proc.StandardError.ReadLine(); if($eline){ Write-Warning $eline } }
        if((Get-Date) -ge $deadline){
          Write-Warning "Python dividend 120s 未完成，停止並改走 RatePlan"
          try{ $proc.Kill($true) } catch {}
          return $false
        }
        Start-Sleep -Milliseconds 200
      }
      while(-not $proc.StandardOutput.EndOfStream){ Write-Host ($proc.StandardOutput.ReadLine()) }
      while(-not $proc.StandardError.EndOfStream ){ Write-Warning ($proc.StandardError.ReadLine()) }

      return $proc.ExitCode -eq 0
    } catch {
      Write-Warning $_.Exception.Message
    }
  }
  return $false
}

$ran = Invoke-RatePlan
if(-not $ran){
  $ran = Invoke-PythonDividend
  if(-not $ran){ $ran = Invoke-RatePlan }
}

if($ran){
  $okDir = ".\_state\ingest\dividend"   # ← 僅寫 ingest
  New-Item -ItemType Directory -Path $okDir -Force | Out-Null
  "" | Out-File -Encoding ascii -Force (Join-Path $okDir "$Start.ok")
  Write-Host "OK dividend checkpoint: $Start @ _state\ingest" -ForegroundColor Green
} else {
  throw "Dividend backfill 最終仍未成功。"
}

