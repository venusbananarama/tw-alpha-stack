$ErrorActionPreference = "Stop"

function Resolve-ProjectRoot {
  # 依序嘗試：PSScriptRoot、目前測試檔路徑、既知專案根、目前工作目錄
  $candidates = @()
  if($script:PSCommandPath){ $candidates += (Split-Path -Parent $script:PSCommandPath) }
  if($PSScriptRoot){         $candidates += $PSScriptRoot }
  if($MyInvocation.MyCommand.Path){ $candidates += (Split-Path -Parent $MyInvocation.MyCommand.Path) }
  $candidates += 'C:\AI\tw-alpha-stack'
  $candidates += (Resolve-Path .).Path
  $candidates = $candidates | Where-Object { $_ } | Select-Object -Unique

  foreach($base in $candidates){
    foreach($p in @($base, (Split-Path -Parent $base))){
      if($p -and (Test-Path (Join-Path $p 'tools\gate\Run-WFGate.ps1'))){
        return (Resolve-Path $p).Path
      }
    }
  }
  throw "找不到專案根（tools\gate\Run-WFGate.ps1 不存在）。目前候選：$($candidates -join '; ')"
}

Describe 'GateWrapper' {
  BeforeAll {
    $root = Resolve-ProjectRoot
    Push-Location $root
    New-Item -ItemType Directory -Force -Path .\reports | Out-Null
    $W = '.\tools\gate\Run-WFGate.ps1'
  }
  AfterAll { Pop-Location }

  It 'Strict → Exit 0' {
    @(@{ run_id='r1'; sharpe=1.2; max_drawdown_pct=0.18; wf_pass_rate=0.9; dsr_after_costs=0.1;
         psr=1.0; t=5; capacity_ok=$true; execution_replay_mae_bps=1.2 }) |
      ConvertTo-Json | Set-Content .\reports\gate_summary.json
    pwsh -NoProfile -File $W -SkipRunner -SmokeOK:$false
    $LASTEXITCODE | Should Be 0
  }

  It 'Smoke → Exit 2' {
    @(@{ run_id='r2'; sharpe=1.1; max_drawdown_pct=20; wf_pass_rate=0.85; dsr_after_costs=0.01;
         psr=0.6; t=1; capacity_ok=$true; execution_replay_mae_bps=5 }) |
      ConvertTo-Json | Set-Content .\reports\gate_summary.json
    pwsh -NoProfile -File $W -SkipRunner -SmokeOK:$true
    $LASTEXITCODE | Should Be 2
  }

  It 'Fail → Exit 1' {
    @(@{ run_id='r3'; sharpe=0.8; max_drawdown_pct=0.25; wf_pass_rate=0.9; dsr_after_costs=-0.1;
         psr=1.1; t=10; capacity_ok=$true; execution_replay_mae_bps=1 }) |
      ConvertTo-Json | Set-Content .\reports\gate_summary.json
    pwsh -NoProfile -File $W -SkipRunner
    $LASTEXITCODE | Should Be 1
  }
}

