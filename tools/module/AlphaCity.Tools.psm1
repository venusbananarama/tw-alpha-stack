Set-StrictMode -Version Latest
$script:ToolsRoot = Split-Path $PSScriptRoot -Parent
function Invoke-AlphaTool {
  param(
    [Parameter(Mandatory)][string]$RelPath,
    [Parameter(ValueFromRemainingArguments=$true)][object[]]$Args
  )
  if ($env:ALPHACITY_ALLOW -ne '1') { throw "ALPHACITY_ALLOW=1 not set." }
  $t = Join-Path $script:ToolsRoot $RelPath
  if(-not (Test-Path $t)){ throw "Target missing: $t" }
  & pwsh -NoProfile -ExecutionPolicy Bypass -File $t @Args
}
function Run-DateID { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'dateid\Run-DateID.ps1' @Args }
function Run-DateID-Incremental { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'dateid\Run-DateID-Incremental.ps1' @Args }
function Run-DateID-Extras { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'dateid\Run-DateID-Extras.ps1' @Args }
function Run-DateID-Extras-Fixed { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'dateid\Run-DateID-Extras-Fixed.ps1' @Args }
function Run-DateID-Heal { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'dateid\Run-DateID-Heal.ps1' @Args }
function Run-FullMarket-DateID-MaxRate { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'fullmarket\Run-FullMarket-DateID-MaxRate.ps1' @Args }
function Run-FullMarket-DateIDMaxRate { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'fullmarket\Run-FullMarket-DateID-MaxRate.ps1' @Args }
function Run-FullMarket-DateID-MaxRange { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'fullmarket\Run-FullMarket-DateID-MaxRange.ps1' @Args }
function Run-Max-Recent { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'orchestrator\Run-Max-Recent.ps1' @Args }
function Run-Max-SmartBackfill { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'orchestrator\Run-Max-SmartBackfill.ps1' @Args }
function Run-Phase1Gate { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'gate\Run-Phase1Gate.ps1' @Args }
function Run-WFGate { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'gate\Run-WFGate.ps1' @Args }
function Run-Preflight-V2 { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'gate\Run-Preflight-V2.ps1' @Args }
function Run-SmokeTests { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'gate\Run-SmokeTests.ps1' @Args }
function Run-LayoutCheck { param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args) Invoke-AlphaTool -RelPath 'repair\Check-CanonicalLayout.ps1' @Args }
Export-ModuleMember -Function Run-DateID,Run-DateID-Incremental,Run-DateID-Extras,Run-DateID-Extras-Fixed,Run-DateID-Heal,Run-FullMarket-DateID-MaxRate,Run-FullMarket-DateIDMaxRate,Run-FullMarket-DateID-MaxRange,Run-Max-Recent,Run-Max-SmartBackfill,Run-Phase1Gate,Run-WFGate,Run-Preflight-V2,Run-SmokeTests,Run-LayoutCheck
