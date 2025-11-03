#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [Parameter(Mandatory)][string]$Start,
  [Parameter(Mandatory)][string]$End,
  [string]$UniverseFile = '.\configs\investable_universe.txt',
  [double]$Qps,
  [int]$BatchSize,
  [int]$MaxConcurrency,
  [int]$MaxRetries,
  [int]$RetryDelaySec
)
Set-StrictMode -Version Latest; $ErrorActionPreference='Stop'
switch ('chip') {
  'prices' { $DoPrices=$true;  $DoChip=$false; $DoDividend=$false; $DoPER=$false }
  'chip'   { $DoPrices=$false; $DoChip=$true;  $DoDividend=$false; $DoPER=$false }
}
$extra = @{}; foreach($k in 'Qps','BatchSize','MaxConcurrency','MaxRetries','RetryDelaySec'){
  if($PSBoundParameters.ContainsKey($k)){ $extra[$k] = $PSBoundParameters[$k] }
}
. .\tools\daily\Backfill-RatePlan.fast.ps1 -Start $Start -End $End -UniverseFile $UniverseFile @extra