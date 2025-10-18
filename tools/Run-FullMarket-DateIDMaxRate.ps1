param([string]$Start,[string]$End="",[int]$BatchSize=400,[int]$WindowDays=7,[int]$Workers=4,[switch]$Strict)
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$real = Join-Path $here "Run-FullMarket-DateID-MaxRate.ps1"
if(-not (Test-Path $real)){ throw "找不到 $real" }
$argv = @("-File",$real,"-Start",$Start)
if($End){ $argv += @("-End",$End) }
$argv += @("-BatchSize",$BatchSize,"-WindowDays",$WindowDays,"-Workers",$Workers)
if($Strict){ $argv += "-Strict" }
& pwsh -NoProfile -ExecutionPolicy Bypass @argv
