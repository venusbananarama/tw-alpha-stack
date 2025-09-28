if (-not $Env:TW_ALPHA_ROOT) { $Env:TW_ALPHA_ROOT = 'G:\AI\tw-alpha-stack' }
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Env:PYTHONIOENCODING='utf-8'; $Env:PYTHONUTF8='1'; $Env:PYTHONUNBUFFERED='1'

function acd    { param([string]$Sub='') Set-Location (Join-Path $Env:TW_ALPHA_ROOT $Sub) }
function acopen { ii $Env:TW_ALPHA_ROOT }
function acvenv { $act=Join-Path $Env:TW_ALPHA_ROOT '.venv\Scripts\Activate.ps1'; if(Test-Path $act){ & $act } else { Write-Host '[AlphaCity] venv not found' -ForegroundColor Red } }
function acpy   { param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Args) & (Join-Path $Env:TW_ALPHA_ROOT '.venv\Scripts\python.exe') -X utf8 @Args }
function ack    { param([switch]$Quick,[string]$Symbol='2330.TW',[double]$Qps=1.6,[int]$Workers=6); $ps1=Join-Path $Env:TW_ALPHA_ROOT 'scripts\ps\Invoke-AlphaVerification.ps1'; if($Quick){ & $ps1 -Quick -Symbol $Symbol -Workers $Workers -Qps $Qps -VerboseCmd } else { & $ps1 -Start '2015-01-01' -End (Get-Date).ToString('yyyy-MM-dd') -Symbol $Symbol -Workers $Workers -Qps $Qps -VerboseCmd } }