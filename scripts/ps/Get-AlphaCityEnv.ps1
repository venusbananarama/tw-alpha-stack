#requires -Version 7.2
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module "$PSScriptRoot/../../modules/AlphaCity.Common.psm1" -Force
Import-Module "$PSScriptRoot/../../modules/AlphaCity.Env.psm1" -Force
Set-ACEncoding

"## AlphaCity Environment" | Write-Output
Get-ACEnvReport | Format-Table -AutoSize
