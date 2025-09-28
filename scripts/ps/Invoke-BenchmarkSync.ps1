#requires -Version 7.2
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

param([switch]$DryRun)
Import-Module "$PSScriptRoot/../../modules/AlphaCity.Benchmark.psm1" -Force
Set-ACEncoding
Invoke-BenchmarkSync -DryRun:$DryRun
