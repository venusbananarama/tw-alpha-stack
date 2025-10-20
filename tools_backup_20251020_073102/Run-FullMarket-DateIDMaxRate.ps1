param([Parameter(ValueFromRemainingArguments=$true)] $Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1" @Args
