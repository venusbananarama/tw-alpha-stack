param([Parameter(ValueFromRemainingArguments=$true)] $Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\gate\Run-Phase1Gate.ps1" @Args
