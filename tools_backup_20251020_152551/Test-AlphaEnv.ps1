param([Parameter(ValueFromRemainingArguments=$true)] $Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\devops\Test-AlphaEnv.ps1" @Args
