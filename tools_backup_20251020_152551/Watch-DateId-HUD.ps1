param([Parameter(ValueFromRemainingArguments=$true)] $Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\orchestrator\Watch-DateId-HUD.ps1" @Args
