param([Parameter(ValueFromRemainingArguments=$true)] $Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\devops\Generate-LLMBrief.ps1" @Args
