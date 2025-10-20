param([Parameter(ValueFromRemainingArguments=$true)] $Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\repair\Standardize-ProjectLayout.ps1" @Args
