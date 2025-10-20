function Invoke-Py {
  [CmdletBinding()]
  param(
    [string] $PythonExe = '.\.venv\Scripts\python.exe',
    [Parameter(ValueFromRemainingArguments=$true)]
    [Alias("Args","ArgList")] [string[]] $PyArgs
  )
  if(-not (Test-Path -LiteralPath $PythonExe)){ throw "Python not found: $PythonExe" }
  if(-not $PyArgs -or $PyArgs.Count -eq 0){ throw "Invoke-Py 需要 -PyArgs。用法：Invoke-Py -PyArgs @('scripts\preflight_check.py','--help')" }
  Write-Host ("PY » " + ($PyArgs -join ' ')) -ForegroundColor DarkCyan
  & $PythonExe @PyArgs
  if($LASTEXITCODE -ne 0){ throw "Python exited with code $LASTEXITCODE" }
}
