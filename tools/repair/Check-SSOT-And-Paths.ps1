Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-ProjectRoot {
  param([Parameter(Mandatory=$true)][string]$Candidate)
  if (Test-Path -LiteralPath $Candidate) { return (Resolve-Path -LiteralPath $Candidate).Path }
  throw "Root path not found: $Candidate"
}

function Ensure-Dir {
  param([Parameter(Mandatory=$true)][string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
  }
  return (Resolve-Path -LiteralPath $Path).Path
}

function Assert-File {
  param([Parameter(Mandatory=$true)][string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Required file not found: $Path"
  }
  return (Resolve-Path -LiteralPath $Path).Path
}

function Get-FileSha256 {
  param([Parameter(Mandatory=$true)][string]$Path)
  return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Read-RulesSchemaVer {
  param([Parameter(Mandatory=$true)][string]$RulesFile)
  $m = Select-String -Path $RulesFile -Pattern '^\s*schema_ver:\s*([^\s#]+)' -Encoding UTF8 | Select-Object -First 1
  if ($m -and $m.Matches.Count -gt 0) { return $m.Matches[0].Groups[1].Value.Trim() }
  return $null
}

function Write-RunManifest {
  param(
    [Parameter(Mandatory=$true)][string]$ManifestPath,
    [Parameter(Mandatory=$true)][string]$SsotHash,
    [string]$SchemaVer = $null
  )
  $obj = if (Test-Path -LiteralPath $ManifestPath) {
    try { Get-Content -LiteralPath $ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json } catch { [ordered]@{} }
  } else { [ordered]@{} }

  $obj.ssot_hash = $SsotHash
  if ($SchemaVer) { $obj.schema_ver = $SchemaVer }

  ($obj | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $ManifestPath -Encoding UTF8
  return $ManifestPath
}
