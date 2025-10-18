param(
  [string]$RepoSlug = "venusbananarama/tw-alpha-stack",
  [long]  $PartMiB  = 1900,
  [string[]]$ExtraAssets
)
$ErrorActionPreference = "Stop"
function Require-Cmd { param([string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) { throw "缺少指令: $Name" }
}
Require-Cmd git; Require-Cmd gh

$tag  = "handover-" + (Get-Date -Format "yyyyMMdd-HHmm")
$root = (Get-Location).Path
$tmp  = Join-Path $env:TEMP ("handover_" + $tag)
if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
New-Item $tmp -ItemType Directory | Out-Null

# 只收交接必要（排除重物與敏感）
robocopy $root $tmp /MIR /XD datahub .venv _archive execution_replay .git /XF *.parquet *.arrow *.feather *.pkl *.zip *.csv.gz *.html *.xlsx *.log *.bak* .env secrets.env reports/tree_snapshot.txt | Out-Null

# 打包 + 雜湊
$zip = Join-Path $root ("AlphaCity_handover_{0}.zip" -f (Get-Date -Format "yyyyMMdd"))
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path (Join-Path $tmp '*') -DestinationPath $zip -CompressionLevel Optimal
$sha = (Get-FileHash $zip -Algorithm SHA256).Hash
$shaFile = "$zip.sha256.txt"
"$sha  $(Split-Path $zip -Leaf)" | Out-File $shaFile -Encoding ascii

# >2GiB 切片
$assets = @($zip, $shaFile)
if ((Get-Item $zip).Length -ge 2GB) {
  $outDir = Join-Path $root ("handover_parts_" + (Get-Date -Format "yyyyMMdd"))
  New-Item $outDir -ItemType Directory | Out-Null
  $chunk = $PartMiB * 1MB
  $buf   = New-Object byte[] (8MB)
  $in = [IO.File]::OpenRead($zip); $i=0
  try {
    while ($in.Position -lt $in.Length) {
      $part = Join-Path $outDir ("{0}.part{1:D3}" -f (Split-Path $zip -Leaf), $i)
      $o = [IO.File]::Open($part,'Create','Write')
      try {
        $w = 0L
        while ($w -lt $chunk) {
          $r = $in.Read($buf,0,$buf.Length)
          if ($r -le 0) { break }
          $o.Write($buf,0,$r); $w += $r
        }
      } finally { $o.Dispose() }
      $i++
    }
  } finally { $in.Dispose() }
  Remove-Item $zip -Force
  $assets = @(Get-ChildItem $outDir -File | Sort-Object Name | ForEach-Object { $_.FullName }) + $shaFile
}

# 額外資產（存在才加）
if ($ExtraAssets) {
  $valid = @()
  foreach ($p in $ExtraAssets) { if (Test-Path $p) { $valid += $p } }
  if ($valid) { $assets += $valid }
}

git tag -a $tag -m ("Monthly handover bundle ({0})" -f $tag)
git push origin $tag

$notes = @"
Handover bundle: $tag

內容：
- 交接 ZIP 或切片（如有）
- SHA256：$sha

復原：若為切片，請見 docs/Handover.md。
"@
$notesFile = "RELEASE_NOTES_$tag.txt"
$notes | Out-File $notesFile -Encoding utf8
gh release create $tag -R $RepoSlug -t $tag -F $notesFile --verify-tag --latest $assets
Write-Host ("Release: https://github.com/{0}/releases/tag/{1}" -f $RepoSlug,$tag) -ForegroundColor Green
