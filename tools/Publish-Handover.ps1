param(
  [string]$RepoSlug = "venusbananarama/tw-alpha-stack",
  [long]  $PartMiB  = 1900,
  [string[]]$ExtraAssets
)
$ErrorActionPreference = "Stop"
function Require-Cmd([string]$n){ if(-not (Get-Command $n -EA SilentlyContinue)){ throw "缺少指令: $n" } }
Require-Cmd git; Require-Cmd gh

$tag  = "handover-" + (Get-Date -Format "yyyyMMdd-HHmm")
$root = (Get-Location).Path
$tmp  = Join-Path $env:TEMP ("handover_" + $tag); if(Test-Path $tmp){ Remove-Item $tmp -Recurse -Force }
New-Item $tmp -ItemType Directory | Out-Null

# 只收交接必要（排除重物/敏感）
robocopy $root $tmp /MIR /XD datahub .venv _archive execution_replay .git `
  /XF *.parquet *.arrow *.feather *.pkl *.zip *.csv.gz *.html *.xlsx *.log *.bak* .env secrets.env reports/tree_snapshot.txt | Out-Null

# 打包 + SHA256
$zip = Join-Path $root ("AlphaCity_handover_{0}.zip" -f (Get-Date -Format "yyyyMMdd"))
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path (Join-Path $tmp '*') -DestinationPath $zip -CompressionLevel Optimal
$sha = (Get-FileHash $zip -Algorithm SHA256).Hash; $shaFile = "$zip.sha256.txt"
"$sha  $(Split-Path $zip -Leaf)" | Out-File $shaFile -Encoding ascii

# >2GiB 自動切片
$assets = @($zip,$shaFile)
if ((Get-Item $zip).Length -ge 2GB) {
  $outDir = Join-Path $root ("handover_parts_" + (Get-Date -Format "yyyyMMdd")); New-Item $outDir -ItemType Directory | Out-Null
  $chunk = $PartMiB * 1MB; $buf = New-Object byte[] (8MB); $in=[IO.File]::OpenRead($zip); $i=0
  try { while ($in.Position -lt $in.Length) {
      $part = Join-Path $outDir ("{0}.part{1:D3}" -f (Split-Path $zip -Leaf), $i)
      $o=[IO.File]::Open($part,'Create','Write'); try {
        $w=0L; while ($w -lt $chunk) { $r=$in.Read($buf,0,$buf.Length); if($r -le 0){break}; $o.Write($buf,0,$r); $w+=$r }
      } finally { $o.Dispose() }
      $i++
  } } finally { $in.Dispose() }
  Remove-Item $zip -Force
  $assets = @(Get-ChildItem $outDir -File | Sort-Object Name | ForEach-Object { $_.FullName }) + $shaFile
}

# 附加額外資產（如 _archive 內的大 zip）
if ($ExtraAssets) {
  $valid = $ExtraAssets | Where-Object { Test-Path $_ }
  if ($valid) { $assets += $valid }
}

# 建 Tag + 推 Tag + 建 Release（含 3 次重試）
git tag -a $tag -m ("Monthly handover bundle ({0})" -f $tag)
git push origin $tag
$nf = "RELEASE_NOTES_$tag.txt"; ("Handover bundle: $tag`r`n`r`nSHA256: $sha`r`n") | Out-File $nf -Encoding utf8
$ok=$false; for($i=1;$i -le 3 -and -not $ok;$i++){ gh release create $tag -R $RepoSlug -t $tag -F $nf --verify-tag --latest $assets; if($LASTEXITCODE -eq 0){ $ok=$true } else { Start-Sleep 5 } }
if(-not $ok){ throw "Release create failed after retries." }
Write-Host ("Release: https://github.com/{0}/releases/tag/{1}" -f $RepoSlug,$tag) -ForegroundColor Green
