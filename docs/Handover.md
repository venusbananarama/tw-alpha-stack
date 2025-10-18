# Handover 操作（切片復原）
將同一個 Release 內的 `*.part***` 與 `.sha256.txt` 下載到同一路徑，PowerShell 合併：

```powershell
param([string]$FirstPart, [string]$OutputZip = "restored.zip")
$base = Split-Path $FirstPart -Leaf
$prefix = $base -replace '\.part\d{3}$',''
$parts = Get-ChildItem -Filter "$prefix.part*" | Sort-Object Name
$buf = New-Object byte[] (8MB)
$out = [IO.File]::Open($OutputZip,'Create','Write')
try{ foreach($p in $parts){ $in=[IO.File]::OpenRead($p.FullName); try{
  while(($r=$in.Read($buf,0,$buf.Length)) -gt 0){ $out.Write($buf,0,$r) }
} finally{ $in.Dispose() } } } finally{ $out.Dispose() }
