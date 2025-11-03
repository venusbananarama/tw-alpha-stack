#requires -Version 7.0
[CmdletBinding()]
param(
  [string[]]$Roots = @('.\tools'),
  [int]$TopN = 12,
  [int]$HugeKB = 256,
  [int]$VeryHugeKB = 1024
)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$root = (Resolve-Path .).Path
function Rel([string]$p){
  try { return [IO.Path]::GetRelativePath($root,(Resolve-Path $p -EA SilentlyContinue).Path) } catch { return $p }
}

# 收集 .ps1
$files = foreach($r in $Roots){ if(Test-Path -LiteralPath $r){ Get-ChildItem -Recurse -File -LiteralPath $r -Filter *.ps1 } }
if(-not $files){ Write-Warning "No .ps1 files under: $($Roots -join ', ')"; return }

# SHA256
$sha = [System.Security.Cryptography.SHA256]::Create()

$rows = foreach($f in $files){
  try{
    $bytes = [IO.File]::ReadAllBytes($f.FullName)
    $hash  = -join ($sha.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') })
    $text  = [System.Text.Encoding]::UTF8.GetString($bytes)
    $sizeKB = [Math]::Round($bytes.Length/1KB,2)
    $lineCount = ($text -split "`r?`n").Count
    $hasBinary = $bytes -contains 0
    $hasWrapperTag = ($text -match 'Auto-generated wrapper') -or ($text -match 'No target found. Tried:')
    $hasPwshPrompt = $text -match '(^|\r?\n)PS [A-Z]:\\'
    $hasMergeMarks = $text -match '<<<<<<<|>>>>>>|======='
    $wrapperRepeat = ([regex]::Matches($text,'Auto-generated wrapper')).Count

    $category  = if($f.FullName -match '\\orchestrator\\'){'orchestrator'}
                 elseif($f.FullName -match '\\fullmarket\\'){'fullmarket'}
                 elseif($f.FullName -match '\\dateid\\'){'dateid'}
                 elseif($f.FullName -match '\\daily\\'){'daily'}
                 elseif($f.FullName -match '\\gate\\'){'gate'}
                 else{'tools'}

    $score = 0
    if($sizeKB -ge $HugeKB){     $score += 2 }
    if($sizeKB -ge $VeryHugeKB){ $score += 2 }
    if($hasBinary){              $score += 2 }
    if($hasMergeMarks){          $score += 2 }
    if($hasPwshPrompt){          $score += 1 }
    if($wrapperRepeat -ge 2){    $score += 1 }

    # 候選備份
    $cand = @()
    if( ($category -in @('orchestrator','fullmarket','dateid')) -and (($sizeKB -ge $HugeKB) -or $hasBinary -or $hasMergeMarks) ){
      $dir = $f.DirectoryName
      $base = [IO.Path]::GetFileNameWithoutExtension($f.Name)
      $cand = Get-ChildItem -File -LiteralPath $dir -EA SilentlyContinue |
              Where-Object {
                $_.Name -match [Regex]::Escape($base) -and
                $_.Name -match '(bak|fix|rewrite|reheader|autofix|final|_bak|_fix|_rewrite)'
              } | Sort-Object LastWriteTime -Descending | Select-Object -First 3
    }

    [pscustomobject]@{
      Path = $f.FullName
      Rel  = Rel $f.FullName
      Dir  = Rel $f.DirectoryName
      Name = $f.Name
      Category = $category
      SizeKB = $sizeKB
      Lines  = $lineCount
      Hash   = $hash
      HasBinary = $hasBinary
      IsWrapper = $hasWrapperTag
      HasMergeMarks = $hasMergeMarks
      HasPwshPrompt = $hasPwshPrompt
      WrapperRepeats = $wrapperRepeat
      IsHuge = ($sizeKB -ge $HugeKB)
      IsVeryHuge = ($sizeKB -ge $VeryHugeKB)
      Score = $score
      Preview = ($text.Substring(0,[Math]::Min($text.Length,160))).Replace("`r"," ").Replace("`n"," ")
      Cand1 = if($cand.Count -ge 1){ $cand[0].FullName } else { $null }
      Cand2 = if($cand.Count -ge 2){ $cand[1].FullName } else { $null }
      Cand3 = if($cand.Count -ge 3){ $cand[2].FullName } else { $null }
    }
  } catch {
    [pscustomobject]@{
      Path=$f.FullName; Rel=Rel $f.FullName; Dir=Rel $f.DirectoryName; Name=$f.Name; Category='?'
      SizeKB=0; Lines=0; Hash=''; HasBinary=$false; IsWrapper=$false; HasMergeMarks=$false; HasPwshPrompt=$false
      WrapperRepeats=0; IsHuge=$false; IsVeryHuge=$false; Score=99; Preview="[READ-ERROR] $($_.Exception.Message)"
      Cand1=$null; Cand2=$null; Cand3=$null
    }
  }
}

# 重複檔（hash 相同）
$dups = $rows |
  Group-Object -Property Hash |
  Where-Object { $_.Count -gt 1 -and $_.Name } |
  ForEach-Object {
    $paths = ($_.Group | ForEach-Object { $_.Rel }) -join '; '
    [pscustomobject]@{
      Hash  = $_.Name
      Count = $_.Count
      Paths = $paths
    }
  }

# 輸出報表
$repDir = Join-Path $root 'reports'
if(-not (Test-Path -LiteralPath $repDir)){ New-Item -ItemType Directory -Force -Path $repDir | Out-Null }
$ts = Get-Date -Format yyyyMMdd_HHmmss
$allJson = Join-Path $repDir ("audit_tools_integrity_{0}.json" -f $ts)
$allCsv  = Join-Path $repDir ("audit_tools_integrity_{0}.csv"  -f $ts)
$badCsv  = Join-Path $repDir ("audit_tools_suspects_{0}.csv"   -f $ts)
$dupCsv  = Join-Path $repDir ("audit_tools_duplicates_{0}.csv" -f $ts)

$rows | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $allJson -Encoding utf8NoBOM
$rows | Export-Csv -NoTypeInformation -Path $allCsv
$suspects = $rows | Where-Object { $_.Score -ge 3 -or $_.IsVeryHuge -or $_.HasBinary -or $_.HasMergeMarks } | Sort-Object Score, SizeKB -Descending
$suspects | Export-Csv -NoTypeInformation -Path $badCsv
$dups | Export-Csv -NoTypeInformation -Path $dupCsv

# 主控台摘要
$top = $rows | Sort-Object SizeKB -Descending | Select-Object -First $TopN
Write-Host ("[AUDIT] scanned={0}  suspects]={1}  duplicates={2}" -f $rows.Count, $suspects.Count, (($dups | Measure-Object).Count))
Write-Host ("[AUDIT] reports -> {0}" -f $repDir)
Write-Host ""
Write-Host "[Largest by size]" -ForegroundColor Cyan
$top | Format-Table -AutoSize Rel,SizeKB,Category,IsWrapper,Score
Write-Host ""
Write-Host "[Suspects (top 10)]" -ForegroundColor Yellow
$suspects | Select-Object -First 10 Rel,SizeKB,Category,Score,Cand1 | Format-Table -AutoSize