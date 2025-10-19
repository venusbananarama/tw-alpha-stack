[CmdletBinding(DefaultParameterSetName="plan")]
param(
  [Parameter(Mandatory, Position=0)]
  [ValidateSet("plan","apply","status","rollback")]
  [string]$Mode,
  [string]$Root   = (Resolve-Path .).Path,
  [string]$OutDir = ".\reports\refactor",
  [switch]$MakeShims = $true,
  [switch]$Confirm   # apply 時必須帶上
)
$ErrorActionPreference='Stop'
Set-Location $Root
if(-not (Test-Path $OutDir)){ New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }
$tools = Join-Path $Root 'tools'

# 分類規則（僅治理 tools 根層）
$rules = @(
  @{ Re='^Backfill-FullMarket\.ps1$';                         To='fullmarket' },
  @{ Re='^Run-FullMarket-DateID-MaxRate\.ps1$';               To='fullmarket' },  # 正名
  @{ Re='^Run-FullMarket-DateIDMaxRate\.ps1$';                To='fullmarket' },  # 舊名（只留 shim）
  @{ Re='^Launch-DateID\.ps1$|^Run-DateID(\-.+)?\.ps1$|^DateId-Checkpoint\.ps1$'; To='dateid' },
  @{ Re='^Daily-Backfill-.+\.ps1$|^Backfill-Dividend-Force\.ps1$|^Preflight-Backfill\.ps1$'; To='daily' },
  @{ Re='^Run-(WFGate|Phase1Gate|Phase1-Validation)\.ps1$|^Run-Preflight-V2\.ps1$|^Run-(SmokeTests|MinimalSmoke)\.ps1$'; To='gate' },
  @{ Re='^Build-IDsFromUniverse\.ps1$|^Build-Universe-Failsafe\.ps1$|^Select-IDs\.ps1$|^Run-LayoutCheck\.ps1$'; To='universe' },
  @{ Re='^Run-Max-Recent\.ps1$|^Smoke-Backfill\.ps1$|^Run-Max-SmartBackfill\.ps1$|^Start-Max-Orchestrator\.ps1$|^Stop-Alpha\.ps1$|^Tail-MaxRate\.ps1$|^Monitor-Backfill\.ps1$|^Decide-After6min\.ps1$|^AckLive\.ps1$|^Watch-DateId-HUD\.ps1$|^Summarize-DateId-Progress\.ps1$|^Build-HistoricalCheckpoint\.ps1$'; To='orchestrator' },
  @{ Re='^Publish-Handover\.ps1$|^Switch-GitHubAccount\.ps1$|^Generate-LLMBrief\.ps1$|^Test-RepoHealth\.ps1$|^Test-AlphaEnv\.ps1$'; To='devops' },
  @{ Re='^Repair-Code6\.ps1$|^Normalize-Calendar\.ps1$|^Standardize-ProjectLayout\.ps1$|^Check-CanonicalLayout\.ps1$|^Check-SSOT-And-Paths\.ps1$|^Check-FMStatus\.ps1$|^Verify-ParquetIntegrity\.ps1$'; To='repair' }
)

function Get-MovePlan {
  $plan=@()
  Get-ChildItem $tools -File -Filter '*.ps1' | Where-Object { $_.DirectoryName -eq $tools } | ForEach-Object {
    $name=$_.Name
    $rule=$rules | Where-Object { $name -match $_.Re } | Select-Object -First 1
    if($rule){
      $plan += [pscustomobject]@{
        Name = $name
        Src  = $_.FullName
        Dest = Join-Path (Join-Path $tools $rule.To) $name
        Cat  = $rule.To
      }
    }
  }
  $plan
}

function Scan-Refs($names){
  if(-not $names){ return @() }
  $patterns = $names | ForEach-Object { [regex]::Escape($_) -replace '\\\.ps1$','\.ps1' }
  $re = '(\.\\|tools\\|\.\/|tools\/)?(' + ($patterns -join '|') + ')'
  $refs=@()
  Get-ChildItem $Root -Recurse -File -Include *.ps1,*.psm1,*.psd1,*.cmd,*.bat,*.md,*.yml,*.yaml,*.json |
    ForEach-Object {
      $hit = Select-String -Path $_.FullName -Pattern $re -AllMatches -ErrorAction SilentlyContinue
      if($hit){
        foreach($m in $hit){
          $refs += [pscustomobject]@{ File=$_.FullName; Line=$m.Line.Trim(); Match=$m.Matches.Value }
        }
      }
    }
  $refs
}

function Save-Plan($plan,$refs){
  $planPath = Join-Path $OutDir 'move_plan.csv'
  $refsPath = Join-Path $OutDir 'cross_refs.csv'
  $treePath = Join-Path $OutDir 'tree_after_simulate.txt'
  $plan | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $planPath
  $refs | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $refsPath
  $lines=@()
  foreach($grp in ($plan | Group-Object Cat)){
    $lines += "## $($grp.Name)"
    foreach($it in $grp.Group){ $lines += "  $(Split-Path -Leaf $($it.Dest))" }
    $lines += ""
  }
  Set-Content -Path $treePath -Encoding UTF8 -Value $lines
  @($planPath,$refsPath,$treePath)
}

function Ensure-Dirs {
  foreach($c in 'fullmarket','dateid','daily','gate','universe','orchestrator','devops','repair','_legacy'){
    $p = Join-Path $tools $c
    if(-not (Test-Path $p)){ New-Item -ItemType Directory -Path $p -Force | Out-Null }
  }
}

function Backup-Tools {
  $backupDir = Join-Path $Root ("tools_backup_{0:yyyyMMdd_HHmmss}" -f (Get-Date))
  Copy-Item $tools $backupDir -Recurse -Force
  $backupDir
}

function Make-Shim($dest){
  $shim = Join-Path $tools (Split-Path $dest -Leaf)
  if(Test-Path $shim){ return }
@"
param([Parameter(ValueFromRemainingArguments=`$true)] `$Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "$(Resolve-Path $dest)" @Args
"@ | Set-Content -LiteralPath $shim -Encoding UTF8
}

function Canonicalize-FullMarket {
  $canon = Join-Path (Join-Path $tools 'fullmarket') 'Run-FullMarket-DateID-MaxRate.ps1'
  $alt   = Join-Path (Join-Path $tools 'fullmarket') 'Run-FullMarket-DateIDMaxRate.ps1'
  if((Test-Path $alt) -and (Test-Path $canon)){
    Remove-Item $alt -Force
    $shimAlt = Join-Path $tools 'Run-FullMarket-DateIDMaxRate.ps1'
@"
param([Parameter(ValueFromRemainingArguments=`$true)] `$Args)
pwsh -NoProfile -ExecutionPolicy Bypass -File "$canon" @Args
"@ | Set-Content -LiteralPath $shimAlt -Encoding UTF8
  }
}

function QuickSyntax($files){
  foreach($f in ($files | Select-Object -Unique)){
    $t=$null;$e=$null
    [System.Management.Automation.Language.Parser]::ParseFile($f,[ref]$t,[ref]$e) | Out-Null
    if($e){ throw "ParserError: $f`n$($e|Out-String)" }
  }
}

switch($Mode){
  'plan' {
    $plan = Get-MovePlan
    $refs = Scan-Refs ($plan.Name)
    $out = Save-Plan $plan $refs
    "📝 Plan saved: $($out -join ', ')"
  }
  'apply' {
    if(-not $Confirm){ throw "為避免誤觸，請加 -Confirm 再執行 apply" }
    $plan = Import-Csv (Join-Path $OutDir 'move_plan.csv') -ErrorAction Stop
    if(-not $plan){ throw "move_plan.csv 為空，請先跑 plan" }

    # Git 乾淨工作樹（若有 git）
    $st = (git status --porcelain) 2>$null
    if($LASTEXITCODE -eq 0 -and $st){ throw "Git 工作樹非乾淨，請先 commit/stash 後再試。" }

    Ensure-Dirs
    $backup = Backup-Tools
    "✅ Backup: $backup"

    foreach($i in $plan){
      $dstDir = Split-Path -Parent $i.Dest
      if(-not (Test-Path $dstDir)){ New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }
      Move-Item -LiteralPath $i.Src -Destination $i.Dest -Force
      "➡️  Moved: $($i.Src) -> $($i.Dest)"
      if($MakeShims){ Make-Shim $i.Dest }
    }

    Canonicalize-FullMarket
    QuickSyntax ($plan.Dest)
    "🎉 apply 完成。舊路徑仍可用（shim）。"
  }
  'status' {
    "Repo: $Root"
    "tools root .ps1 count: " + (Get-ChildItem $tools -File -Filter *.ps1 | ?{ $_.DirectoryName -eq $tools }).Count
    "subfolder .ps1 count  : " + (Get-ChildItem $tools -Recurse -File -Filter *.ps1 | ?{ $_.DirectoryName -ne $tools }).Count
    foreach($c in 'fullmarket','dateid','daily','gate','universe','orchestrator','devops','repair','_legacy'){
      $p = Join-Path $tools $c
      $n = (Get-ChildItem $p -File -Filter *.ps1 -ErrorAction SilentlyContinue).Count
      "{0,-14}: {1}" -f $c,$n
    }
  }
  'rollback' {
    $last = Get-ChildItem $Root -Directory -Filter 'tools_backup_*' | Sort-Object LastWriteTime -Desc | Select-Object -First 1
    if(-not $last){ throw "找不到 tools_backup_*" }
    "⚠️ 將以 $($last.FullName) 覆蓋 tools\ （5 秒後開始；Ctrl+C 可取消）"
    Start-Sleep -Seconds 5
    Remove-Item $tools -Recurse -Force
    Copy-Item $last.FullName $tools -Recurse -Force
    "✅ Rolled back from $($last.FullName)"
  }
}
