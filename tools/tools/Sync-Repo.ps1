#requires -Version 7.0
<#
功能：一次完成「忽略備份/隔離 → 從索引移除已追蹤備份 → 納入所有新增/移動/刪除 → 安全推送（no‑rebase；遇 non‑FF 自動 ours‑merge + --force-with-lease）」。
#>
param()
$ErrorActionPreference='Stop'
# 進 repo 根
Set-Location (Split-Path -Parent $PSCommandPath); Set-Location ..\..
[Environment]::CurrentDirectory = (Get-Location).Path

# 固化 no-rebase
git rebase --abort 2>$null; git merge --abort 2>$null
git config --local pull.rebase false
git config --local rebase.autoStash false
git config --local pull.ff only

# ignore 備份/隔離（冪等）
$gi='.gitignore'
if(-not (Test-Path $gi)){New-Item $gi -ItemType File|Out-Null}
$block=@"
# --- ai: backup & quarantine ignore (idempotent) ---
backups/
.bak/
*.bak
*.bak_*
*.bak*
*.__pre_*
tools/_quarantine/
# -----------------------------------------------
"@
foreach($line in ($block -split "`r?`n")){
  if($line -and -not (Select-String -Path $gi -Pattern ([regex]::Escape($line)) -SimpleMatch -Quiet)){
    Add-Content $gi $line
  }
}

# 從索引移除已追蹤備份（不刪本機）
$toUntrack = git ls-files -z | % { $_ -split "`0" } | ? { $_ } |
             ? { $_ -match '(^|/)(backups/|\.bak/|tools/_quarantine/)' -or $_ -match '\.bak($|_)' -or $_ -like '*.bak*' -or $_ -match '\.__pre_' }
foreach($p in $toUntrack){ git rm --cached --ignore-unmatch -- "$p" | Out-Null }

# 送交 + 安全推
git add -A
$staged = git diff --cached --name-only
if($staged){ git commit -m ("sync: {0} local changes (add/mv/del); exclude backups" -f (Get-Date -Format 'yyyy-MM-dd HH:mm')) }

git push -u origin main
if($LASTEXITCODE -ne 0){
  git fetch origin --tags
  git merge --allow-unrelated-histories -s ours --no-edit origin/main
  git push --force-with-lease origin HEAD:main
}
