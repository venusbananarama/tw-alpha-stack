param(
  [Parameter(Mandatory=$true)][string]$UserName,
  [Parameter(Mandatory=$true)][string]$Email,
  [string]$RepoSlug = "venusbananarama/tw-alpha-stack"
)
function Require-Cmd { param([string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) { throw "缺少指令: $Name" }
}
Require-Cmd git; Require-Cmd gh; Require-Cmd ssh
gh auth login -h github.com
git config user.name  $UserName
git config user.email $Email
git remote set-url origin ("git@github.com:{0}.git" -f $RepoSlug)
ssh -T git@github.com | Out-Host   # 提示成功但退出碼非 0，這裡不檢查
Write-Host ("Switched to {0} <{1}> for {2}" -f $UserName,$Email,$RepoSlug) -ForegroundColor Green
