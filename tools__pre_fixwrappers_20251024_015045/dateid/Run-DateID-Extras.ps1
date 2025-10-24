param(
  [string]$Date,
  [string]$Start,
  [string]$End,
  [string]$IDs,
  [string]$Group = "A",
  [int]$ThrottleRPM = [int]([string]::IsNullOrEmpty($env:FINMIND_THROTTLE_RPM) ? 0 : $env:FINMIND_THROTTLE_RPM),
  [string]$Tag = "A",
  [switch]$FailOnError
)

$ErrorActionPreference='Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

function TS { Get-Date -Format "yyyy-MM-dd HH:mm:ss" }
function Log([string]$type,[string]$msg){
  $line = "[{0}] {1} {2}" -f (TS), $type, $msg
  $line | Tee-Object -FilePath $script:LogFile -Append | Out-Host
}

# --- 時間窗（--end 半開） ---
if($Date){
  $from=[datetime]$Date; $to=$from.AddDays(1)
}elseif($Start){
  $from=[datetime]$Start
  $to = if([string]::IsNullOrEmpty($End)){ $from.AddDays(1) } else { [datetime]$End }
}else{
  throw "必須提供 -Date 或 -Start [-End]"
}

# --- IDs 解析：-IDs > groups\A.txt > investable_universe.txt ---
$ids=@()
if($IDs){ $ids = $IDs -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ } }
elseif(Test-Path ".\configs\groups\$Group.txt"){ $ids = Get-Content ".\configs\groups\$Group.txt" | Where-Object { $_.Trim() } }
elseif(Test-Path ".\configs\investable_universe.txt"){ $ids = Get-Content ".\configs\investable_universe.txt" | Where-Object { $_.Trim() } }
if(-not $ids -or $ids.Count -eq 0){ throw "找不到可用 IDs（請提供 -IDs 或準備 configs\groups\$Group.txt）" }

# --- Log 檔 ---
$tagPart = if([string]::IsNullOrEmpty($Tag)){"A"} else {$Tag}
$script:LogFile = ".\reports\dateid_extras_{0}_{1}.log" -f ($from.ToString('yyyyMMdd')),$tagPart
"[{0}] === {1} → {2} === IDs={3}" -f (TS),$from.ToString('yyyy-MM-dd'),$to.ToString('yyyy-MM-dd'),(($ids -join ',').Substring(0,[Math]::Min(80,($ids -join ',').Length))) | Tee-Object -FilePath $script:LogFile -Append | Out-Host

# --- API base ---
$ApiBase = if([string]::IsNullOrEmpty($env:FINMIND_API_BASE)) { "https://api.finmindtrade.com/api/v4/data" } else { $env:FINMIND_API_BASE }
$Token   = $env:FINMIND_TOKEN

# 資料集：KBar 可能 400，必要時 fallback 到 TaiwanStockPrice
$Datasets = @(
  'TaiwanStockKBar',
  'TaiwanStockShareholding',
  'TaiwanStockMarketValue',
  'TaiwanStockMarketValueWeight',
  'TaiwanStockSplitPrice',
  'TaiwanStockParValueChange',
  'TaiwanStockDelisting',
  'TaiwanStockCapitalReductionReferencePrice'
)
$Fallback = @{ 'TaiwanStockKBar' = 'TaiwanStockPrice' }

# 每次呼叫之間的節流
$delayMs = if($ThrottleRPM -gt 0){ [int](60000 / [Math]::Max(1,$ThrottleRPM)) } else { 0 }

function Build-Query([string]$ds,[string]$id){
  # 用 ordered 確保輸出順序穩定
  $q = [ordered]@{ dataset=$ds; data_id=$id }
  if($ds -eq 'TaiwanStockKBar'){
    # K 線多數情況用 start_time / end_time + time_interval
    $q.start_time   = $from.ToString('yyyy-MM-dd')
    $q.end_time     = $to.ToString('yyyy-MM-dd')
    $q.time_interval= 1
  } else {
    $q.start_date   = $from.ToString('yyyy-MM-dd')
    $q.end_date     = $to.ToString('yyyy-MM-dd')
  }
  if($Token){ $q.token=$Token }
  return $q
}

function Invoke-One([string]$ds,[string]$id){
  $qs  = Build-Query $ds $id
  $uri = $ApiBase + '?' + (($qs.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '&')
  $tries=0
  while($true){
    try{
      $resp = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 60
      $rows = 0; if($resp -and $resp.data){ $rows = ($resp.data | Measure-Object).Count }
      Log 'DONE' ("{0}:{1} {2}→{3} total_rows={4}" -f $ds,$id,$from.ToString('yyyy-MM-dd'),$to.ToString('yyyy-MM-dd'),$rows)
      break
    }catch{
      $code   = $_.Exception.Response.StatusCode.value__ 2>$null
      $detail = $_.Exception.Message

      # 5xx → 重試一次
      if($code -ge 500 -and $tries -lt 1){
        $tries++; Start-Sleep -Milliseconds 500; continue
      }

      # 400 for KBar → fallback 到 TaiwanStockPrice
      if($code -eq 400 -and $ds -eq 'TaiwanStockKBar' -and $Fallback.ContainsKey($ds)){
        $alt = $Fallback[$ds]
        Log 'WARN' ("{0}:{1} 400 → fallback {2}" -f $ds,$id,$alt)
        $qs2  = Build-Query $alt $id
        $uri2 = $ApiBase + '?' + (($qs2.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '&')
        try{
          $resp2 = Invoke-RestMethod -Method Get -Uri $uri2 -TimeoutSec 60
          $rows2 = 0; if($resp2 -and $resp2.data){ $rows2 = ($resp2.data | Measure-Object).Count }
          Log 'DONE' ("{0}:{1} {2}→{3} total_rows={4}" -f $alt,$id,$from.ToString('yyyy-MM-dd'),$to.ToString('yyyy-MM-dd'),$rows2)
        }catch{
          $code2 = $_.Exception.Response.StatusCode.value__ 2>$null
          $etype2= ($code2 -eq 429) ? '429' : 'FAIL'
          Log $etype2 ("{0}:{1} {2}→{3} http={4} err={5}" -f $alt,$id,$from.ToString('yyyy-MM-dd'),$to.ToString('yyyy-MM-dd'),$code2,$_.Exception.Message)
          if($FailOnError){ throw }
        }
        break
      }

      $etype = ($code -eq 429) ? '429' : 'FAIL'
      Log $etype ("{0}:{1} {2}→{3} http={4} err={5}" -f $ds,$id,$from.ToString('yyyy-MM-dd'),$to.ToString('yyyy-MM-dd'),$code,$detail)
      if($FailOnError){ throw }
      break
    }
  }
}

foreach($id in $ids){
  foreach($ds in $Datasets){
    Invoke-One $ds $id
    if($delayMs -gt 0){ Start-Sleep -Milliseconds $delayMs }
  }
}
