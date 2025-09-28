# 移除所有相關任務
$tasks = "AlphaCity_DailyETL","AlphaCity_Daily_Quick","AlphaCity_WeeklyBacktest","AlphaCity_Weekly_Strict"
foreach($t in $tasks){
  try {
    Unregister-ScheduledTask -TaskName $t -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "[DEL] $t"
  } catch {
    Write-Warning "Failed to remove $t: $($_.Exception.Message)"
  }
}