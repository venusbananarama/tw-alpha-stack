param(
  [Parameter(Mandatory=$true)][ValidateSet("get","set")]$Mode,
  [Parameter(Mandatory=$true)][string]$IdsHash,
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End,
  [string]$File = ".\reports\dateid_extras_checkpoint.json"
)
if(!(Test-Path $File)){ "{}" | Set-Content $File -Encoding UTF8 }
$json = Get-Content $File -Raw -Encoding UTF8 | ConvertFrom-Json
$key = "$IdsHash|$Start|$End"
if($Mode -eq 'get'){
  if($json.PSObject.Properties.Name -contains $key){ 'HIT' } else { 'MISS' }
} else {
  $json | Add-Member -NotePropertyName $key -NotePropertyValue 'OK' -Force
  ($json | ConvertTo-Json -Depth 2) | Set-Content $File -Encoding UTF8
  "SET $key"
}
