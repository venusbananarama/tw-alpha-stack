param(
  [Parameter(Mandatory=$true)][string] $Dir,
  [int] $Max = 5,
  [string] $PythonExe = ".\.venv\Scripts\python.exe"
)
$ErrorActionPreference = "Stop"; Set-StrictMode -Version Latest
$files = Get-ChildItem -Path $Dir -Recurse -Filter *.parquet -File -ErrorAction SilentlyContinue | Sort-Object Length -Descending | Select-Object -First $Max
if (-not $files) { Write-Warning "No parquet files under $Dir"; exit 0 }

$pycode = @'
import sys, pyarrow.parquet as pq
ok, fails = 0, []
for path in sys.argv[1:]:
    try:
        t = pq.read_table(path, memory_map=True)
        n = t.num_rows
        print(f"[OK] {path} rows={n}")
        ok += 1
    except Exception as e:
        print(f"[ERR] {path} {e!r}")
        fails.append((path, repr(e)))
print(f"SUMMARY ok={ok} fail={len(fails)}")
'@

$args = @()
foreach ($f in $files) { $args += $f.FullName }

& $PythonExe -c $pycode @args