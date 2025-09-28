#!/usr/bin/env python3
import subprocess, sys, os, re
from pathlib import Path

def latest_metrics_csv(metrics_dir: Path):
  pats = ["*.csv","ingest*.csv","ingest_summary_*.csv","*metrics*.csv"]
  cands = []
  for p in pats:
    cands += list(metrics_dir.glob(p))
  cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
  return cands[0] if cands else None

def main():
  if len(sys.argv) < 2:
    print("USAGE: emit_metrics_wrapper.py <inner_script> [args...]", file=sys.stderr)
    sys.exit(2)
  repo_root = Path(__file__).resolve().parents[1]
  metrics_dir = repo_root / "metrics"
  metrics_dir.mkdir(parents=True, exist_ok=True)

  cmd = [sys.argv[1]] + sys.argv[2:]

  # Force unbuffered, UTF-8 in the child as well.
  env = os.environ.copy()
  env["PYTHONUTF8"] = "1"
  env["PYTHONIOENCODING"] = "utf-8"
  env["PYTHONUNBUFFERED"] = "1"

  proc = subprocess.Popen([sys.executable, "-u"] + cmd, cwd=repo_root,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, bufsize=1, encoding="utf-8", errors="replace",
                          env=env)

  seen = []
  for line in proc.stdout:
    print(line, end="")
    m = re.search(r"metrics:\s*(.+?\.csv)", line, re.IGNORECASE)
    if m:
      p = Path(m.group(1).strip())
      if not p.is_absolute():
        p = (repo_root / p).resolve()
      if p.exists():
        seen.append(p)
  proc.wait()

  chosen = None
  if seen:
    def name_ts(p: Path):
      m = re.search(r"(\d{8})-(\d{6})", p.name)
      if m:
        try: return int(m.group(1) + m.group(2))
        except: return -1
      return -1
    chosen = sorted(seen, key=lambda x: (name_ts(x), x.stat().st_mtime), reverse=True)[0]
  else:
    lm = latest_metrics_csv(metrics_dir)
    if lm: chosen = lm.resolve()

  phase = os.getenv("AC_PHASE") or ""
  phase_tag = f" phase:{phase}" if phase else ""
  print(f"=== Backfill Done ==={phase_tag}  metrics: {str(chosen) if chosen else ''}")
  sys.exit(proc.returncode or 0)

if __name__ == "__main__":
  main()