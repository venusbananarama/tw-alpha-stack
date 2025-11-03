import re, pathlib
p = pathlib.Path(r".\scripts\wf_runner.py")
s = p.read_text(encoding="utf-8")
def fix_leading_ws(line: str) -> str:
    m = re.match(r"^([ \t]*)", line)
    if not m: return line
    ws = m.group(1)
    # 依 Python 語義將 tab 以 8 欄位展開；只動「行首」縮排，不觸及內文字串
    ws2 = ws.expandtabs(8)
    return ws2 + line[len(ws):]
out = "".join(fix_leading_ws(ln) for ln in s.splitlines(keepends=True))
p.write_text(out, encoding="utf-8", newline="")
print("[fixed] leading whitespace normalized (tabs→8-space stops)")
