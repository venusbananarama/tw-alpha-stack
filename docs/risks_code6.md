# 代號六（風險與備忘） — 2025-10-21 03:24:26
1) Orchestrator 誤植自呼：self_call=True；以 git restore 還原，AST 插 guard；入口禁 shim。
2) -and 誤判為參數：外層加括號 `(Test-Path) -and -not (...)`。
3) 範圍運算子優先序：`$lines[(($start-1))..(($end-1))]`。
4) -join 誤用：先 Select/Sort，再 `-join ', '`。
5) git show "$sha:$path" 插值：用 `"{0}:{1}" -f $sha,$path` / `"$($sha):$path"` / `git restore --source`。
6) Kill race：`Stop-Process -ErrorAction SilentlyContinue`；二次列舉確認。
7) 根層 InvalidLeftHandSide：隔離至 _legacy\quarantine + 根層 shim；維持 Parser 全綠。
