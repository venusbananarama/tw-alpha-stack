# 代號4 SSOT 擴充更新包（2025-09-30）

包含：
- `configs/rules.yaml`（完整覆蓋版，含 DSR/WF/槓桿等鍵）
- `scripts/preflight_check.py`（啟動前檢核與 run_manifest 輸出）
- `scripts/wf_gate_helper.py`（WF Gate 套用模組）
- `tools/Run-SmokeTests.ps1.new`（可直接使用或套用補丁）
- `patches/*.unified.patch`（wf_runner 與 SmokeTests 的最小修補）

最小整合步驟：
1. 備份原專案。
2. 覆蓋 `configs/rules.yaml`，新增 `scripts/preflight_check.py`、`scripts/wf_gate_helper.py`。
3. 二選一：
   - 直接使用 `tools/Run-SmokeTests.ps1.new` 取代現檔；或
   - 套用 `patches/Run-SmokeTests.ps1.unified.patch`。
4. 在 `wf_runner.py` 讀取彙總 DataFrame 後，加入：
   ```python
   from scripts.wf_gate_helper import apply_gate, print_gate_result
   import yaml, json, sys
   rules = yaml.safe_load(open('configs/rules.yaml', encoding='utf-8'))
   gate = apply_gate(df_results, rules)
   print_gate_result(gate)
   sys.exit(0 if gate['ok'] else 2)
   ```

驗證：
```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\Run-SmokeTests.ps1.new
```
