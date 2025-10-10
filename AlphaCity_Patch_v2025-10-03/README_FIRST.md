# AlphaCity Patch v2025-10-03（非破壞式）
目的：補上 DSR/PSR/t、執行回放門檻、POV/限價保護、容量/行業中性、不確定性輸出。
不覆蓋任何既有檔案。請手動合併或在測試目錄驗證後再覆蓋。

## 安裝步驟（不改你既有指令）
1) 解壓到專案根目錄旁邊，不要直接覆蓋原始檔案。
2) 先跑預檢：
   ```powershell
   $env:ALPHACITY_ALLOW='1'
   $PY .\scripts\preflight_check.py --rules .\rules.yaml --export reports
   ```
3) 檢視本包 `rules\rules_patch.yaml`，按需合併進你現有 `rules.yaml`。
4) 若 **configs/universe.yaml 沒改** → 無需重建投資池；若調整了板別/流動性門檻等，再執行：
   ```powershell
   $PY .\scripts\build_universe.py --config .\configs\universe.yaml --out .\datahub\silver\alpha\universe --drop-empty
   ```
5) 可選：執行 `tools\Run-PreflightAndGate.ps1` 做一次預檢與 Gate。

## 不會衝突的原因
- 只新增 Gate 與執行旗標；不改 `finmind_backfill.py`、`build_universe.py` 的參數與流程。
- `ALPHACITY_ALLOW=1` 只是解鎖腳本；`killswitch` 僅在策略模擬/實盤依據回撤生效，互不干涉。

## 快速清單
- [ ] `validation.dsr_min_after_costs=0.0`
- [ ] `validation.psr_min=0.9`
- [ ] `validation.t_min=2`
- [ ] `execution_checks.replay_mae_bps_max=2`
- [ ] `execution.pov=0.05` + `execution.limit_protect.atr_mul=2.0`
- [ ] `portfolio.position.max_weight=0.05`
- [ ] `portfolio.liquidity.max_adv_part=0.05`
- [ ] `portfolio.industry_neutral=true`
- [ ] `deep.uncertainty_output=[p05,p50,p95]`
- [ ] `live_controls.killswitch.dd_soft=-0.10, dd_hard=-0.15`
- [ ] `ssot.wf_pass_min=0.80, leverage_cap=1.3`

版本：2025-10-02
